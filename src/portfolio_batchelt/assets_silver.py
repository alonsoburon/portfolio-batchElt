from dagster import (
    asset,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    TableSchema,
    TableColumn,
    TableColumnLineage,
    TableColumnDep,
    asset_check,
    AssetCheckResult,
)
from dagster.preview.freshness import FreshnessPolicy
from datetime import timedelta
from dagster_duckdb import DuckDBResource

bronze_taxi_trips_key = AssetKey(["bronze_layer", "yellow_taxi_trips"])
bronze_taxi_zones_key = AssetKey(["bronze_layer", "taxi_zones"])
bronze_weather_key = AssetKey(["bronze_layer", "hourly_weather"])


@asset(
    deps=[bronze_taxi_trips_key, bronze_taxi_zones_key, bronze_weather_key],
    group_name="silver_layer",
    key_prefix=["silver_layer"],
    tags={"layer": "silver"},
    freshness_policy=FreshnessPolicy.cron(
        deadline_cron="0 1 * * *",
        lower_bound_delta=timedelta(hours=1),
    ),
)
def silver_taxi_trips(duckdb: DuckDBResource):
  """Combines taxi data with zones and weather."""
  with duckdb.get_connection() as conn:
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver_layer;")
    conn.execute("""
            CREATE OR REPLACE TABLE silver_layer.silver_taxi_trips AS
            WITH taxi_trips AS (SELECT * FROM bronze_layer.yellow_taxi_trips),
                 taxi_zones AS (
                     SELECT "Location ID" AS location_id, borough, zone 
                     FROM bronze_layer.taxi_zones
                 ),
                 weather_data AS (
                     SELECT t.value AS observation_time,
                            temp.value AS temperature,
                            prec.value AS precipitation
                     FROM bronze_layer."hourly_weather__hourly__time" AS t
                     LEFT JOIN bronze_layer."hourly_weather__hourly__temperature_2m" AS temp
                         ON t._dlt_parent_id = temp._dlt_parent_id 
                        AND t._dlt_list_idx = temp._dlt_list_idx
                     LEFT JOIN bronze_layer."hourly_weather__hourly__precipitation" AS prec
                         ON t._dlt_parent_id = prec._dlt_parent_id 
                        AND t._dlt_list_idx = prec._dlt_list_idx
                 )
            SELECT tt.tpep_pickup_datetime,
                   tt.tpep_dropoff_datetime,
                   tt.passenger_count,
                   tt.trip_distance,
                   tt.total_amount,
                   pickup_zone.borough AS pickup_borough,
                   pickup_zone.zone AS pickup_zone,
                   dropoff_zone.borough AS dropoff_borough,
                   dropoff_zone.zone AS dropoff_zone,
                   wd.temperature,
                   wd.precipitation
            FROM taxi_trips tt
            LEFT JOIN taxi_zones AS pickup_zone ON tt."PULocationID" = pickup_zone.location_id
            LEFT JOIN taxi_zones AS dropoff_zone ON tt."DOLocationID" = dropoff_zone.location_id
            LEFT JOIN weather_data wd ON date_trunc('hour', CAST(tt.tpep_pickup_datetime AS TIMESTAMP)) = wd.observation_time
            WHERE tt.total_amount >= 0 AND pickup_zone.borough IS NOT NULL;
        """)

    row_count = conn.execute(
        "SELECT COUNT(*) FROM silver_layer.silver_taxi_trips").fetchone()[0]
    preview = conn.execute(
        "SELECT * FROM silver_layer.silver_taxi_trips LIMIT 5").fetchdf().to_markdown()

    column_schema = TableSchema(
        columns=[
            TableColumn("tpep_pickup_datetime", "timestamp"),
            TableColumn("tpep_dropoff_datetime", "timestamp"),
            TableColumn("passenger_count", "bigint"),
            TableColumn("trip_distance", "double"),
            TableColumn("total_amount", "double"),
            TableColumn("pickup_borough", "varchar"),
            TableColumn("pickup_zone", "varchar"),
            TableColumn("dropoff_borough", "varchar"),
            TableColumn("dropoff_zone", "varchar"),
            TableColumn("temperature", "double"),
            TableColumn("precipitation", "double"),
        ]
    )

    column_lineage = TableColumnLineage(
        deps_by_column={
            "pickup_borough": [TableColumnDep(asset_key=bronze_taxi_zones_key, column_name="Borough")],
            "pickup_zone": [TableColumnDep(asset_key=bronze_taxi_zones_key, column_name="Zone")],
            "dropoff_borough": [TableColumnDep(asset_key=bronze_taxi_zones_key, column_name="Borough")],
            "dropoff_zone": [TableColumnDep(asset_key=bronze_taxi_zones_key, column_name="Zone")],
            "temperature": [TableColumnDep(asset_key=bronze_weather_key, column_name="value")],
            "precipitation": [TableColumnDep(asset_key=bronze_weather_key, column_name="value")],
        }
    )

    yield MaterializeResult(
        metadata={
            "dagster/row_count": row_count,
            "preview": MetadataValue.md(preview),
            "dagster/column_schema": column_schema,
            "dagster/column_lineage": column_lineage,
        }
    )


@asset_check(asset=silver_taxi_trips)
def check_pickup_borough_not_null(duckdb: DuckDBResource):
  """Validates that pickup_borough is not null."""
  with duckdb.get_connection() as conn:
    null_count = conn.execute(
        "SELECT COUNT(*) FROM silver_layer.silver_taxi_trips WHERE pickup_borough IS NULL"
    ).fetchone()[0]

    return AssetCheckResult(
        passed=(null_count == 0),
        metadata={"null_borough_count": null_count},
    )


@asset_check(asset=silver_taxi_trips)
def check_total_amount_positive(duckdb: DuckDBResource):
  """Validates that total_amount >= 0."""
  with duckdb.get_connection() as conn:
    negative_count = conn.execute(
        "SELECT COUNT(*) FROM silver_layer.silver_taxi_trips WHERE total_amount < 0"
    ).fetchone()[0]

    return AssetCheckResult(
        passed=(negative_count == 0),
        metadata={"negative_amount_count": negative_count},
    )
