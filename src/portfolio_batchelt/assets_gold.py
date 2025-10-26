from dagster import (
    asset,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    TableSchema,
    TableColumn,
    asset_check,
    AssetCheckResult,
)
from dagster_duckdb import DuckDBResource

silver_taxi_trips_key = AssetKey(("silver_layer", "silver_taxi_trips"))


@asset(
    deps=[silver_taxi_trips_key],
    group_name="gold_layer",
    key_prefix=["gold_layer"],
    tags={"layer": "gold"}
)
def gold_hourly_metrics(duckdb: DuckDBResource):
  """Hourly trip metrics with weather data."""
  with duckdb.get_connection() as conn:
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold_layer;")
    conn.execute("""
            CREATE OR REPLACE TABLE gold_layer.hourly_metrics AS
            SELECT date_trunc('hour', tpep_pickup_datetime) AS hour,
                   COUNT(*) AS total_trips,
                   AVG(total_amount) AS avg_fare,
                   AVG(trip_distance) AS avg_distance,
                   MAX(temperature) AS temperature,
                   MAX(precipitation) AS precipitation
            FROM silver_layer.silver_taxi_trips
            GROUP BY 1
            ORDER BY 1;
        """)

    row_count = conn.execute(
        "SELECT COUNT(*) FROM gold_layer.hourly_metrics").fetchone()[0]
    preview = conn.execute(
        "SELECT * FROM gold_layer.hourly_metrics LIMIT 5").fetchdf().to_markdown()

    yield MaterializeResult(
        metadata={
            "dagster/row_count": row_count,
            "preview": MetadataValue.md(preview),
            "dagster/column_schema": TableSchema(
                columns=[
                    TableColumn("hour", "timestamp"),
                    TableColumn("total_trips", "bigint"),
                    TableColumn("avg_fare", "double"),
                    TableColumn("avg_distance", "double"),
                    TableColumn("temperature", "double"),
                    TableColumn("precipitation", "double"),
                ]
            )
        }
    )


@asset(
    deps=[silver_taxi_trips_key],
    group_name="gold_layer",
    key_prefix=["gold_layer"],
    tags={"layer": "gold"},
)
def gold_daily_metrics_by_borough(duckdb: DuckDBResource):
  """Daily metrics by borough."""
  with duckdb.get_connection() as conn:
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold_layer;")
    conn.execute("""
            CREATE OR REPLACE TABLE gold_layer.daily_metrics_by_borough AS
            SELECT date_trunc('day', tpep_pickup_datetime) AS day,
                   pickup_borough,
                   COUNT(*) AS total_trips,
                   SUM(total_amount) AS total_fare
            FROM silver_layer.silver_taxi_trips
            WHERE pickup_borough IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 1, 2;
        """)

    row_count = conn.execute(
        "SELECT COUNT(*) FROM gold_layer.daily_metrics_by_borough").fetchone()[0]
    preview = conn.execute(
        "SELECT * FROM gold_layer.daily_metrics_by_borough LIMIT 5").fetchdf().to_markdown()

    yield MaterializeResult(
        metadata={
            "dagster/row_count": row_count,
            "preview": MetadataValue.md(preview),
            "dagster/column_schema": TableSchema(
                columns=[
                    TableColumn("day", "timestamp"),
                    TableColumn("pickup_borough", "varchar"),
                    TableColumn("total_trips", "bigint"),
                    TableColumn("total_fare", "double"),
                ]
            )
        }
    )


@asset_check(asset=gold_hourly_metrics)
def check_hourly_total_trips_positive(duckdb: DuckDBResource):
  """Validates that trip counts are non-negative."""
  with duckdb.get_connection() as conn:
    negative_count = conn.execute(
        "SELECT COUNT(*) FROM gold_layer.hourly_metrics WHERE total_trips < 0"
    ).fetchone()[0]

    return AssetCheckResult(
        passed=(negative_count == 0),
        metadata={"negative_trip_count": negative_count},
    )
