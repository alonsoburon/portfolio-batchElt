import dlt
import requests
from dagster import asset, asset_check, AssetCheckResult
from dagster_duckdb import DuckDBResource
from pathlib import Path
from dlt_sources.utils import download_file_if_not_exists

DATA_DIR = Path("data")
TAXI_TRIPS_FILE = DATA_DIR / "yellow_tripdata_2024-01.parquet"
TAXI_TRIPS_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
TAXI_ZONES_FILE = DATA_DIR / "taxi_zones.csv"
TAXI_ZONES_URL = "https://data.cityofnewyork.us/api/views/8meu-9t5y/rows.csv?accessType=DOWNLOAD"


@asset(
    group_name="bronze_layer",
    key_prefix=["bronze_layer"],
    tags={"layer": "bronze"},
)
def yellow_taxi_trips(duckdb: DuckDBResource) -> None:
  """Ingests yellow taxi trip data from NYC Open Data."""
  download_file_if_not_exists(TAXI_TRIPS_URL, TAXI_TRIPS_FILE)

  with duckdb.get_connection() as conn:
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze_layer;")
    conn.execute(f"""
            CREATE OR REPLACE TABLE bronze_layer.yellow_taxi_trips AS
            SELECT * FROM read_parquet('{TAXI_TRIPS_FILE.as_posix()}');
        """)


@asset_check(asset=yellow_taxi_trips)
def check_trip_distance_positive(duckdb: DuckDBResource):
  """Validates that trip_distance >= 0."""
  with duckdb.get_connection() as conn:
    negative_count = conn.execute(
        "SELECT COUNT(*) FROM bronze_layer.yellow_taxi_trips WHERE trip_distance < 0"
    ).fetchone()[0]

    return AssetCheckResult(
        passed=(negative_count == 0),
        metadata={"negative_distance_count": negative_count},
    )


@asset(
    group_name="bronze_layer",
    key_prefix=["bronze_layer"],
    tags={"layer": "bronze"},
)
def taxi_zones(duckdb: DuckDBResource) -> None:
  """Ingests and deduplicates NYC taxi zone data."""
  download_file_if_not_exists(TAXI_ZONES_URL, TAXI_ZONES_FILE)

  with duckdb.get_connection() as conn:
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze_layer;")
    conn.execute(f"""
            CREATE OR REPLACE TABLE bronze_layer.taxi_zones AS
            WITH ranked_zones AS (
                SELECT *, 
                       ROW_NUMBER() OVER(PARTITION BY "Location ID" ORDER BY "Zone" DESC) as rn
                FROM read_csv_auto('{TAXI_ZONES_FILE.as_posix()}', header=true)
            )
            SELECT * EXCLUDE (rn) FROM ranked_zones WHERE rn = 1;
        """)


@asset_check(asset=taxi_zones)
def check_taxi_zones_unique_ids(duckdb: DuckDBResource):
  """Validates that Location IDs are unique."""
  with duckdb.get_connection() as conn:
    duplicate_count = conn.execute(
        'SELECT COUNT("Location ID") - COUNT(DISTINCT "Location ID") FROM bronze_layer.taxi_zones'
    ).fetchone()[0]

    return AssetCheckResult(
        passed=(duplicate_count == 0),
        metadata={"duplicate_id_count": duplicate_count},
    )


@dlt.source
def weather_dlt_source():
  """API source for historical weather data in NYC."""
  @dlt.resource(name="hourly_weather", write_disposition="replace")
  def weather_resource():
    response = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": 40.71,
            "longitude": -74.01,
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "hourly": "temperature_2m,precipitation"
        }
    )
    response.raise_for_status()
    yield response.json()

  return weather_resource()


@asset(
    group_name="bronze_layer",
    key_prefix=["bronze_layer"],
    tags={"layer": "bronze"},
)
def hourly_weather(duckdb: DuckDBResource) -> None:
  """Ingests weather data using dlt with shared connection."""
  with duckdb.get_connection() as conn:
    pipeline = dlt.pipeline(
        pipeline_name="weather_data",
        destination=dlt.destinations.duckdb(credentials=conn),
        dataset_name="bronze_layer",
    )
    pipeline.run(weather_dlt_source())
