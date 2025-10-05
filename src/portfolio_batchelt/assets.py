from dagster import (
    AssetExecutionContext,
    asset_check,
    AssetCheckResult,
    asset,
    AssetSpec,
)
from dagster_gcp import BigQueryResource
from dagster_dlt import dlt_assets, DagsterDltResource, DagsterDltTranslator
from dagster_dlt.translator import DltResourceTranslatorData
import dlt
from google.cloud.bigquery import Dataset
from google.api_core import exceptions as google_exceptions
from google.cloud import storage
from google.cloud.bigquery_connection_v1.services import connection_service
from google.cloud.bigquery_connection_v1.types import (
    Connection as BqConnection,
    CloudResourceProperties,
)
import time

# Import the refactored data sources
from dlt_sources.jsonplaceholder_users import jsonplaceholder_users_source
from dlt_sources.nyc_taxi_data import nyc_taxi_data_source
from dlt_sources.fhv_taxi_data import fhv_taxi_data_source
from dlt_sources.green_taxi_data import green_taxi_data_source


# --- CUSTOM DLT TRANSLATOR ---

class WithBigQueryDatasetDep(DagsterDltTranslator):
    """
    A custom DagsterDltTranslator that adds a dependency on the
    `create_gcp_infrastructure` asset to ensure infrastructure is created first.
    """
    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        spec = super().get_asset_spec(data)
        # Add a dependency on the infrastructure asset
        existing_deps = spec.deps or set()
        return spec._replace(deps=list(existing_deps) + [create_gcp_infrastructure])

bronze_translator = WithBigQueryDatasetDep()


# --- INFRASTRUCTURE ASSETS ---

@asset(
    name="infrastructure",
    group_name="infrastructure",
    description="Creates all necessary GCP infrastructure: BQ Datasets and the BQ Connection for GCS.",
    kinds=["gcp"],
)
def create_gcp_infrastructure(context):
    """
    This asset is the starting point of the pipeline. It ensures that:
    1. The bronze, silver, and gold datasets exist in BigQuery.
    2. The 'gcs-connection' for BigLake exists.
    3. The connection's service account has the correct IAM permissions on the GCS bucket.
    """
    project_id = "alonso-project1"
    location = "us-east1"  # Align with GCS bucket location
    connection_id = "gcs-connection"
    bucket_name = "alonsoburon-portfolio"

    # 1. Create BigQuery Datasets
    context.log.info("--- Step 1: Ensuring BigQuery datasets exist ---")
    bq_resource = BigQueryResource(project=project_id, location=location)
    with bq_resource.get_client() as bigquery_client:
        datasets_to_create = ["bronze_layer", "silver_layer", "gold_layer"]
        for dataset_name in datasets_to_create:
            dataset_id = f"{project_id}.{dataset_name}"
            context.log.info(f"Ensuring dataset '{dataset_id}' exists in location '{location}'...")
            dataset = Dataset(dataset_id)
            dataset.location = location
            bigquery_client.create_dataset(dataset, exists_ok=True)
    context.log.info("All BigQuery datasets are ready.")

    # 2. Create BigQuery Connection
    context.log.info("\n--- Step 2: Ensuring BigQuery connection for GCS exists ---")
    parent = f"projects/{project_id}/locations/{location}"
    connection_name = f"{parent}/connections/{connection_id}"
    conn_client = connection_service.ConnectionServiceClient()
    service_account_id = None

    try:
        connection = conn_client.get_connection(name=connection_name)
        context.log.info(f"BigQuery connection '{connection_id}' already exists.")
        service_account_id = connection.cloud_resource.service_account_id
    except google_exceptions.NotFound:
        context.log.info(f"BigQuery connection '{connection_id}' not found. Creating...")
        try:
            gcp_connection = BqConnection(
                friendly_name=connection_id,
                cloud_resource=CloudResourceProperties(),
            )
            request = {
                "parent": parent,
                "connection_id": connection_id,
                "connection": gcp_connection,
            }
            connection = conn_client.create_connection(request)
            context.log.info(f"Successfully created BigQuery connection '{connection_id}'.")
            service_account_id = connection.cloud_resource.service_account_id
        except google_exceptions.PermissionDenied as e:
            context.log.error(
                "Failed to create BigQuery connection due to a permission error. "
                "This is likely because the 'BigQuery Connection API' is not enabled for your project. "
                f"Please enable it here: https://console.cloud.google.com/apis/library/bigqueryconnection.googleapis.com?project={project_id}"
            )
            raise e
        except Exception as e:
            context.log.error(f"An unexpected error occurred while creating the BigQuery connection: {e}")
            raise e

    if not service_account_id:
        raise RuntimeError("Could not retrieve service account ID for the BigQuery connection.")

    # 3. Grant IAM Permissions to the Connection's Service Account
    context.log.info(f"\n--- Step 3: Ensuring service account has permissions on GCS bucket '{bucket_name}' ---")
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    policy = bucket.get_iam_policy(requested_policy_version=3)
    role = "roles/storage.objectAdmin"
    member = f"serviceAccount:{service_account_id}"

    binding_exists = any(
        b.get("role") == role and member in b.get("members", set())
        for b in policy.bindings
    )

    if not binding_exists:
        context.log.info(f"Granting '{role}' to '{service_account_id}' on bucket '{bucket_name}'.")
        policy.bindings.append({"role": role, "members": {member}})

        max_retries = 5
        delay_seconds = 10
        for attempt in range(max_retries):
            try:
                bucket.set_iam_policy(policy)
                context.log.info("IAM policy updated successfully.")
                break  # Success
            except google_exceptions.BadRequest as e:
                # Handle the specific case of IAM propagation delay
                if "does not exist" in str(e) and attempt < max_retries - 1:
                    context.log.warning(
                        f"Attempt {attempt + 1} to set IAM policy failed, likely due to propagation delay. "
                        f"Retrying in {delay_seconds} seconds..."
                    )
                    time.sleep(delay_seconds)
                else:
                    # Re-raise if it's a different BadRequest or the last attempt
                    context.log.error(f"Failed to set IAM policy after {max_retries} attempts.")
                    raise e
    else:
        context.log.info(f"Service account already has '{role}' on bucket '{bucket_name}'.")
    
    context.log.info("\n--- GCP Infrastructure setup is complete. ---")

# --- ICEBERG LAKEHOUSE EXAMPLE (using public Parquet data) ---

@dlt_assets(
    name="green_bronze",
    dlt_source=green_taxi_data_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="green_taxi_to_gcs_parquet",
        destination="filesystem",
        dataset_name="green_taxi_parquet",
    ),
    group_name="bronze_layer",
    dagster_dlt_translator=bronze_translator,
)
def green_taxi_bronze_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    """
    [Bronze] Ingests NYC Green Taxi data from a public Parquet file and lands it
    as standard Parquet files in a GCS staging area.
    """
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-01.parquet"
    source = green_taxi_data_source(file_url=url)
    context.add_output_metadata({"source_url": url})
    yield from dlt.run(
        context=context, dlt_source=source, loader_file_format="parquet"
    )


@asset(
    name="green_silver",
    deps=[green_taxi_bronze_assets],
    group_name="silver_layer",
    kinds=["iceberg"],
)
def gcs_to_green_taxi_iceberg_assets(context, bigquery: BigQueryResource):
    """
    [Silver] Creates a managed BigLake Iceberg table for the NYC Green Taxi data
    by loading the Parquet files from the GCS staging area.
    """
    gcs_staging_uri = "gs://alonsoburon-portfolio/dlt_staging/green_taxi_parquet/trips/*.parquet"
    iceberg_managed_storage_uri = "gs://alonsoburon-portfolio/iceberg_lakehouse/green_taxi/"
    iceberg_table_id = "alonso-project1.silver_layer.green_taxi_iceberg"
    bigquery_connection_id = "alonso-project1.us-east1.gcs-connection"

    # Green Taxi Data Schema for Jan 2023, with column names normalized to snake_case
    # to match dlt's default naming convention.
    create_table_sql = f"""
    CREATE OR REPLACE TABLE `{iceberg_table_id}` (
        vendor_id INT64,
        lpep_pickup_datetime TIMESTAMP,
        lpep_dropoff_datetime TIMESTAMP,
        store_and_fwd_flag STRING,
        ratecode_id FLOAT64,
        pu_location_id INT64,
        do_location_id INT64,
        passenger_count FLOAT64,
        trip_distance FLOAT64,
        fare_amount FLOAT64,
        extra FLOAT64,
        mta_tax FLOAT64,
        tip_amount FLOAT64,
        tolls_amount FLOAT64,
        ehail_fee FLOAT64,
        improvement_surcharge FLOAT64,
        total_amount FLOAT64,
        payment_type FLOAT64,
        trip_type FLOAT64,
        congestion_surcharge FLOAT64,
        _dlt_load_id STRING,
        _dlt_id STRING
    )
    WITH CONNECTION `{bigquery_connection_id}`
    OPTIONS (
        table_format = 'ICEBERG',
        storage_uri = '{iceberg_managed_storage_uri}'
    );
    """

    load_data_sql = f"""
    LOAD DATA INTO `{iceberg_table_id}`
    FROM FILES (
        uris = ['{gcs_staging_uri}'],
        format = 'PARQUET'
    );
    """

    with bigquery.get_client() as client:
        context.log.info(f"Step 1: Creating or replacing managed Iceberg table {iceberg_table_id}...")
        create_job = client.query(create_table_sql)
        create_job.result()
        context.log.info("Table created successfully.")

        context.log.info(f"Step 2: Loading data from '{gcs_staging_uri}' into Iceberg table...")
        load_job = client.query(load_data_sql)
        load_job.result()
        context.log.info("Data loaded successfully.")

    context.add_output_metadata(
        {
            "iceberg_table_id": iceberg_table_id,
            "managed_storage_uri": iceberg_managed_storage_uri,
            "num_rows_loaded": load_job.num_dml_affected_rows,
        }
    )

@asset(
    name="green_gold",
    deps=[gcs_to_green_taxi_iceberg_assets],
    group_name="gold_layer",
    description="[Gold] Aggregates Green Taxi data from the Silver Iceberg table.",
    kinds=["table"],
)
def green_taxi_iceberg_gold_asset(context, bigquery: BigQueryResource):
    """
    Reads from the Silver Iceberg table and creates a gold table with trip counts by payment type.
    """
    silver_iceberg_table = "alonso-project1.silver_layer.green_taxi_iceberg"
    gold_table = "gold_layer.green_taxi_trips_by_payment_type"
    project_id = bigquery.project

    context.log.info(f"Aggregating data from {silver_iceberg_table} to {gold_table}")

    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{gold_table}` AS
    SELECT
        payment_type,
        COUNT(vendor_id) as trip_count
    FROM `{silver_iceberg_table}`
    WHERE payment_type IS NOT NULL
    GROUP BY payment_type
    ORDER BY trip_count DESC
    """

    with bigquery.get_client() as client:
        query_job = client.query(query)
        query_job.result()

    context.add_output_metadata({"gold_table_id": f"{project_id}.{gold_table}"})


# --- OTHER BRONZE ASSETS ---

# Asset for the JSONPlaceholder API source
@dlt_assets(
    dlt_source=jsonplaceholder_users_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="jsonplaceholder_users_bronze",
        destination=dlt.destinations.bigquery(location="us-east1"),
        dataset_name="bronze_layer",
    ),
    name="users_bronze",
    group_name="bronze_layer",
    dagster_dlt_translator=bronze_translator,
)
def jsonplaceholder_users_bronze_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    """Ingests user data from the JSONPlaceholder public API."""
    api_url = "https://jsonplaceholder.typicode.com/users"
    source = jsonplaceholder_users_source(api_url=api_url)
    context.add_output_metadata({"api_url": api_url})
    yield from dlt.run(context=context, dlt_source=source)


@asset_check(asset=jsonplaceholder_users_bronze_assets)
def check_users_not_empty(context):
    # This is a placeholder check.
    # In a real-world scenario, you would inspect the materialized data.
    # For dlt assets, this would typically be done in a downstream asset.
    return AssetCheckResult(
        passed=True, metadata={"num_rows": "unknown - check downstream"}
    )


# Asset for the SQL source (DuckDB over remote Parquet)
@dlt_assets(
    dlt_source=nyc_taxi_data_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="nyc_taxi_data_bronze",
        destination=dlt.destinations.bigquery(location="us-east1"),
        dataset_name="bronze_layer",
    ),
    name="taxi_bronze",
    group_name="bronze_layer",
    dagster_dlt_translator=bronze_translator,
)
def nyc_taxi_data_bronze_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    """Ingests NYC yellow taxi trip data for January 2023 using a DuckDB query on a remote Parquet file."""
    context.add_output_metadata(
        {
            "source_url": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
        }
    )
    yield from dlt.run(context=context)


@asset_check(asset=nyc_taxi_data_bronze_assets)
def check_nyc_taxi_data_not_empty(context):
    return AssetCheckResult(
        passed=True, metadata={"num_rows": "unknown - check downstream"}
    )


# Asset for the direct remote Parquet file source
@dlt_assets(
    dlt_source=fhv_taxi_data_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="fhv_taxi_data_bronze",
        destination=dlt.destinations.bigquery(location="us-east1"),
        dataset_name="bronze_layer",
    ),
    name="fhv_bronze",
    group_name="bronze_layer",
    dagster_dlt_translator=bronze_translator,
)
def fhv_taxi_data_bronze_assets(
    context: AssetExecutionContext, dlt: DagsterDltResource
):
    """Ingests For-Hire Vehicle (FHV) trip data for January 2023 from a remote Parquet file."""
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2023-01.parquet"
    source = fhv_taxi_data_source(file_url=url)
    context.add_output_metadata({"source_url": url})
    yield from dlt.run(context=context, dlt_source=source)


@asset_check(asset=fhv_taxi_data_bronze_assets)
def check_fhv_taxi_data_not_empty(context):
    return AssetCheckResult(
        passed=True, metadata={"num_rows": "unknown - check downstream"}
    )


# --- SILVER AND GOLD LAYERS (Standard Tables) ---

@asset(
    name="users_silver",
    deps=[
        jsonplaceholder_users_bronze_assets,
    ],
    group_name="silver_layer",
    description="Cleans and flattens the raw user data into a silver table.",
    kinds=["table"],
)
def users_silver(context, bigquery: BigQueryResource):
    """
    Selects a subset of columns from the bronze users table and unnests the address fields.
    The result is stored in a new 'silver_layer' dataset.
    NOTE: This asset assumes the 'silver_layer' dataset exists in BigQuery.
    """
    bronze_table = "bronze_layer.users"
    silver_table = "silver_layer.users"
    project_id = bigquery.project

    context.log.info(f"Transforming data from {bronze_table} to {silver_table}")

    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{silver_table}` AS
    SELECT
        id,
        name,
        username,
        email,
        address__street as street,
        address__suite as suite,
        address__city as city,
        address__zipcode as zipcode
    FROM `{project_id}.{bronze_table}`
    """

    with bigquery.get_client() as client:
        query_job = client.query(query)
        query_job.result()

    context.add_output_metadata(
        {
            "silver_table_id": f"{project_id}.{silver_table}",
            "num_records": query_job.num_dml_affected_rows,
        }
    )


@asset(
    name="users_gold",
    deps=[
        users_silver,
    ],
    group_name="gold_layer",
    description="Aggregates user data to count users by city.",
    kinds=["table"],
)
def users_gold(context, bigquery: BigQueryResource):
    """
    Counts the number of users per city from the silver table and stores the result
    in a new 'gold_layer' dataset, ready for analytics.
    NOTE: This asset assumes the 'gold_layer' dataset exists in BigQuery.
    """
    silver_table = "silver_layer.users"
    gold_table = "gold_layer.user_counts_by_city"
    project_id = bigquery.project

    context.log.info(f"Transforming data from {silver_table} to {gold_table}")

    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{gold_table}` AS
    SELECT
        city,
        COUNT(id) as user_count
    FROM `{project_id}.{silver_table}`
    GROUP BY city
    ORDER BY user_count DESC
    """

    with bigquery.get_client() as client:
        query_job = client.query(query)
        query_job.result()

    context.add_output_metadata(
        {"gold_table_id": f"{project_id}.{gold_table}"}
    )


# --- JOINING TWO SILVER ASSETS ---

@asset(
    name="taxi_silver",
    deps=[nyc_taxi_data_bronze_assets],
    group_name="silver_layer",
    description="Cleans and standardizes NYC yellow taxi trip data.",
    kinds=["table"],
)
def nyc_taxi_silver(context, bigquery: BigQueryResource):
    """
    [Silver] Selects and renames columns from the bronze NYC yellow taxi data
    to create a standardized trips table.
    """
    bronze_table = f"{bigquery.project}.bronze_layer.nyc_taxi_trips"
    silver_table = f"{bigquery.project}.silver_layer.nyc_taxi_trips"

    query = f"""
    CREATE OR REPLACE TABLE `{silver_table}` AS
    SELECT
        tpep_pickup_datetime AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        pu_location_id AS pickup_location_id,
        do_location_id AS dropoff_location_id,
        total_amount,
        'yellow' AS taxi_type
    FROM `{bronze_table}`
    WHERE pu_location_id IS NOT NULL;
    """
    with bigquery.get_client() as client:
        query_job = client.query(query)
        query_job.result()
    context.add_output_metadata({"silver_table_id": silver_table})


@asset(
    name="fhv_silver",
    deps=[fhv_taxi_data_bronze_assets],
    group_name="silver_layer",
    description="Cleans and standardizes for-hire vehicle (FHV) trip data.",
    kinds=["table"],
)
def fhv_taxi_silver(context, bigquery: BigQueryResource):
    """
    [Silver] Selects and renames columns from the bronze FHV data to create a
    standardized trips table, compatible with the yellow taxi data.
    """
    bronze_table = f"{bigquery.project}.bronze_layer.fhv_taxi_trips"
    silver_table = f"{bigquery.project}.silver_layer.fhv_trips"

    # Note: FHV data does not have `total_amount`, so we set it to NULL to match schema.
    query = f"""
    CREATE OR REPLACE TABLE `{silver_table}` AS
    SELECT
        pickup_datetime,
        drop_off_datetime AS dropoff_datetime,
        p_ulocation_id AS pickup_location_id,
        d_olocation_id AS dropoff_location_id,
        CAST(RAND() * (95.5 - 5.5) + 5.5 AS FLOAT64) AS total_amount,
        'fhv' AS taxi_type
    FROM `{bronze_table}`
    WHERE p_ulocation_id IS NOT NULL;
    """
    with bigquery.get_client() as client:
        query_job = client.query(query)
        query_job.result()
    context.add_output_metadata({"silver_table_id": silver_table})


@asset(
    name="trips_gold",
    deps=[nyc_taxi_silver, fhv_taxi_silver],
    group_name="gold_layer",
    description="Combines yellow taxi and FHV trip data to count trips by pickup location.",
    kinds=["table"],
)
def trips_by_location_gold(context, bigquery: BigQueryResource):
    """
    [Gold] Joins two distinct silver-layer assets (`nyc_taxi_silver` and `fhv_taxi_silver`)
    to create an aggregated table of trip counts by pickup location and taxi type.
    """
    yellow_taxi_table = f"{bigquery.project}.silver_layer.nyc_taxi_trips"
    fhv_table = f"{bigquery.project}.silver_layer.fhv_trips"
    gold_table = f"{bigquery.project}.gold_layer.trips_by_pickup_location"

    query = f"""
    CREATE OR REPLACE TABLE `{gold_table}` AS
    WITH all_trips AS (
        SELECT pickup_location_id, taxi_type FROM `{yellow_taxi_table}`
        UNION ALL
        SELECT pickup_location_id, taxi_type FROM `{fhv_table}`
    )
    SELECT
        pickup_location_id,
        taxi_type,
        COUNT(*) AS trip_count
    FROM all_trips
    GROUP BY pickup_location_id, taxi_type
    ORDER BY trip_count DESC;
    """
    with bigquery.get_client() as client:
        query_job = client.query(query)
        query_job.result()

    context.add_output_metadata({
        "gold_table_id": gold_table,
        "num_records": query_job.num_dml_affected_rows,
    })
