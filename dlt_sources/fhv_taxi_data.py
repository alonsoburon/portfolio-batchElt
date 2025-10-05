import dlt
import duckdb


@dlt.source(name="fhv_taxi_data")
def fhv_taxi_data_source(
    file_url="https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2023-01.parquet",
):
    """
    A dlt source to read FHV taxi data from a remote Parquet file.
    The table is fully replaced on each run to avoid merge complexities.
    """

    @dlt.resource(
        name="fhv_taxi_trips",
        write_disposition="replace",
    )
    def fhv_resource():
        """
        A dlt resource that uses DuckDB to query the FHV taxi dataset.
        The full table is loaded on each run.
        """
        query = f"""
        SELECT
            "dispatching_base_num",
            "pickup_datetime",
            "dropOff_datetime",
            "PULocationID",
            "DOLocationID",
            "SR_Flag",
            "Affiliated_base_number"
        FROM '{file_url}'
        WHERE "dispatching_base_num" IS NOT NULL
          AND "pickup_datetime" IS NOT NULL
          AND "PULocationID" IS NOT NULL
        """

        with duckdb.connect(config={'memory_limit': '1GB'}) as conn:
            df = conn.execute(query).fetchdf()
            yield df.to_dict("records")

    return fhv_resource()
