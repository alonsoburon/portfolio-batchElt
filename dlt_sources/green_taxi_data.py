import dlt
import pandas as pd


@dlt.source(name="green_taxi_data")
def green_taxi_data_source(
    file_url="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-01.parquet",
):
    """
    A dlt source to read NYC Green Taxi data from a remote Parquet file.
    This serves as the source for our Iceberg lakehouse example.
    """

    @dlt.resource(name="trips", write_disposition="replace")
    def green_taxi_resource():
        df = pd.read_parquet(file_url)
        yield df.to_dict("records")

    return green_taxi_resource()
