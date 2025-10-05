import dlt
import duckdb

@dlt.resource(name="nyc_taxi_trips", write_disposition="replace")
def nyc_taxi_resource():
    """
    A dlt resource that uses DuckDB to query the NYC taxi dataset from a remote Parquet file.
    """
    query = "SELECT * FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet' LIMIT 100"
    
    with duckdb.connect() as conn:
        df = conn.execute(query).fetchdf()
        yield df.to_dict('records')

@dlt.source(name="nyc_taxi_data")
def nyc_taxi_data_source():
    """
    A dlt source for the NYC taxi dataset.
    """
    return nyc_taxi_resource()
