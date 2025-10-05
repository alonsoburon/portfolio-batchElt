from dagster import Definitions, load_assets_from_modules
from dagster_dlt import DagsterDltResource
from dagster_gcp import BigQueryResource

from . import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dlt": DagsterDltResource(),
        "bigquery": BigQueryResource(project="alonso-project1", location="us-east1"),
    },
)
