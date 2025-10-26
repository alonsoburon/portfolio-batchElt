from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from dagster_dlt import DagsterDltResource
from dagster_duckdb import DuckDBResource

from . import assets_bronze, assets_silver, assets_gold

bronze_assets = load_assets_from_modules([assets_bronze])
silver_assets = load_assets_from_modules([assets_silver])
gold_assets = load_assets_from_modules([assets_gold])

bronze_checks = load_asset_checks_from_modules([assets_bronze])
silver_checks = load_asset_checks_from_modules([assets_silver])
gold_checks = load_asset_checks_from_modules([assets_gold])


duckdb_resource = DuckDBResource(database="local_data.duckdb")

defs = Definitions(
    assets=[*bronze_assets, *silver_assets, *gold_assets],
    asset_checks=[*bronze_checks, *silver_checks, *gold_checks],
    resources={
        "dlt": DagsterDltResource(),
        "duckdb": duckdb_resource,
    },
)
