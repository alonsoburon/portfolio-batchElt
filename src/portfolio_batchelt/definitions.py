from pathlib import Path
from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from dagster_dlt import DagsterDltResource
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource

from . import assets_dlt, assets_dbt, assets_viz

DBT_PROJECT_PATH = Path(__file__).parent.parent.parent / "dbt_project"

dlt_assets = load_assets_from_modules([assets_dlt])
dbt_assets = load_assets_from_modules([assets_dbt])
viz_assets = load_assets_from_modules([assets_viz])

dlt_checks = load_asset_checks_from_modules([assets_dlt])

duckdb_resource = DuckDBResource(database="dbt_project/local_data.duckdb")
dbt_resource = DbtCliResource(
  project_dir=str(DBT_PROJECT_PATH),
  profiles_dir=str(DBT_PROJECT_PATH),
)

defs = Definitions(
  assets=[*dlt_assets, *dbt_assets, *viz_assets],
  asset_checks=[*dlt_checks],
  resources={
    "dlt": DagsterDltResource(),
    "duckdb": duckdb_resource,
    "dbt": dbt_resource,
  },
)
