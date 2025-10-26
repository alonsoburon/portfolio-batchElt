from pathlib import Path
from typing import Any, Mapping
from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator


DBT_PROJECT_PATH = Path(__file__).parent.parent.parent / "dbt_project"


class CustomDbtTranslator(DagsterDbtTranslator):
  def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
    """Assign group based on dbt model path."""
    fqn = dbt_resource_props.get("fqn", [])
    
    if "silver" in fqn:
      return "silver_layer"
    elif "gold" in fqn:
      return "gold_layer"
    
    return "default"
  
  def get_tags(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, str]:
    """Add layer tags based on model path."""
    tags = super().get_tags(dbt_resource_props)
    fqn = dbt_resource_props.get("fqn", [])
    
    if "silver" in fqn:
      return {**tags, "layer": "silver"}
    elif "gold" in fqn:
      return {**tags, "layer": "gold"}
    
    return tags


@dbt_assets(
  manifest=DBT_PROJECT_PATH / "target" / "manifest.json",
  dagster_dbt_translator=CustomDbtTranslator(),
  select="resource_type:model",
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
  """dbt models for Silver and Gold layers."""
  yield from dbt.cli(["build"], context=context).stream()
