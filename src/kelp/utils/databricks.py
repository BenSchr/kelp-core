import json
import logging

import yaml
from databricks.sdk import WorkspaceClient

from kelp.models.metric_view import MetricView
from kelp.models.model import Model

logger = logging.getLogger(__name__)


def _parse_clustering_columns(raw_value: str | list[object] | None) -> list[str]:
    """Parse Databricks clustering columns into a flat list of column names."""
    if raw_value is None:
        return []

    parsed: object = raw_value
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            # Fallback for non-JSON string formats
            return [col.strip().strip('"').strip("'") for col in text.split(",") if col.strip()]

    if not isinstance(parsed, list):
        return []

    columns: list[str] = []
    for item in parsed:
        if isinstance(item, str):
            value = item.strip()
            if value:
                columns.append(value)
        elif isinstance(item, list) and item:
            first = item[0]
            if isinstance(first, str):
                value = first.strip()
                if value:
                    columns.append(value)

    return columns


def get_table_from_dbx_sdk(
    full_table: str,
    w: WorkspaceClient | None = None,
    profile: str | None = None,
) -> Model | None:
    """Retrieve table metadata from Databricks SDK and convert to Kelp Table format."""
    w = w or WorkspaceClient(profile=profile)
    try:
        info = w.tables.get(full_table)
    except Exception:  # noqa: BLE001
        # if message starts with not found
        return None
    table_tags = {}
    for tag in w.entity_tag_assignments.list("tables", full_table):
        table_tags[tag.tag_key] = tag.tag_value

    table_obj = {}
    table_obj["name"] = info.name
    table_obj["catalog"] = info.catalog_name
    table_obj["schema"] = info.schema_name
    table_obj["table_type"] = info.table_type.value.lower() if info.table_type else "unknown"
    table_obj["description"] = info.comment
    table_obj["tags"] = table_tags
    table_obj["columns"] = []
    table_obj["partition_cols"] = []
    table_obj["cluster_by"] = []
    table_obj["cluster_by_auto"] = False
    if info.properties:
        table_obj["cluster_by_auto"] = (
            info.properties.get("clusterByAuto", "false").lower() == "true"
        )
        table_obj["cluster_by"] = _parse_clustering_columns(
            info.properties.get("clusteringColumns", "[]")
        )
    if info.columns:
        table_obj["partition_cols"] = [
            col.name
            for col in sorted(
                [col for col in info.columns if col.partition_index is not None],
                key=lambda col: col.partition_index,
            )
        ]

        for col in info.columns:
            col_tags = {}
            for tag in w.entity_tag_assignments.list("columns", f"{full_table}.{col.name}"):
                col_tags[tag.tag_key] = tag.tag_value
            col_obj = {
                "name": col.name,
                "description": col.comment,
                "data_type": col.type_text,
                "nullable": col.nullable,
                "tags": col_tags,
            }
            table_obj["columns"].append(col_obj)

    table_obj["table_properties"] = info.properties

    ## constraints
    table_obj["constraints"] = []
    pk_contraint = {}
    fk_constraint = {}
    if info.table_constraints:
        for constraint in info.table_constraints:
            if constraint.primary_key_constraint:
                pk_contraint = {
                    "name": constraint.primary_key_constraint.name,
                    "type": "primary_key",
                    "columns": constraint.primary_key_constraint.child_columns,
                }
                table_obj["constraints"].append(pk_contraint)
            if constraint.foreign_key_constraint:
                fk_constraint = {
                    "name": constraint.foreign_key_constraint.name,
                    "type": "foreign_key",
                    "columns": constraint.foreign_key_constraint.child_columns,
                    "reference_table": constraint.foreign_key_constraint.parent_table,
                    "reference_columns": constraint.foreign_key_constraint.parent_columns,
                }
                table_obj["constraints"].append(fk_constraint)

    return Model(**table_obj)


def get_metric_view_from_dbx_sdk(
    full_metric_view: str,
    w: WorkspaceClient | None = None,
    profile: str | None = None,
) -> MetricView:
    """Retrieve metric view metadata from Databricks SDK and convert to Kelp MetricView format."""
    w = w or WorkspaceClient(profile=profile)
    info = w.tables.get(full_metric_view)

    metric_tags = {}
    for tag in w.entity_tag_assignments.list("tables", full_metric_view):
        metric_tags[tag.tag_key] = tag.tag_value

    view_definition = info.view_definition or ""
    definition_payload = yaml.safe_load(view_definition) if view_definition else {}
    if not isinstance(definition_payload, dict):
        definition_payload = {}

    description = info.comment
    if not description and isinstance(definition_payload, dict):
        description = definition_payload.get("comment")

    if isinstance(definition_payload, dict) and "comment" in definition_payload:
        definition_payload = {k: v for k, v in definition_payload.items() if k != "comment"}

    # Extract comments and tags from columns and inject into definition
    # Comments and tags are stored in columns metadata, not in view_definition
    dimension_comments = {}
    measure_comments = {}
    dimension_tags = {}
    measure_tags = {}

    for col in info.columns or []:
        try:
            # Parse type_json to get metric_view metadata
            type_data = json.loads(col.type_json) if col.type_json else {}
            metadata = type_data.get("metadata", {})
            mv_type = metadata.get("metric_view.type")

            # Fetch column tags
            col_tags = {}
            for tag in w.entity_tag_assignments.list("columns", f"{full_metric_view}.{col.name}"):
                col_tags[tag.tag_key] = tag.tag_value

            if mv_type == "dimension":
                if col.comment:
                    dimension_comments[col.name] = col.comment
                if col_tags:
                    dimension_tags[col.name] = col_tags
            elif mv_type == "measure":
                if col.comment:
                    measure_comments[col.name] = col.comment
                if col_tags:
                    measure_tags[col.name] = col_tags
        except (json.JSONDecodeError, AttributeError):
            pass  # Skip if type_json is invalid

    # Inject comments and tags into definition dimensions
    if isinstance(definition_payload.get("dimensions"), list):
        for dim in definition_payload["dimensions"]:
            if isinstance(dim, dict):
                dim_name = dim.get("name")
                if dim_name in dimension_comments:
                    dim["comment"] = dimension_comments[dim_name]
                if dim_name in dimension_tags:
                    dim["tags"] = dimension_tags[dim_name]

    # Inject comments and tags into definition measures
    if isinstance(definition_payload.get("measures"), list):
        for measure in definition_payload["measures"]:
            if isinstance(measure, dict):
                measure_name = measure.get("name")
                if measure_name in measure_comments:
                    measure["comment"] = measure_comments[measure_name]
                if measure_name in measure_tags:
                    measure["tags"] = measure_tags[measure_name]

    payload = {}
    payload["name"] = info.name
    payload["catalog"] = info.catalog_name
    payload["schema_"] = info.schema_name
    payload["description"] = description
    payload["tags"] = metric_tags
    payload["definition"] = definition_payload

    return MetricView(**payload)
