import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient

from kelp.models.metric_view import MetricView
from kelp.models.model import Model

logger = logging.getLogger(__name__)


def on_databricks() -> bool:
    """Detect if code is running in a Databricks environment."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


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


def _fetch_tags(w: WorkspaceClient, entity_type: str, entity_name: str) -> dict[str, str]:
    """Fetch tag assignments for a UC entity as a plain ``{key: value}`` dict."""
    return {
        tag.tag_key: tag.tag_value
        for tag in w.entity_tag_assignments.list(entity_type, entity_name)
        if tag.tag_key is not None and tag.tag_value is not None
    }


def _fetch_column_tags(w: WorkspaceClient, columns: list, fqn: str) -> dict[str, dict[str, str]]:
    """Fetch tags for every column of *fqn*, one SDK call per column in parallel.

    Returns a ``{column_name: {tag_key: tag_value}}`` mapping. Each column's
    tags require a separate ``entity_tag_assignments.list`` call (there's no
    batched "all columns" endpoint), so a wide table means many round trips —
    threading keeps that latency from stacking up serially.
    """
    if not columns:
        return {}

    def _fetch_one(col: Any) -> tuple[str, dict[str, str]]:
        return col.name, _fetch_tags(w, "columns", f"{fqn}.{col.name}")

    with ThreadPoolExecutor() as executor:
        return dict(executor.map(_fetch_one, columns))


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
    table_tags = _fetch_tags(w, "tables", full_table)

    table_obj: dict[str, Any] = {}
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
    table_obj["auto_ttl"] = None
    if info.properties:
        table_obj["cluster_by_auto"] = (
            info.properties.get("clusterByAuto", "false").lower() == "true"
        )
        table_obj["cluster_by"] = _parse_clustering_columns(
            info.properties.get("clusteringColumns", "[]")
        )

        ### AutoTTL
        auto_ttl_expiration = info.properties.get("autottl.expireInDays")
        auto_ttl_time_col = info.properties.get("autottl.timestampColumn")
        if auto_ttl_expiration and auto_ttl_time_col:
            table_obj["auto_ttl"] = {
                "expire_in_days": int(auto_ttl_expiration),
                "timestamp_column": auto_ttl_time_col,
            }

    if info.columns:
        table_obj["partition_cols"] = [
            col.name
            for col in sorted(
                [col for col in info.columns if col.partition_index is not None],
                key=lambda col: col.partition_index,
            )
        ]

        column_tags_by_name = _fetch_column_tags(w, info.columns, full_table)
        for col in info.columns:
            col_tags = column_tags_by_name.get(col.name, {}) if col.name else {}
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


def inject_metric_view_column_metadata(
    definition: dict,
    comments_by_column: dict[str, str],
    tags_by_column: dict[str, dict[str, str]],
) -> None:
    """Inject column comments/tags into a metric view definition in-place.

    Comments and tags live in Unity Catalog column metadata, not in the
    view's YAML body — this merges them back into the definition's
    ``dimensions``/``fields``/``measures`` entries by name so the resulting
    definition reflects the full remote state.
    """
    for section in ("dimensions", "fields", "measures"):
        entries = definition.get(section)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            if name in comments_by_column:
                entry["comment"] = comments_by_column[name]
            if tags_by_column.get(name):
                entry["tags"] = tags_by_column[name]


def get_metric_view_from_dbx_sdk(
    full_metric_view: str,
    w: WorkspaceClient | None = None,
    profile: str | None = None,
) -> MetricView | None:
    """Retrieve metric view metadata from Databricks SDK and convert to Kelp MetricView format.

    The returned ``definition`` follows the Databricks metric view YAML
    reference verbatim (including its ``comment``); column comments and
    Kelp-managed tags are merged into the dimension/field/measure entries.
    """
    w = w or WorkspaceClient(profile=profile)
    try:
        info = w.tables.get(full_metric_view)
    except Exception:  # noqa: BLE001
        return None

    view_definition = info.view_definition or ""
    definition_payload = yaml.safe_load(view_definition) if view_definition else {}
    if not isinstance(definition_payload, dict):
        definition_payload = {}

    # The view-level comment is part of the YAML spec; if Unity Catalog only
    # reports it as the object comment, mirror it back into the definition.
    if "comment" not in definition_payload and info.comment:
        definition_payload["comment"] = info.comment

    columns = info.columns or []
    comments_by_column = {col.name: col.comment for col in columns if col.name and col.comment}
    tags_by_column = _fetch_column_tags(w, columns, full_metric_view)
    inject_metric_view_column_metadata(definition_payload, comments_by_column, tags_by_column)

    return MetricView(
        name=info.name or "",
        catalog=info.catalog_name,
        schema_=info.schema_name,
        tags=_fetch_tags(w, "tables", full_metric_view),
        definition=definition_payload,
    )
