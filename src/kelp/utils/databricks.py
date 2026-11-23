import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast
from urllib.parse import quote

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from kelp.models.grant import Grant, merge_grants
from kelp.models.metric_view import MetricView
from kelp.models.model import Model

logger = logging.getLogger(__name__)

# Semantic metadata properties Unity Catalog strips from a metric view's stored
# YAML body and reports in each column's raw ``metadata`` object instead.
METRIC_VIEW_SEMANTIC_METADATA_KEYS = ("display_name", "format", "synonyms")


def get_direct_grants_from_dbx_sdk(
    securable_type: str,
    fqn: str,
    w: WorkspaceClient | None = None,
    profile: str | None = None,
) -> list[Grant] | None:
    """Fetch direct grants from the raw Unity Catalog permissions API.

    The SDK's generated response decodes privileges through an enum, which can
    discard values unknown to that SDK version. Reading the raw response keeps
    those privileges available to the managed-mode diff.

    Args:
        securable_type: Unity Catalog API type, such as ``"table"`` or ``"function"``.
        fqn: Fully qualified securable name.
        w: Optional configured WorkspaceClient.
        profile: Databricks CLI profile used when constructing a client.

    Returns:
        Direct grants, an empty list when no direct grants exist, or ``None``
        when the securable does not exist.

    Raises:
        ValueError: If the securable type is unsupported.
        Exception: Propagates API and permission failures.
    """
    if securable_type not in {"table", "function"}:
        raise ValueError(f"Unsupported grants securable type: {securable_type!r}")

    client = w or WorkspaceClient(profile=profile)
    path = (
        "/api/2.1/unity-catalog/permissions/"
        f"{quote(securable_type, safe='')}/{quote(fqn, safe='.')}"
    )
    page_token: str | None = None
    grants: list[Grant] = []

    try:
        while True:
            query: dict[str, str | int] = {"max_results": 0}
            if page_token is not None:
                query["page_token"] = page_token
            raw_response = client.api_client.do("GET", path, query=query)
            if not isinstance(raw_response, dict):
                raise TypeError(
                    f"Expected a permissions response object for '{fqn}', "
                    f"got {type(raw_response).__name__}"
                )
            response = cast(dict[str, Any], raw_response)
            for assignment in response.get("privilege_assignments", []):
                principal = assignment.get("principal")
                if not principal:
                    logger.warning(
                        "Skipping direct grants for a deleted principal on '%s' because "
                        "the permissions response has no principal name",
                        fqn,
                    )
                    continue
                privileges = assignment.get("privileges") or []
                grants.append(Grant(principal=principal, privileges=privileges))
            page_token = response.get("next_page_token")
            if not page_token:
                break
    except NotFound:
        return None

    return merge_grants(grants) or []


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


def _fetch_column_tags(
    w: WorkspaceClient, column_names: list[str], fqn: str
) -> dict[str, dict[str, str]]:
    """Fetch tags for every column of *fqn*, one SDK call per column in parallel.

    Returns a ``{column_name: {tag_key: tag_value}}`` mapping. Each column's
    tags require a separate ``entity_tag_assignments.list`` call (there's no
    batched "all columns" endpoint), so a wide table means many round trips —
    threading keeps that latency from stacking up serially.
    """
    if not column_names:
        return {}

    def _fetch_one(column_name: str) -> tuple[str, dict[str, str]]:
        return column_name, _fetch_tags(w, "columns", f"{fqn}.{column_name}")

    with ThreadPoolExecutor() as executor:
        return dict(executor.map(_fetch_one, column_names))


def _get_raw_table_info(w: WorkspaceClient, fqn: str) -> dict[str, Any]:
    """Return the raw Unity Catalog table response.

    The generated SDK ``ColumnInfo`` model drops each column's ``metadata``
    object, where metric views report semantic metadata such as
    ``display_name``.

    Raises:
        TypeError: If the API returns a non-object response.
    """
    path = f"/api/2.1/unity-catalog/tables/{quote(fqn, safe='.')}"
    raw_response = w.api_client.do("GET", path)
    if not isinstance(raw_response, dict):
        raise TypeError(
            f"Expected a table response object for '{fqn}', got {type(raw_response).__name__}"
        )
    return cast(dict[str, Any], raw_response)


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

        column_tags_by_name = _fetch_column_tags(
            w, [col.name for col in info.columns if col.name], full_table
        )
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
    pk_constraint = {}
    fk_constraint = {}
    if info.table_constraints:
        for constraint in info.table_constraints:
            if constraint.primary_key_constraint:
                pk_constraint = {
                    "name": constraint.primary_key_constraint.name,
                    "type": "primary_key",
                    "columns": constraint.primary_key_constraint.child_columns,
                }
                table_obj["constraints"].append(pk_constraint)
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
    semantic_metadata_by_column: dict[str, dict[str, Any]] | None = None,
) -> None:
    """Inject column comments, tags, and semantic metadata into a metric view definition.

    Unity Catalog stores these values as column metadata instead of in the
    SDK-visible YAML body. This merges them back into the definition's
    ``dimensions``/``fields``/``measures`` entries by name so the resulting
    definition reflects the full remote state.
    """
    semantic_metadata_by_column = semantic_metadata_by_column or {}
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
            entry.update(semantic_metadata_by_column.get(name, {}))


def get_metric_view_from_dbx_sdk(
    full_metric_view: str,
    w: WorkspaceClient | None = None,
    profile: str | None = None,
) -> MetricView | None:
    """Retrieve metric view metadata from Databricks SDK and convert to Kelp MetricView format.

    The returned ``definition`` follows the Databricks metric view YAML
    reference verbatim (including its ``comment``); column comments, semantic
    metadata, and Kelp-managed tags are merged into the dimension/field/measure
    entries.
    """
    w = w or WorkspaceClient(profile=profile)
    try:
        info = _get_raw_table_info(w, full_metric_view)
    except Exception:  # noqa: BLE001
        return None

    view_definition = info.get("view_definition") or ""
    definition_payload = yaml.safe_load(view_definition) if view_definition else {}
    if not isinstance(definition_payload, dict):
        definition_payload = {}

    # The view-level comment is part of the YAML spec; if Unity Catalog only
    # reports it as the object comment, mirror it back into the definition.
    comment = info.get("comment")
    if "comment" not in definition_payload and comment:
        definition_payload["comment"] = comment

    columns = [
        col
        for col in info.get("columns") or []
        if isinstance(col, dict) and isinstance(col.get("name"), str) and col["name"]
    ]
    comments_by_column = {col["name"]: col["comment"] for col in columns if col.get("comment")}
    semantic_metadata_by_column = {
        col["name"]: {
            key: metadata[key] for key in METRIC_VIEW_SEMANTIC_METADATA_KEYS if key in metadata
        }
        for col in columns
        if isinstance(metadata := col.get("metadata"), dict)
    }
    tags_by_column = _fetch_column_tags(w, [col["name"] for col in columns], full_metric_view)
    inject_metric_view_column_metadata(
        definition_payload,
        comments_by_column,
        tags_by_column,
        semantic_metadata_by_column,
    )

    return MetricView(
        name=info.get("name") or "",
        catalog=info.get("catalog_name"),
        schema_=info.get("schema_name"),
        tags=_fetch_tags(w, "tables", full_metric_view),
        definition=definition_payload,
    )
