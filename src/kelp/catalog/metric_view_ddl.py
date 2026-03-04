"""DDL generation for Databricks Metric Views.

This module handles the generation of SQL DDL statements for creating
and managing Databricks Metric Views.

See:
- https://docs.databricks.com/aws/en/metric-views/create/sql
- https://docs.databricks.com/aws/en/metric-views/data-modeling/
"""

from __future__ import annotations

import copy
import logging
from typing import Any

import yaml

from kelp.catalog.uc_models import DictDiff
from kelp.catalog.uc_query_builder import UCQueryBuilder
from kelp.models.metric_view import MetricView

logger = logging.getLogger(__name__)


def _normalize_metric_definition(
    definition: dict[str, Any],
    description: str | None,
) -> dict[str, Any]:
    """Normalize a metric view definition for YAML output.

    Notes:
        - Ensures ``version`` is set (defaults to 1.1).
        - Uses ``comment`` from description when not provided.
        - Maps legacy ``table`` to ``source`` when ``source`` is missing.
        - Maps legacy ``metrics`` to ``measures`` and ``expression`` to ``expr``.
        - Removes unsupported ``type`` fields from dimensions and measures.

    """
    payload = copy.deepcopy(definition)

    if "version" not in payload:
        payload["version"] = "1.1"

    if description and "comment" not in payload:
        payload["comment"] = description

    if "source" not in payload and "table" in payload:
        payload["source"] = payload.pop("table")

    if "metrics" in payload and "measures" not in payload:
        payload["measures"] = payload["metrics"]

    if "metrics" in payload:
        payload.pop("metrics", None)

    dimensions = payload.get("dimensions")
    if isinstance(dimensions, list):
        for dimension in dimensions:
            if isinstance(dimension, dict):
                if "expr" not in dimension and "name" in dimension:
                    dimension["expr"] = dimension["name"]
                dimension.pop("type", None)
                # Remove tags from DDL (they're managed separately via ALTER statements)
                dimension.pop("tags", None)

    measures = payload.get("measures")
    if isinstance(measures, list):
        for measure in measures:
            if isinstance(measure, dict):
                if "expr" not in measure and "expression" in measure:
                    measure["expr"] = measure.pop("expression")
                measure.pop("type", None)
                # Remove tags from DDL (they're managed separately via ALTER statements)
                measure.pop("tags", None)

    return payload


def generate_create_metric_view_ddl(metric_view: MetricView) -> str:
    """Generate CREATE OR REPLACE VIEW DDL statement for a metric view.

    Args:
        metric_view: The metric view model to generate DDL for.

    Returns:
        SQL DDL statement as a string.

    Raises:
        ValueError: If required fields are missing.

    Example SQL output:
        CREATE OR REPLACE VIEW catalog.schema.metric_name
        COMMENT 'Description here'
        AS {
          "dimensions": [...],
          "metrics": [...],
          "table": "catalog.schema.table_name"
        }

    """
    if not metric_view.name:
        raise ValueError("Metric view name is required")

    # Build fully qualified name
    fqn = metric_view.get_qualified_name()

    # Start building the DDL with proper metric view syntax
    ddl_parts = [f"CREATE OR REPLACE VIEW {fqn}"]
    ddl_parts.append("WITH METRICS")
    ddl_parts.append("LANGUAGE YAML")

    # Add the definition - must be in YAML format wrapped in $$
    if not metric_view.definition:
        raise ValueError(f"Metric view '{fqn}' must have a definition")

    definition_payload = _normalize_metric_definition(
        metric_view.definition,
        metric_view.description,
    )
    yaml_body = yaml.safe_dump(definition_payload, sort_keys=False, allow_unicode=True).rstrip()

    # Wrap YAML in $$ delimiters
    ddl_parts.append("AS $$")
    ddl_parts.append(yaml_body)
    ddl_parts.append("$$")

    return "\n".join(ddl_parts)


def generate_drop_metric_view_ddl(metric_view: MetricView) -> str:
    """Generate DROP VIEW DDL statement for a metric view.

    Args:
        metric_view: The metric view model to generate DDL for.

    Returns:
        SQL DDL statement as a string.

    """
    fqn = metric_view.get_qualified_name()
    return f"DROP VIEW IF EXISTS {fqn}"


def generate_alter_metric_view_tags_ddl(metric_view: MetricView, tags: dict[str, str]) -> list[str]:
    """Generate ALTER VIEW statements for setting tags on a metric view.

    Args:
        metric_view: The metric view model.
        tags: Dictionary of tags to set.

    Returns:
        List of SQL DDL statements.

    """
    if not tags:
        return []

    fqn = metric_view.get_qualified_name()
    statements = []

    for tag_key, tag_value in tags.items():
        # Escape single quotes in tag values
        escaped_value = tag_value.replace("'", "''")
        stmt = f"ALTER VIEW {fqn} SET TAGS ('{tag_key}' = '{escaped_value}')"
        statements.append(stmt)

    return statements


def generate_alter_metric_view_definition_ddl(metric_view: MetricView) -> str:
    """Generate ALTER METRIC VIEW statement to update metric view definition.

    Per Databricks docs: https://docs.databricks.com/aws/en/metric-views/create/sql#alter-a-metric-view

    Args:
        metric_view: The metric view model with updated definition.

    Returns:
        SQL DDL statement as a string.

    Raises:
        ValueError: If required fields are missing.

    """
    if not metric_view.name:
        raise ValueError("Metric view name is required")

    fqn = metric_view.get_qualified_name()

    if not metric_view.definition:
        raise ValueError(f"Metric view '{fqn}' must have a definition")

    # Normalize the definition
    definition_payload = _normalize_metric_definition(
        metric_view.definition,
        metric_view.description,
    )
    yaml_body = yaml.safe_dump(definition_payload, sort_keys=False, allow_unicode=True).rstrip()

    # Build ALTER METRIC VIEW statement (note: ALTER METRIC VIEW, not ALTER VIEW)
    ddl_parts = [f"ALTER VIEW {fqn}"]
    ddl_parts.append("AS $$")
    ddl_parts.append(yaml_body)
    ddl_parts.append("$$")

    return "\n".join(ddl_parts)


def generate_alter_metric_view_column_tags_ddl(
    metric_view: MetricView,
    local_def: dict,
    remote_def: dict,
    enforce_tags: bool = False,
) -> list[str]:
    """Generate ALTER VIEW statements for metric view column tags (dimensions/measures).

    Reuses UCQueryBuilder logic for generating view column tag statements.

    Args:
        metric_view: The metric view model.
        local_def: Local definition dict with tags.
        remote_def: Remote definition dict with tags.

    Returns:
        List of SQL SET TAG ON / UNSET TAG ON statements for view columns.

    """
    statements = []
    fqn = metric_view.get_qualified_name()
    builder = UCQueryBuilder()

    # Process dimensions
    local_dims = {
        d["name"]: d.get("tags", {}) for d in local_def.get("dimensions", []) if isinstance(d, dict)
    }
    remote_dims = {
        d["name"]: d.get("tags", {})
        for d in remote_def.get("dimensions", [])
        if isinstance(d, dict)
    }

    for dim_name in set(local_dims.keys()) | set(remote_dims.keys()):
        tag_diff = _create_tag_diff(
            local_dims.get(dim_name, {}), remote_dims.get(dim_name, {}) if not enforce_tags else {}
        )

        if tag_diff.has_changes:
            logger.debug(
                "Tag diff for dimension '%s' in metric view '%s': %s", dim_name, fqn, tag_diff
            )
            tag_statements = builder._column_tag_queries(fqn, dim_name, tag_diff, "view")  # noqa: SLF001
            ## filter out unset tag if enfore_tags
            filtered_statements = []
            if enforce_tags:
                filtered_statements = [
                    stmt for stmt in tag_statements if not stmt.strip().startswith("UNSET TAG")
                ]
            else:
                filtered_statements = tag_statements
            statements.extend(filtered_statements)

    # Process measures
    local_measures = {
        m["name"]: m.get("tags", {}) for m in local_def.get("measures", []) if isinstance(m, dict)
    }
    remote_measures = {
        m["name"]: m.get("tags", {}) for m in remote_def.get("measures", []) if isinstance(m, dict)
    }

    for measure_name in set(local_measures.keys()) | set(remote_measures.keys()):
        tag_diff = _create_tag_diff(
            local_measures.get(measure_name, {}),
            remote_measures.get(measure_name, {}) if not enforce_tags else {},
        )

        if tag_diff.has_changes:
            logger.debug(
                "Tag diff for measure '%s' in metric view '%s': %s", measure_name, fqn, tag_diff
            )
            tag_statements = builder._column_tag_queries(fqn, measure_name, tag_diff, "view")  # noqa: SLF001
            ## filter out unset tag if enfore_tags
            filtered_statements = []
            if enforce_tags:
                filtered_statements = [
                    stmt for stmt in tag_statements if not stmt.strip().startswith("UNSET TAG")
                ]
            else:
                filtered_statements = tag_statements
            statements.extend(filtered_statements)

    return statements


def _create_tag_diff(local_tags: dict[str, str], remote_tags: dict[str, str]) -> DictDiff:
    """Create a DictDiff by comparing local and remote tag dictionaries.

    Args:
        local_tags: Desired tags.
        remote_tags: Current tags.

    Returns:
        DictDiff with updates and deletes.

    """
    creates = {key: value for key, value in local_tags.items() if key not in remote_tags}
    updates = {key: value for key, value in local_tags.items() if remote_tags.get(key) != value}
    deletes = [key for key in remote_tags if key not in local_tags]

    return DictDiff(creates=creates, updates=updates, deletes=deletes)
