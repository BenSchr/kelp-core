"""Spark-based remote fetchers for tables and metric views.

Alternative to the SDK/REST engine in :mod:`kelp.catalog.remote_fetchers.sdk`
for use inside a Databricks job/notebook where a Spark session is already
active. Instead of one Databricks SDK call per table plus one per column
(see :mod:`kelp.utils.databricks`), this engine issues:

- One ``DESCRIBE EXTENDED <fqn> AS JSON`` query for entity-level metadata
  (columns, comment, properties, view text). ``TABLE`` is deliberately
  omitted from the statement so the same query also works for metric views.
  See https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table#json-formatted-output
- One query against ``information_schema.table_tags`` for entity tags.
- One query against ``information_schema.column_tags`` for every column's
  tags (instead of a per-column API round trip).

Caveats (read before relying on this engine):

- This module is only imported lazily by
  :class:`~kelp.catalog.remote_fetchers.factory.RemoteFetcherFactory`, since
  ``pyspark`` is not a core ``kelp-core`` dependency (Databricks Runtime
  provides it).
- Requires an active Spark session. Raises ``RuntimeError`` if none is
  active rather than fetching nothing silently. Use the ``"sdk"`` engine
  when running locally, in CI, or from the CLI.
"""

import json
import logging
import re
from typing import Any

import yaml

from kelp.models.metric_view import MetricView
from kelp.models.model import Model
from kelp.utils.databricks import _parse_clustering_columns, inject_metric_view_column_metadata

logger = logging.getLogger(__name__)


def _get_spark():
    """Return the active Spark session, or raise if none is running."""
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "The 'spark' remote-fetch engine requires an active Spark session "
            "(e.g. running inside a Databricks job/notebook). Use the 'sdk' "
            "engine instead when running locally, in CI, or from the CLI."
        )
    return spark


def _split_fqn(fqn: str) -> tuple[str, str, str]:
    """Split ``catalog.schema.table`` into its three parts."""
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected a 3-part fully-qualified name, got '{fqn}'")
    return parts[0], parts[1], parts[2]


def _describe_as_json(spark, fqn: str) -> dict[str, Any] | None:
    """Run ``DESCRIBE EXTENDED <fqn> AS JSON`` and parse the result.

    Returns ``None`` when the entity doesn't exist (or DESCRIBE fails).
    """
    try:
        result_row = spark.sql(f"DESCRIBE EXTENDED {fqn} AS JSON").collect()[0]
    except Exception:  # noqa: BLE001
        return None
    return json.loads(result_row[0])


def _format_column_type(type_json: Any) -> str | None:
    """Render a DESCRIBE ... AS JSON structured type into DDL text.

    The JSON output encodes types as objects, e.g.
    ``{"name": "string", "collation": "UTF8_BINARY"}`` or nested
    ``{"name": "array", "element_type": {...}}`` — this renders them into
    the same lowercase DDL text the SDK engine reports (``type_text``),
    e.g. ``string``, ``decimal(10,2)``, ``array<struct<a:int>>``.
    """
    if type_json is None:
        return None
    if isinstance(type_json, str):
        return type_json

    name = str(type_json.get("name", "unknown"))
    if name == "decimal":
        return f"decimal({type_json.get('precision', 10)},{type_json.get('scale', 0)})"
    if name in {"varchar", "char"} and "length" in type_json:
        return f"{name}({type_json['length']})"
    if name == "array":
        return f"array<{_format_column_type(type_json.get('element_type'))}>"
    if name == "map":
        key = _format_column_type(type_json.get("key_type"))
        value = _format_column_type(type_json.get("value_type"))
        return f"map<{key},{value}>"
    if name == "struct":
        fields = ",".join(
            f"{field.get('name')}:{_format_column_type(field.get('type'))}"
            for field in type_json.get("fields", [])
        )
        return f"struct<{fields}>"
    if name == "interval":
        start_unit = type_json.get("start_unit")
        end_unit = type_json.get("end_unit")
        if start_unit and end_unit:
            return f"interval {start_unit} to {end_unit}"
        if start_unit:
            return f"interval {start_unit}"
        return name
    return name


# DESCRIBE ... AS JSON stringifies constraints as e.g.
#   [(pk_1,PRIMARY KEY (`order_id`)), (fk_1,FOREIGN KEY (`user_id`) REFERENCES `cat`.`sch`.`users` (`id`))]
_PK_CONSTRAINT_RE = re.compile(r"\(\s*`?(?P<name>[^,`]+)`?\s*,\s*PRIMARY KEY\s*\((?P<cols>[^)]*)\)")
_FK_CONSTRAINT_RE = re.compile(
    r"\(\s*`?(?P<name>[^,`]+)`?\s*,\s*FOREIGN KEY\s*\((?P<cols>[^)]*)\)\s*"
    r"REFERENCES\s+(?P<ref_table>[^\s(]+)\s*\((?P<ref_cols>[^)]*)\)"
)


def _split_constraint_columns(raw: str) -> list[str]:
    """Extract column names from a constraint column blob.

    Prefers backticked identifiers (which also drops options like
    ``TIMESERIES`` that can trail a column name); falls back to a plain
    comma split when no backticks are present.
    """
    backticked = re.findall(r"`([^`]+)`", raw)
    if backticked:
        return backticked
    return [col.strip() for col in raw.split(",") if col.strip()]


def _parse_table_constraints(raw: Any) -> list[dict[str, Any]]:
    """Parse the stringified ``table_constraints`` field into Kelp constraint dicts.

    Produces the same shapes as the SDK engine:
    ``{"name", "type": "primary_key", "columns"}`` and
    ``{"name", "type": "foreign_key", "columns", "reference_table", "reference_columns"}``.
    """
    if not isinstance(raw, str) or not raw.strip():
        return []

    constraints: list[dict[str, Any]] = [
        {
            "name": match["name"].strip(),
            "type": "primary_key",
            "columns": _split_constraint_columns(match["cols"]),
        }
        for match in _PK_CONSTRAINT_RE.finditer(raw)
    ]
    constraints.extend(
        {
            "name": match["name"].strip(),
            "type": "foreign_key",
            "columns": _split_constraint_columns(match["cols"]),
            "reference_table": match["ref_table"].replace("`", ""),
            "reference_columns": _split_constraint_columns(match["ref_cols"]),
        }
        for match in _FK_CONSTRAINT_RE.finditer(raw)
    )
    return constraints


def _fetch_table_tags(spark, catalog: str, schema: str, table: str) -> dict[str, str]:
    """Fetch a table's tags in a single query."""
    rows = spark.sql(
        f"SELECT tag_name, tag_value FROM {catalog}.information_schema.table_tags "  # noqa: S608
        f"WHERE schema_name = '{schema}' AND table_name = '{table}'"
    ).collect()
    return {row.tag_name: row.tag_value for row in rows}


def _fetch_column_tags(spark, catalog: str, schema: str, table: str) -> dict[str, dict[str, str]]:
    """Fetch every column's tags for one table in a single query."""
    rows = spark.sql(
        f"SELECT column_name, tag_name, tag_value FROM {catalog}.information_schema.column_tags "  # noqa: S608
        f"WHERE schema_name = '{schema}' AND table_name = '{table}'"
    ).collect()
    tags_by_column: dict[str, dict[str, str]] = {}
    for row in rows:
        tags_by_column.setdefault(row.column_name, {})[row.tag_name] = row.tag_value
    return tags_by_column


class SparkTableFetcher:
    """Fetch table metadata via an active Spark session."""

    def fetch(self, fqn: str, profile: str | None = None) -> Model | None:
        """Return the table's current state, or ``None`` if it doesn't exist.

        Args:
            fqn: Fully-qualified table name (``catalog.schema.table``).
            profile: Unused by this engine (kept for interface parity with
                the SDK engine) — the active Spark session's own
                authentication is used instead of a Databricks CLI profile.

        """
        logger.debug("Fetching table '%s' via Spark engine", fqn)
        del profile
        spark = _get_spark()
        catalog, schema, table = _split_fqn(fqn)

        info = _describe_as_json(spark, fqn)
        if info is None:
            return None

        table_tags = _fetch_table_tags(spark, catalog, schema, table)
        column_tags_by_name = _fetch_column_tags(spark, catalog, schema, table)

        properties = info.get("table_properties") or {}
        columns_json = info.get("columns") or []

        table_obj: dict[str, Any] = {
            "name": table,
            "catalog": catalog,
            "schema": schema,
            "table_type": str(info.get("type", "unknown")).lower(),
            "description": info.get("comment"),
            "tags": table_tags,
            "columns": [
                {
                    "name": col.get("name"),
                    "description": col.get("comment"),
                    "data_type": _format_column_type(col.get("type")),
                    "nullable": col.get("nullable", True),
                    "tags": column_tags_by_name.get(col.get("name"), {}),
                }
                for col in columns_json
            ],
            "partition_cols": [
                col if isinstance(col, str) else col.get("name")
                for col in info.get("partition_columns") or []
            ],
            "cluster_by_auto": str(properties.get("clusterByAuto", "false")).lower() == "true",
            "cluster_by": _parse_clustering_columns(properties.get("clusteringColumns", "[]")),
            "auto_ttl": None,
            "table_properties": properties,
            "constraints": _parse_table_constraints(info.get("table_constraints")),
        }

        auto_ttl_expiration = properties.get("autottl.expireInDays")
        auto_ttl_time_col = properties.get("autottl.timestampColumn")
        if auto_ttl_expiration and auto_ttl_time_col:
            table_obj["auto_ttl"] = {
                "expire_in_days": int(auto_ttl_expiration),
                "timestamp_column": auto_ttl_time_col,
            }

        return Model(**table_obj)


class SparkMetricViewFetcher:
    """Fetch metric view metadata via an active Spark session.

    Uses the same ``DESCRIBE EXTENDED … AS JSON`` statement as the table
    fetcher (no ``TABLE`` keyword, so it works for metric views too) and
    parses the view's YAML body from the returned view text. Dimension,
    field, and measure comments come from the described columns; their tags
    come from ``information_schema.column_tags`` and are injected into the
    definition by name, mirroring the SDK engine's output shape.
    """

    def fetch(self, fqn: str, profile: str | None = None) -> MetricView | None:
        """Return the metric view's current state, or ``None`` if it doesn't exist.

        Args:
            fqn: Fully-qualified metric view name (``catalog.schema.view``).
            profile: Unused by this engine (kept for interface parity with
                the SDK engine).

        """
        del profile
        spark = _get_spark()
        catalog, schema, view = _split_fqn(fqn)

        info = _describe_as_json(spark, fqn)
        if info is None:
            return None

        view_text = info.get("view_text") or ""
        definition = yaml.safe_load(view_text) if view_text else {}
        if not isinstance(definition, dict):
            definition = {}

        # The view-level comment is part of the YAML spec; if it only shows
        # up as the object comment, mirror it back into the definition.
        if "comment" not in definition and info.get("comment"):
            definition["comment"] = info["comment"]

        columns_json = info.get("columns") or []
        comments_by_column = {
            col.get("name"): col.get("comment") for col in columns_json if col.get("comment")
        }
        tags_by_column = _fetch_column_tags(spark, catalog, schema, view)
        inject_metric_view_column_metadata(definition, comments_by_column, tags_by_column)

        return MetricView(
            name=view,
            catalog=catalog,
            schema_=schema,
            tags=_fetch_table_tags(spark, catalog, schema, view),
            definition=definition,
        )
