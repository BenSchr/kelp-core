"""Query builder and DDL generation for Databricks Metric Views.

The metric view ``definition`` follows the Databricks metric view YAML
reference verbatim and is passed through to DDL as-is — including its
``comment``. The only Kelp extension is an optional ``tags`` map on
dimension/field/measure entries, which Kelp manages via ``ALTER``
statements and therefore strips from the DDL body.

Unlike the table builders in this package, metric views are not diffed
through a precomputed :class:`~kelp.catalog.uc_models.TableDiff` — the
definition is a full YAML/dict blob compared wholesale, and tags apply both
at the view level and per dimension/field/measure.
:class:`MetricViewQueryBuilder` mirrors the same "build a list of SQL
statements" shape as the table builders while accommodating that difference.

See:
- https://docs.databricks.com/aws/en/uc-semantics/metric-views/yaml-reference
- https://docs.databricks.com/aws/en/metric-views/create/sql
"""

import copy
import logging
from typing import Any

import yaml

from kelp.catalog.query_builders._sql import esc
from kelp.catalog.query_builders.view import ViewQueryBuilder
from kelp.catalog.uc_diff import TableDiffCalculator
from kelp.catalog.uc_models import DictDiff, RemoteCatalogConfig
from kelp.models.metric_view import MetricView

logger = logging.getLogger(__name__)

# Definition sections that carry per-entry Kelp-managed tags. The spec
# accepts both ``fields`` (preferred) and ``dimensions`` as equivalent keys.
_COLUMN_SECTIONS = ("fields", "dimensions", "measures")


def _dict_entries(definition: dict[str, Any], key: str) -> list[dict[str, Any]]:
    """Return the dict entries of a definition list field (e.g. ``measures``)."""
    entries = definition.get(key)
    if not isinstance(entries, list):
        return []
    return [entry for entry in entries if isinstance(entry, dict)]


def _strip_managed_tags(definition: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of *definition* without Kelp-managed ``tags`` entries.

    Tags on dimension/field/measure entries are a Kelp extension applied via
    ``ALTER`` statements — they are not part of the Databricks YAML spec, so
    they are removed both from the DDL body and from definition comparison.
    Everything else is left untouched.
    """
    stripped = copy.deepcopy(definition)
    for section in _COLUMN_SECTIONS:
        for entry in _dict_entries(stripped, section):
            entry.pop("tags", None)
    return stripped


def _yaml_body(metric_view: MetricView) -> str:
    """Render a metric view's definition as a YAML DDL body.

    Raises:
        ValueError: If the name or definition is missing.

    """
    if not metric_view.name:
        raise ValueError("Metric view name is required")
    if not metric_view.definition:
        raise ValueError(f"Metric view '{metric_view.get_qualified_name()}' must have a definition")

    payload = _strip_managed_tags(metric_view.definition)
    return yaml.safe_dump(payload, sort_keys=False, allow_unicode=True).rstrip()


def generate_create_metric_view_ddl(metric_view: MetricView) -> str:
    """Generate CREATE OR REPLACE VIEW DDL statement for a metric view.

    Args:
        metric_view: The metric view model to generate DDL for.

    Returns:
        SQL DDL statement as a string.

    Raises:
        ValueError: If required fields are missing.

    """
    yaml_body = _yaml_body(metric_view)
    fqn = metric_view.get_qualified_name()
    return "\n".join(
        [
            f"CREATE OR REPLACE VIEW {fqn}",
            "WITH METRICS",
            "LANGUAGE YAML",
            "AS $$",
            yaml_body,
            "$$",
        ]
    )


def generate_drop_metric_view_ddl(metric_view: MetricView) -> str:
    """Generate DROP VIEW DDL statement for a metric view.

    Args:
        metric_view: The metric view model to generate DDL for.

    Returns:
        SQL DDL statement as a string.

    """
    return f"DROP VIEW IF EXISTS {metric_view.get_qualified_name()}"


def generate_alter_metric_view_tags_ddl(metric_view: MetricView, tags: dict[str, str]) -> list[str]:
    """Generate ALTER VIEW statements for setting tags on a metric view.

    Args:
        metric_view: The metric view model.
        tags: Dictionary of tags to set.

    Returns:
        List of SQL DDL statements.

    """
    fqn = metric_view.get_qualified_name()
    return [
        f"ALTER VIEW {fqn} SET TAGS ('{tag_key}' = '{esc(tag_value)}')"
        for tag_key, tag_value in tags.items()
    ]


def generate_alter_metric_view_definition_ddl(metric_view: MetricView) -> str:
    """Generate ALTER VIEW statement to update a metric view definition.

    Per Databricks docs: https://docs.databricks.com/aws/en/metric-views/create/sql#alter-a-metric-view

    Args:
        metric_view: The metric view model with updated definition.

    Returns:
        SQL DDL statement as a string.

    Raises:
        ValueError: If required fields are missing.

    """
    yaml_body = _yaml_body(metric_view)
    fqn = metric_view.get_qualified_name()
    return "\n".join([f"ALTER VIEW {fqn}", "AS $$", yaml_body, "$$"])


def generate_alter_metric_view_column_tags_ddl(
    metric_view: MetricView,
    local_def: dict,
    remote_def: dict,
    enforce_tags: bool = False,
) -> list[str]:
    """Generate ALTER VIEW statements for metric view column tags.

    Covers ``dimensions``/``fields``/``measures`` entries. Reuses
    ViewQueryBuilder logic for generating view column tag statements
    (metric views are UC views under the hood).

    Args:
        metric_view: The metric view model.
        local_def: Local definition dict with tags.
        remote_def: Remote definition dict with tags.
        enforce_tags: When True, remote tags are ignored and only SET TAG
            statements are emitted (no UNSET) — used when the remote view
            doesn't exist yet.

    Returns:
        List of SQL SET TAG ON / UNSET TAG ON statements for view columns.

    """
    statements: list[str] = []
    fqn = metric_view.get_qualified_name()
    builder = ViewQueryBuilder()

    for section in _COLUMN_SECTIONS:
        local_tags = {
            entry["name"]: entry.get("tags", {}) for entry in _dict_entries(local_def, section)
        }
        remote_tags = {
            entry["name"]: entry.get("tags", {}) for entry in _dict_entries(remote_def, section)
        }

        for name in set(local_tags) | set(remote_tags):
            tag_diff = _create_tag_diff(
                local_tags.get(name, {}),
                {} if enforce_tags else remote_tags.get(name, {}),
            )
            if not tag_diff.has_changes:
                continue
            logger.debug(
                "Tag diff for %s '%s' in metric view '%s': %s", section, name, fqn, tag_diff
            )
            tag_statements = builder.column_tag_queries(fqn, name, tag_diff)
            if enforce_tags:
                tag_statements = [
                    stmt for stmt in tag_statements if not stmt.strip().startswith("UNSET TAG")
                ]
            statements.extend(tag_statements)

    return statements


def _create_tag_diff(local_tags: dict[str, str], remote_tags: dict[str, str]) -> DictDiff:
    """Create a DictDiff by comparing local and remote tag dictionaries.

    Args:
        local_tags: Desired tags.
        remote_tags: Current tags.

    Returns:
        DictDiff with creates, updates, and deletes.

    """
    creates = {key: value for key, value in local_tags.items() if key not in remote_tags}
    updates = {key: value for key, value in local_tags.items() if remote_tags.get(key) != value}
    deletes = [key for key in remote_tags if key not in local_tags]

    return DictDiff(creates=creates, updates=updates, deletes=deletes)


class MetricViewQueryBuilder:
    """Translate a local/remote metric view pair into SQL statements.

    Args:
        config: Sync configuration controlling tag management mode/scope.

    """

    def __init__(self, config: RemoteCatalogConfig) -> None:
        self._config = config
        self._differ = TableDiffCalculator(config)

    def build(self, metric_view: MetricView, remote: MetricView | None) -> list[str]:
        """Return SQL statements required to sync *metric_view* to match *remote*.

        Args:
            metric_view: Local metric view definition from the project catalog.
            remote: Current remote state, or ``None`` if it doesn't exist yet.
                When ``None``, a ``CREATE`` statement is emitted and tags are
                enforced (set-only, no deletes) since there is no remote
                state to diff against.

        Returns:
            Ordered list of SQL statements to execute.

        """
        fqn = metric_view.get_qualified_name()
        statements: list[str] = []
        enforce_tags = remote is None

        if remote is None:
            remote = metric_view
            statements.append(generate_create_metric_view_ddl(metric_view))

        definition_changed = _strip_managed_tags(metric_view.definition) != _strip_managed_tags(
            remote.definition
        )
        if definition_changed:
            statements.append(generate_alter_metric_view_definition_ddl(metric_view))
            logger.info("Definition changed for metric view '%s'", fqn)

        column_tag_statements = generate_alter_metric_view_column_tags_ddl(
            metric_view,
            metric_view.definition,
            remote.definition,
            enforce_tags=enforce_tags,
        )
        if column_tag_statements:
            statements.extend(column_tag_statements)
            logger.info(
                "Column tags changed for metric view '%s': %d statements",
                fqn,
                len(column_tag_statements),
            )

        tag_diff = self._differ.diff_dicts(
            metric_view.tags,
            remote.tags if not enforce_tags else {},
            self._config.managed_table_tags,
            self._config.table_tag_mode,
        )

        if tag_diff.has_changes:
            statements.extend(
                f"ALTER VIEW {fqn} SET TAGS ('{tag_key}' = '{tag_value}')"
                for tag_key, tag_value in tag_diff.updates.items()
            )
            statements.extend(
                f"ALTER VIEW {fqn} UNSET TAGS ('{tag_key}')" for tag_key in tag_diff.deletes
            )
            logger.info(
                "Tags changed for metric view '%s': +%d / -%d",
                fqn,
                len(tag_diff.updates),
                len(tag_diff.deletes),
            )

        return statements
