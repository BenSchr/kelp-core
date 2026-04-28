"""Unity Catalog adapter (v2) that integrates with Kelp runtime context."""

from __future__ import annotations

import logging

from kelp.catalog.abac_ddl import generate_create_abac_policy_ddl
from kelp.catalog.function_ddl import generate_create_function_ddl
from kelp.catalog.metric_view_ddl import generate_create_metric_view_ddl
from kelp.catalog.query_builders import UCQueryBuilderFactory
from kelp.catalog.uc_diff import TableDiffCalculator
from kelp.catalog.uc_models import Model, RemoteCatalogConfig
from kelp.catalog.uc_query_builder import UCQueryBuilder
from kelp.config import get_context
from kelp.models.abac import AbacPolicy
from kelp.models.function import KelpFunction
from kelp.models.metric_view import MetricView as KelpMetricView
from kelp.models.model import Model as KelpModel
from kelp.models.project_config import ProjectConfig
from kelp.service.model_manager import ModelManager
from kelp.utils.databricks import get_table_from_dbx_sdk

logger = logging.getLogger(__name__)


class UnityCatalogAdapter:
    """Orchestrate diff detection and SQL generation for Unity Catalog syncs.

    Args:
        config: Sync configuration controlling which fields are managed. When
            omitted, the adapter loads configuration from the runtime context.
        query_builder: SQL generator. A default instance is created when not
            supplied.

    """

    def __init__(
        self,
        config: RemoteCatalogConfig | None = None,
        query_builder: UCQueryBuilder | None = None,
    ) -> None:
        self._config = config or self._get_config()
        self._differ = TableDiffCalculator(self._config)
        self._builder = query_builder or UCQueryBuilder()

    def _get_config(self) -> RemoteCatalogConfig:
        """Return the RemoteCatalogConfig from the runtime context.

        Returns:
            RemoteCatalogConfig instance mapped from project settings.

        """
        project_config: ProjectConfig = get_context().project_settings
        return project_config.remote_catalog_config.model_copy(deep=True)

    def _fetch_remote_table(self, fqn: str, profile: str | None = None) -> Model | None:
        """Fetch remote table state and convert to v2 model.

        Args:
            fqn: Fully-qualified table name.
            profile: Databricks CLI profile to use.

        Returns:
            Converted Table or None if missing.

        """
        return get_table_from_dbx_sdk(fqn, profile=profile)

    def _get_fqn(self, table: KelpModel) -> str:
        """Return the fully-qualified name for a Kelp Table."""
        return ModelManager.get_qualified_name_from_model(table)

    def sync_table(self, table: KelpModel, profile: str | None = None) -> list[str]:
        """Return SQL queries required to sync a single table.

        Args:
            table: Local table definition from the project catalog.
            profile: Databricks CLI profile to use for remote metadata lookups.

        Returns:
            Ordered list of SQL statements to execute.

        """
        fqn = self._get_fqn(table)
        remote = self._fetch_remote_table(fqn, profile=profile)

        if remote is None:
            logger.warning("Table '%s' not found in Unity Catalog; skipping sync.", fqn)
            return []

        local = table
        diff = self._differ.calculate(local, remote)
        logger.debug("Diff for '%s': %s", fqn, diff)
        return UCQueryBuilderFactory().build(
            fqn=fqn,
            diff=diff,
            table_type=_table_type_value(remote.table_type),
        )
        # return self._builder.build(fqn, diff, _table_type_value(remote.table_type))

    def sync_tables(
        self,
        tables: list[KelpModel],
        profile: str | None = None,
    ) -> list[str]:
        """Return SQL queries for all provided tables.

        Args:
            tables: Local table definitions to sync.
            profile: Databricks CLI profile to use for remote metadata lookups.

        Returns:
            Concatenated list of SQL statements for every table.

        """
        queries: list[str] = []
        for table in tables:
            queries.extend(self.sync_table(table, profile=profile))
        return queries

    def sync_all_tables(
        self,
        tables: list[KelpModel] | None = None,
        profile: str | None = None,
    ) -> list[str]:
        """Sync all tables from the current project context.

        Args:
            tables: Optional list of tables to sync. If omitted, all catalog
                tables from the runtime context are used.
            profile: Databricks CLI profile to use for remote metadata lookups.

        Returns:
            Ordered list of SQL statements to execute.

        """
        catalog_tables = tables or get_context().catalog_index.get_all("models")
        return self.sync_tables(catalog_tables, profile=profile)

    def sync_function(self, function: KelpFunction) -> list[str]:
        """Return SQL queries required to sync a single function.

        Functions are treated as pre-applied entities and currently use
        CREATE OR REPLACE semantics.
        """
        return [generate_create_function_ddl(function)]

    def sync_functions(self, functions: list[KelpFunction]) -> list[str]:
        """Return SQL queries for all provided functions."""
        queries: list[str] = []
        for function in functions:
            queries.extend(self.sync_function(function))
        return queries

    def sync_all_functions(self, functions: list[KelpFunction] | None = None) -> list[str]:
        """Sync all functions from the current project context."""
        catalog_functions = functions or get_context().catalog_index.get_all("functions")
        return self.sync_functions(catalog_functions)

    def sync_metric_view(
        self,
        metric_view: KelpMetricView,
        profile: str | None = None,
    ) -> list[str]:
        """Return SQL queries required to sync a single metric view.

        Detects changes in definition, description, and tags between local and
        remote metric views, then generates appropriate SQL statements.

        Args:
            metric_view: Local metric view definition from the project catalog.
            profile: Databricks CLI profile to use for remote metadata lookups.

        Returns:
            Ordered list of SQL statements to execute.

        """
        from kelp.utils.databricks import get_metric_view_from_dbx_sdk

        fqn = metric_view.get_qualified_name()
        statements: list[str] = []
        enforce_tags: bool = False
        try:
            remote = get_metric_view_from_dbx_sdk(fqn, profile=profile)
        except Exception:  # noqa: BLE001
            # logger.warning(
            #     "Metric view '%s' not found in Unity Catalog; skipping sync. Error: %s",
            #     fqn,
            #     e,
            # )
            # return []
            # if not exists switch to create mode
            remote = metric_view
            enforce_tags = True
            statements.extend([self.create_metric_view(metric_view)])

        # Normalize definitions for comparison (remove comment and tags as they're handled separately)
        def _normalize_for_comparison(definition: dict) -> dict:
            """Remove comment and tags fields from definition for comparison."""
            import copy

            normalized = copy.deepcopy(definition)
            normalized.pop("comment", None)

            # Remove tags from dimensions and measures
            if isinstance(normalized.get("dimensions"), list):
                for dim in normalized["dimensions"]:
                    if isinstance(dim, dict):
                        dim.pop("tags", None)

            if isinstance(normalized.get("measures"), list):
                for measure in normalized["measures"]:
                    if isinstance(measure, dict):
                        measure.pop("tags", None)

            return normalized

        local_def_normalized = _normalize_for_comparison(metric_view.definition)
        remote_def_normalized = _normalize_for_comparison(remote.definition)

        # Check if definition changed (excluding comment and tags which are handled separately)
        definition_changed = local_def_normalized != remote_def_normalized
        description_changed = metric_view.description != remote.description

        if definition_changed or description_changed:
            # Generate ALTER VIEW statement to update definition
            from kelp.catalog.metric_view_ddl import generate_alter_metric_view_definition_ddl

            ddl = generate_alter_metric_view_definition_ddl(metric_view)
            statements.append(ddl)
            logger.info("Definition or description changed for metric view '%s'", fqn)

        # Check column tags (dimensions and measures)
        from kelp.catalog.metric_view_ddl import generate_alter_metric_view_column_tags_ddl

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

        # Check tags diff using existing differ logic
        tag_diff = self._differ.diff_dicts(
            metric_view.tags,
            remote.tags if not enforce_tags else {},
            self._config.managed_table_tags,
            self._config.table_tag_mode,
        )

        if tag_diff.has_changes:
            # Generate ALTER VIEW statements for tags
            statements.extend(
                [
                    f"ALTER VIEW {fqn} SET TAGS ('{tag_key}' = '{tag_value}')"
                    for tag_key, tag_value in tag_diff.updates.items()
                ],
            )
            statements.extend(
                [f"ALTER VIEW {fqn} UNSET TAGS ('{tag_key}')" for tag_key in tag_diff.deletes],
            )
            logger.info(
                "Tags changed for metric view '%s': +%d / -%d",
                fqn,
                len(tag_diff.updates),
                len(tag_diff.deletes),
            )

        return statements

    def sync_metric_views(
        self,
        metric_views: list[KelpMetricView],
        profile: str | None = None,
    ) -> list[str]:
        """Return SQL queries for all provided metric views.

        Args:
            metric_views: Local metric view definitions to sync.
            profile: Databricks CLI profile to use for remote metadata lookups.

        Returns:
            Concatenated list of SQL statements for every metric view.

        """
        queries: list[str] = []
        for metric_view in metric_views:
            queries.extend(self.sync_metric_view(metric_view, profile=profile))
        return queries

    def sync_all_metric_views(
        self,
        metric_views: list[KelpMetricView] | None = None,
        profile: str | None = None,
    ) -> list[str]:
        """Sync all metric views from the current project context.

        Args:
            metric_views: Optional list of metric views to sync. If omitted,
                all metric views from the runtime context are used.
            profile: Databricks CLI profile to use for remote metadata lookups.

        Returns:
            Ordered list of SQL statements to execute.

        """
        catalog_metrics = metric_views or get_context().catalog_index.get_all("metric_views")
        return self.sync_metric_views(catalog_metrics, profile=profile)

    def create_metric_view(self, metric_view: KelpMetricView) -> str:
        """Generate DDL for creating a single metric view.

        Args:
            metric_view: Local metric view definition from the project catalog.

        Returns:
            SQL DDL statement to create the metric view.

        """
        return generate_create_metric_view_ddl(metric_view)

    def create_metric_views(self, metric_views: list[KelpMetricView]) -> list[str]:
        """Generate DDL for all provided metric views.

        Args:
            metric_views: Local metric view definitions to create.

        Returns:
            List of SQL DDL statements for every metric view.

        """
        statements: list[str] = []
        for metric_view in metric_views:
            try:
                stmt = self.create_metric_view(metric_view)
                statements.append(stmt)
            except Exception as e:  # noqa: BLE001
                logger.error(
                    "Failed to generate DDL for metric view '%s': %s",
                    metric_view.name,
                    e,
                )
        return statements

    def create_all_metric_views(
        self,
        metric_views: list[KelpMetricView] | None = None,
    ) -> list[str]:
        """Create all metric views from the current project context.

        Args:
            metric_views: Optional list of metric views to create. If omitted,
                all metric views from the runtime context are used.

        Returns:
            List of SQL DDL statements to execute.

        """
        catalog_metrics = metric_views or get_context().catalog_index.get_all("metric_views")
        return self.create_metric_views(catalog_metrics)

    def sync_abac_policy(self, policy: AbacPolicy) -> list[str]:
        """Return SQL queries required to sync a single ABAC policy."""
        return [generate_create_abac_policy_ddl(policy)]

    def sync_abac_policies(self, policies: list[AbacPolicy]) -> list[str]:
        """Return SQL queries for all provided ABAC policies."""
        queries: list[str] = []
        for policy in policies:
            queries.extend(self.sync_abac_policy(policy))
        return queries

    def sync_all_abac_policies(self, policies: list[AbacPolicy] | None = None) -> list[str]:
        """Sync all ABAC policies from the current project context."""
        catalog_policies = policies or get_context().catalog_index.get_all("abacs")
        return self.sync_abac_policies(catalog_policies)


def _table_type_value(table_type) -> str:
    """Normalize the table type to a string."""
    if hasattr(table_type, "value"):
        return table_type.value
    return str(table_type)
