"""Unity Catalog adapter (v2) that integrates with Kelp runtime context."""

import logging

from kelp.catalog.query_builders import UCQueryBuilderFactory
from kelp.catalog.query_builders.metric_view import generate_create_metric_view_ddl
from kelp.catalog.remote_fetchers import DEFAULT_ENGINE, RemoteFetcherFactory
from kelp.catalog.uc_diff import TableDiffCalculator
from kelp.catalog.uc_models import Model, RemoteCatalogConfig
from kelp.config import get_context
from kelp.models.abac import AbacPolicy
from kelp.models.function import KelpFunction
from kelp.models.metric_view import MetricView as KelpMetricView
from kelp.models.model import Model as KelpModel
from kelp.models.project_config import ProjectConfig
from kelp.service.model_manager import ModelManager

logger = logging.getLogger(__name__)


class UnityCatalogAdapter:
    """Orchestrate diff detection and SQL generation for Unity Catalog syncs.

    Args:
        config: Sync configuration controlling which fields are managed. When
            omitted, the adapter loads configuration from the runtime context.

    """

    def __init__(
        self,
        config: RemoteCatalogConfig | None = None,
    ) -> None:
        self._config = config or self._get_config()
        self._differ = TableDiffCalculator(self._config)

    def _get_config(self) -> RemoteCatalogConfig:
        """Return the RemoteCatalogConfig from the runtime context.

        Returns:
            RemoteCatalogConfig instance mapped from project settings.

        """
        project_config: ProjectConfig = get_context().project_settings
        return project_config.remote_catalog_config.model_copy(deep=True)

    def _fetch_remote_table(
        self, fqn: str, profile: str | None = None, engine: str = DEFAULT_ENGINE
    ) -> Model | None:
        """Fetch remote table state and convert to v2 model.

        Args:
            fqn: Fully-qualified table name.
            profile: Databricks CLI profile to use.
            engine: Fetch engine — ``"spark"`` (default, requires an active
                Spark session; see ``kelp.catalog.remote_fetchers.spark`` for
                caveats) or ``"sdk"`` (works anywhere, used by the CLI).

        Returns:
            Converted Table or None if missing.

        """
        return RemoteFetcherFactory().get_table_fetcher(engine).fetch(fqn, profile=profile)

    def _get_fqn(self, table: KelpModel) -> str:
        """Return the fully-qualified name for a Kelp Table."""
        return ModelManager.get_qualified_name_from_model(table)

    def sync_table(
        self,
        table: KelpModel,
        profile: str | None = None,
        engine: str = DEFAULT_ENGINE,
    ) -> list[str]:
        """Return SQL queries required to sync a single table.

        Args:
            table: Local table definition from the project catalog.
            profile: Databricks CLI profile to use for remote metadata lookups.
            engine: Remote-fetch engine — ``"spark"`` (default) or ``"sdk"``.

        Returns:
            Ordered list of SQL statements to execute.

        """
        fqn = self._get_fqn(table)
        remote = self._fetch_remote_table(fqn, profile=profile, engine=engine)

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

    def sync_tables(
        self,
        tables: list[KelpModel],
        profile: str | None = None,
        engine: str = DEFAULT_ENGINE,
    ) -> list[str]:
        """Return SQL queries for all provided tables.

        Args:
            tables: Local table definitions to sync.
            profile: Databricks CLI profile to use for remote metadata lookups.
            engine: Remote-fetch engine — ``"spark"`` (default) or ``"sdk"``.

        Returns:
            Concatenated list of SQL statements for every table.

        """
        queries: list[str] = []
        for table in tables:
            queries.extend(self.sync_table(table, profile=profile, engine=engine))
        return queries

    def sync_all_tables(
        self,
        tables: list[KelpModel] | None = None,
        profile: str | None = None,
        engine: str = DEFAULT_ENGINE,
    ) -> list[str]:
        """Sync all tables from the current project context.

        Args:
            tables: Optional list of tables to sync. If omitted, all catalog
                tables from the runtime context are used.
            profile: Databricks CLI profile to use for remote metadata lookups.
            engine: Remote-fetch engine — ``"spark"`` (default) or ``"sdk"``.

        Returns:
            Ordered list of SQL statements to execute.

        """
        catalog_tables = tables or get_context().catalog_index.get_all("models")
        return self.sync_tables(catalog_tables, profile=profile, engine=engine)

    def sync_function(self, function: KelpFunction) -> list[str]:
        """Return SQL queries required to sync a single function.

        Functions are treated as pre-applied entities and currently use
        CREATE OR REPLACE semantics.
        """
        return UCQueryBuilderFactory().get_builder("function").build(function)

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
        engine: str = DEFAULT_ENGINE,
    ) -> list[str]:
        """Return SQL queries required to sync a single metric view.

        Detects changes in definition, description, and tags between local and
        remote metric views, then generates appropriate SQL statements.

        Args:
            metric_view: Local metric view definition from the project catalog.
            profile: Databricks CLI profile to use for remote metadata lookups.
            engine: Remote-fetch engine — ``"spark"`` (default) or ``"sdk"``.

        Returns:
            Ordered list of SQL statements to execute.

        """
        fetcher = RemoteFetcherFactory().get_metric_view_fetcher(engine)
        remote = fetcher.fetch(metric_view.get_qualified_name(), profile=profile)
        builder = UCQueryBuilderFactory().get_builder("metric_view", config=self._config)
        return builder.build(metric_view, remote)

    def sync_metric_views(
        self,
        metric_views: list[KelpMetricView],
        profile: str | None = None,
        engine: str = DEFAULT_ENGINE,
    ) -> list[str]:
        """Return SQL queries for all provided metric views.

        Args:
            metric_views: Local metric view definitions to sync.
            profile: Databricks CLI profile to use for remote metadata lookups.
            engine: Remote-fetch engine — ``"spark"`` (default) or ``"sdk"``.

        Returns:
            Concatenated list of SQL statements for every metric view.

        """
        queries: list[str] = []
        for metric_view in metric_views:
            queries.extend(self.sync_metric_view(metric_view, profile=profile, engine=engine))
        return queries

    def sync_all_metric_views(
        self,
        metric_views: list[KelpMetricView] | None = None,
        profile: str | None = None,
        engine: str = DEFAULT_ENGINE,
    ) -> list[str]:
        """Sync all metric views from the current project context.

        Args:
            metric_views: Optional list of metric views to sync. If omitted,
                all metric views from the runtime context are used.
            profile: Databricks CLI profile to use for remote metadata lookups.
            engine: Remote-fetch engine — ``"spark"`` (default) or ``"sdk"``.

        Returns:
            Ordered list of SQL statements to execute.

        """
        catalog_metrics = metric_views or get_context().catalog_index.get_all("metric_views")
        return self.sync_metric_views(catalog_metrics, profile=profile, engine=engine)

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
        return UCQueryBuilderFactory().get_builder("abac").build(policy)

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
