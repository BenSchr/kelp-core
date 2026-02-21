"""Unity Catalog adapter (v2) that integrates with Kelp runtime context."""

from __future__ import annotations

import logging

from kelp.catalog.uc_diff import TableDiffCalculator
from kelp.catalog.uc_models import RemoteCatalogConfig, Table
from kelp.catalog.uc_query_builder import UCQueryBuilder
from kelp.config.lifecycle import get_context
from kelp.models.table import Table as KelpTable
from kelp.service.table_manager import TableManager
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
        ctx_config = get_context().project_config.remote_catalog_config
        return ctx_config.model_copy(deep=True)

    def _fetch_remote_table(self, fqn: str) -> Table | None:
        """Fetch remote table state and convert to v2 model.

        Args:
            fqn: Fully-qualified table name.

        Returns:
            Converted Table or None if missing.
        """
        return get_table_from_dbx_sdk(fqn)

    def _get_fqn(self, table: KelpTable) -> str:
        """Return the fully-qualified name for a Kelp Table."""
        return TableManager.get_qualified_tablename_from_table(table)

    def sync_table(self, table: KelpTable) -> list[str]:
        """Return SQL queries required to sync a single table.

        Args:
            table: Local table definition from the project catalog.

        Returns:
            Ordered list of SQL statements to execute.
        """
        fqn = self._get_fqn(table)
        remote = self._fetch_remote_table(fqn)

        if remote is None:
            logger.warning(
                "Table '%s' not found in Unity Catalog; skipping sync.", fqn)
            return []

        local = table
        diff = self._differ.calculate(local, remote)
        logger.debug("Diff for '%s': %s", fqn, diff)

        return self._builder.build(fqn, diff, _table_type_value(remote.table_type))

    def sync_tables(self, tables: list[KelpTable]) -> list[str]:
        """Return SQL queries for all provided tables.

        Args:
            tables: Local table definitions to sync.

        Returns:
            Concatenated list of SQL statements for every table.
        """
        queries: list[str] = []
        for table in tables:
            queries.extend(self.sync_table(table))
        return queries

    def sync_all_tables(self, tables: list[KelpTable] | None = None) -> list[str]:
        """Sync all tables from the current project context.

        Args:
            tables: Optional list of tables to sync. If omitted, all catalog
                tables from the runtime context are used.

        Returns:
            Ordered list of SQL statements to execute.
        """
        catalog_tables = tables or get_context().catalog.get_tables()
        return self.sync_tables(catalog_tables)


def _table_type_value(table_type) -> str:
    """Normalize the table type to a string."""
    if hasattr(table_type, "value"):
        return table_type.value
    return str(table_type)
