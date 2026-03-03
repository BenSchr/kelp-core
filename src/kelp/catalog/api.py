import logging

from kelp.catalog.uc_adapter import UnityCatalogAdapter
from kelp.config.lifecycle import get_context

logger = logging.getLogger(f"{__name__}")


def sync_catalog(
    sync_functions: bool = False,
    sync_metric_views: bool = True,
    sync_tables: bool = True,
    sync_abacs: bool = True,
) -> list[str]:
    """Synchronize all tables and metric views to remote Databricks catalog.

    Synchronizes the entire local catalog (tables and metric views) to the remote
    Databricks Unity Catalog, applying any configuration such as tags and properties
    based on the remote_catalog_config settings.

    Args:
        sync_metric_views: If True, syncs all metric views to remote catalog.
        sync_tables: If True, syncs all tables to remote catalog.

    Returns:
        List of SQL queries executed for synchronization.

    Raises:
        Exception: If catalog synchronization fails (see UnityCatalogAdapter for details).
    """
    tables = get_context().catalog.get_tables()
    uc_adapter = UnityCatalogAdapter()
    logger.info("Starting remote catalog sync for all tables & metric views...")
    queries: list[str] = []
    if sync_functions:
        queries.extend(uc_adapter.sync_all_functions(get_context().catalog.get_functions()))
    if sync_tables:
        queries.extend(uc_adapter.sync_all_tables(tables))
    if sync_metric_views:
        queries.extend(uc_adapter.sync_all_metric_views(get_context().catalog.get_metric_views()))
    if sync_abacs:
        queries.extend(uc_adapter.sync_all_abac_policies(get_context().catalog.get_abacs()))

    return queries


def sync_functions(function_names: list[str] | None = None) -> list[str]:
    """Synchronize specified functions to the remote catalog.

    Functions are pre-applied entities and are synced via CREATE OR REPLACE DDL.
    """
    function_names = function_names or []
    functions = [f for f in get_context().catalog.get_functions() if f.name in function_names]
    uc_adapter = UnityCatalogAdapter()
    queries = uc_adapter.sync_all_functions(functions)
    return queries


def create_metric_views(view_names: list[str] | None = None) -> list[str]:
    """Create specified metric views in the remote catalog.

    Creates metric views in Databricks Unity Catalog. If no specific view names
    are provided, all metric views from the catalog are created.

    Args:
        view_names: List of metric view names to create. If None, creates all views.

    Returns:
        List of SQL queries executed to create metric views.

    Raises:
        KeyError: If a specified metric view name is not found in the catalog.
        Exception: If metric view creation fails (see UnityCatalogAdapter for details).
    """
    view_names = view_names or []
    metric_views = [mv for mv in get_context().catalog.get_metric_views() if mv.name in view_names]
    uc_adapter = UnityCatalogAdapter()
    logger.info("Starting remote catalog sync for all metric views...")
    queries = uc_adapter.create_all_metric_views(metric_views)
    return queries


def sync_metric_views(view_names: list[str] | None = None) -> list[str]:
    """Synchronize specified metric views to the remote catalog.

    Synchronizes metric views from the local catalog to Databricks Unity Catalog.
    If no specific view names are provided, all metric views are synchronized.

    Args:
        view_names: List of metric view names to sync. If None, syncs all views.

    Returns:
        List of SQL queries executed for synchronization.

    Raises:
        KeyError: If a specified metric view name is not found in the catalog.
        Exception: If metric view sync fails (see UnityCatalogAdapter for details).
    """
    view_names = view_names or []
    metric_views = [mv for mv in get_context().catalog.get_metric_views() if mv.name in view_names]
    uc_adapter = UnityCatalogAdapter()
    logger.info("Starting sync for metric views...")
    queries = uc_adapter.sync_all_metric_views(metric_views)
    return queries


def sync_abac_policies(policy_names: list[str] | None = None) -> list[str]:
    """Synchronize specified ABAC policies to the remote catalog."""
    policy_names = policy_names or []
    policies = [p for p in get_context().catalog.get_abacs() if p.name in policy_names]
    uc_adapter = UnityCatalogAdapter()
    queries = uc_adapter.sync_abac_policies(policies)
    return queries


def sync_tables(table_names: list[str] | None = None) -> list[str]:
    """Synchronize specified tables to the remote catalog.

    Synchronizes tables from the local catalog to Databricks Unity Catalog,
    applying configuration such as tags and properties based on remote_catalog_config.
    If no specific table names are provided, all tables are synchronized.

    Args:
        table_names: List of table names to sync. If None, syncs all tables.

    Returns:
        List of SQL queries executed for synchronization.

    Raises:
        KeyError: If a specified table name is not found in the catalog.
        Exception: If table sync fails (see UnityCatalogAdapter for details).
    """
    table_names = table_names or []
    tables = [t for t in get_context().catalog.get_tables() if t.name in table_names]
    uc_adapter = UnityCatalogAdapter()
    queries = uc_adapter.sync_tables(tables)
    return queries
