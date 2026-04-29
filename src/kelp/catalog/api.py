import logging
from typing import Any

from kelp.catalog.uc_adapter import UnityCatalogAdapter
from kelp.config import get_context

logger = logging.getLogger(f"{__name__}")


def _get_objects(
    catalog_key: str,
    names: list[str] | None = None,
    filter_by_meta: dict[str, Any] | None = None,
) -> list[Any]:
    """Fetch catalog objects, optionally narrowed by name list and/or meta filter.

    When both *names* and *filter_by_meta* are supplied they combine with AND
    semantics: only objects that satisfy the meta filter **and** appear in the
    name list are returned.

    Args:
        catalog_key: Catalog type key (e.g. "models", "metric_views").
        names: Optional explicit name list.  When non-empty, only objects whose
            ``name`` attribute is in the list are included.
        filter_by_meta: Optional dict filter applied against each object's
            ``meta`` field using recursive dict-subset matching via the
            generic ``MetaCatalog.filter_by`` method.

    Returns:
        Filtered list of catalog objects.
    """
    catalog_index = get_context().catalog_index

    if filter_by_meta:
        objects = catalog_index.filter_by(catalog_key, "meta", filter_by_meta)
    else:
        objects = catalog_index.get_all(catalog_key)

    if names:
        name_set = set(names)
        objects = [obj for obj in objects if getattr(obj, "name", None) in name_set]

    return objects


def sync_catalog(
    sync_functions: bool = False,
    sync_metric_views: bool = True,
    sync_tables: bool = True,
    sync_abacs: bool = True,
    filter_by_meta: dict[str, Any] | None = None,
    profile: str | None = None,
) -> list[str]:
    """Synchronize all tables and metric views to remote Databricks catalog.

    Synchronizes the entire local catalog (tables and metric views) to the remote
    Databricks Unity Catalog, applying any configuration such as tags and properties
    based on the remote_catalog_config settings.

    Args:
        sync_functions: If True, syncs all functions to remote catalog.
        sync_metric_views: If True, syncs all metric views to remote catalog.
        sync_tables: If True, syncs all tables to remote catalog.
        sync_abacs: If True, syncs all ABAC policies to remote catalog.
        filter_by_meta: Optional dict filter applied against each object's ``meta``
            field using recursive dict-subset matching.
        profile: Databricks CLI profile to use for remote metadata lookups.

    Returns:
        List of SQL queries executed for synchronization.

    Raises:
        Exception: If catalog synchronization fails (see UnityCatalogAdapter for details).
    """
    uc_adapter = UnityCatalogAdapter()
    logger.info("Starting remote catalog sync for all tables & metric views...")
    queries: list[str] = []
    if sync_functions:
        queries.extend(
            uc_adapter.sync_all_functions(_get_objects("functions", filter_by_meta=filter_by_meta))
        )
    if sync_tables:
        queries.extend(
            uc_adapter.sync_all_tables(
                _get_objects("models", filter_by_meta=filter_by_meta),
                profile=profile,
            )
        )
    if sync_metric_views:
        queries.extend(
            uc_adapter.sync_all_metric_views(
                _get_objects("metric_views", filter_by_meta=filter_by_meta),
                profile=profile,
            )
        )
    if sync_abacs:
        queries.extend(
            uc_adapter.sync_all_abac_policies(_get_objects("abacs", filter_by_meta=filter_by_meta))
        )

    return queries


def sync_functions(
    function_names: list[str] | None = None,
    filter_by_meta: dict[str, Any] | None = None,
) -> list[str]:
    """Synchronize specified functions to the remote catalog.

    Functions are pre-applied entities and are synced via CREATE OR REPLACE DDL.
    """
    functions = _get_objects("functions", names=function_names, filter_by_meta=filter_by_meta)
    uc_adapter = UnityCatalogAdapter()
    queries = uc_adapter.sync_all_functions(functions)
    return queries


def create_metric_views(
    view_names: list[str] | None = None,
    filter_by_meta: dict[str, Any] | None = None,
) -> list[str]:
    """Create specified metric views in the remote catalog.

    Creates metric views in Databricks Unity Catalog. If no specific view names
    are provided, all metric views from the catalog are created.

    Args:
        view_names: List of metric view names to create. If None, creates all views.
        filter_by_meta: Optional dict filter applied against each object's ``meta``
            field using recursive dict-subset matching.

    Returns:
        List of SQL queries executed to create metric views.

    Raises:
        KeyError: If a specified metric view name is not found in the catalog.
        Exception: If metric view creation fails (see UnityCatalogAdapter for details).
    """
    metric_views = _get_objects("metric_views", names=view_names, filter_by_meta=filter_by_meta)
    uc_adapter = UnityCatalogAdapter()
    logger.info("Starting remote catalog sync for all metric views...")
    queries = uc_adapter.create_all_metric_views(metric_views)
    return queries


def sync_metric_views(
    view_names: list[str] | None = None,
    filter_by_meta: dict[str, Any] | None = None,
    profile: str | None = None,
) -> list[str]:
    """Synchronize specified metric views to the remote catalog.

    Synchronizes metric views from the local catalog to Databricks Unity Catalog.
    If no specific view names are provided, all metric views are synchronized.

    Args:
        view_names: List of metric view names to sync. If None, syncs all views.
        filter_by_meta: Optional dict filter applied against each object's ``meta``
            field using recursive dict-subset matching.
        profile: Databricks CLI profile to use for remote metadata lookups.

    Returns:
        List of SQL queries executed for synchronization.

    Raises:
        KeyError: If a specified metric view name is not found in the catalog.
        Exception: If metric view sync fails (see UnityCatalogAdapter for details).
    """
    metric_views = _get_objects("metric_views", names=view_names, filter_by_meta=filter_by_meta)
    uc_adapter = UnityCatalogAdapter()
    logger.info("Starting sync for metric views...")
    queries = uc_adapter.sync_all_metric_views(metric_views, profile=profile)
    return queries


def sync_abac_policies(
    policy_names: list[str] | None = None,
    filter_by_meta: dict[str, Any] | None = None,
) -> list[str]:
    """Synchronize specified ABAC policies to the remote catalog."""
    policies = _get_objects("abacs", names=policy_names, filter_by_meta=filter_by_meta)
    uc_adapter = UnityCatalogAdapter()
    queries = uc_adapter.sync_abac_policies(policies)
    return queries


def sync_tables(
    model_names: list[str] | None = None,
    filter_by_meta: dict[str, Any] | None = None,
    profile: str | None = None,
) -> list[str]:
    """Synchronize specified tables to the remote catalog.

    Synchronizes tables from the local catalog to Databricks Unity Catalog,
    applying configuration such as tags and properties based on remote_catalog_config.
    If no specific table names are provided, all tables are synchronized.

    Args:
        model_names: List of table names to sync. If None, syncs all tables.
        filter_by_meta: Optional dict filter applied against each object's ``meta``
            field using recursive dict-subset matching.
        profile: Databricks CLI profile to use for remote metadata lookups.

    Returns:
        List of SQL queries executed for synchronization.

    Raises:
        KeyError: If a specified table name is not found in the catalog.
        Exception: If table sync fails (see UnityCatalogAdapter for details).
    """
    tables = _get_objects("models", names=model_names, filter_by_meta=filter_by_meta)
    uc_adapter = UnityCatalogAdapter()
    queries = uc_adapter.sync_tables(tables, profile=profile)
    return queries
