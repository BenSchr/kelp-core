import logging

from kelp.catalog.uc_adapter import UnityCatalogAdapter
from kelp.config.lifecycle import get_context

logger = logging.getLogger(f"{__name__}")


def sync_catalog(
    create_metric_views: bool = True,
    sync_metric_views: bool = True,
    sync_tables: bool = True,
) -> list[str]:
    tables = get_context().catalog.get_tables()
    uc_adapter = UnityCatalogAdapter()
    logger.info("Starting remote catalog sync for all tables & metric views...")
    queries: list[str] = []
    if create_metric_views:
        queries.extend(uc_adapter.create_all_metric_views(get_context().catalog.get_metric_views()))
    if sync_metric_views:
        queries.extend(uc_adapter.sync_all_metric_views(get_context().catalog.get_metric_views()))
    if sync_tables:
        queries.extend(uc_adapter.sync_all_tables(tables))
    return queries


def create_metric_views(view_names: list[str] | None = None) -> list[str]:
    view_names = view_names or []
    metric_views = [mv for mv in get_context().catalog.get_metric_views() if mv.name in view_names]
    uc_adapter = UnityCatalogAdapter()
    logger.info("Starting remote catalog sync for all metric views...")
    queries = uc_adapter.create_all_metric_views(metric_views)
    return queries


def sync_metric_views(view_names: list[str] | None = None) -> list[str]:
    view_names = view_names or []
    metric_views = [mv for mv in get_context().catalog.get_metric_views() if mv.name in view_names]
    uc_adapter = UnityCatalogAdapter()
    logger.info("Starting sync for metric views...")
    queries = uc_adapter.sync_all_metric_views(metric_views)
    return queries


def sync_tables(table_names: list[str] | None = None) -> list[str]:
    table_names = table_names or []
    tables = [t for t in get_context().catalog.get_tables() if t.name in table_names]
    uc_adapter = UnityCatalogAdapter()
    queries = uc_adapter.sync_tables(tables)
    return queries
