import logging

from kelp.catalog_legacy.uc_adapter import UnityCatalogAdapter
from kelp.config.lifecycle import get_context

logger = logging.getLogger(f"{__name__}")


def sync_catalog() -> list[str]:
    tables = get_context().catalog.get_tables()
    uc_adapter = UnityCatalogAdapter()
    logger.info("Starting remote catalog sync for all tables...")
    queries = uc_adapter.sync_all_tables(tables)
    return queries
    # logger.info("Remote catalog sync complete.")


def sync_table(table_name: str) -> list[str]:
    table = get_context().catalog.get_table(table_name)
    uc_adapter = UnityCatalogAdapter()
    queries = uc_adapter.sync_table(table)
    logger.info("Remote catalog sync complete for table %s.", table_name)
    return queries
