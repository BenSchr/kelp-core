import logging

from kelp.catalog.uc_dataper import UnityCatalogAdapter
from kelp.config.lifecycle import get_context

logger = logging.getLogger(f"{__name__}")


def sync_catalog() -> None:
    tables = get_context().catalog.get_tables()
    uc_adapter = UnityCatalogAdapter()
    logger.info("Starting remote catalog sync for all tables...")
    queries = uc_adapter.sync_all_tables(tables)
    return queries
    # logger.info("Remote catalog sync complete.")


def sync_table(table_name: str) -> None:
    table = get_context().catalog.get_table(table_name)
    uc_adapter = UnityCatalogAdapter()
    queries = uc_adapter.sync_table(table)
    logger.info(f"Remote catalog sync complete for table {table_name}.")
    return queries
