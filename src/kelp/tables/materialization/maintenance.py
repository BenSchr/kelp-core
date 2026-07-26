"""Table upkeep around a materialization write: full-refresh resets, OPTIMIZE,
VACUUM and catalog metadata sync.

Every statement here interpolates a table name, so all of them go through
:func:`~kelp.tables.materialization.resolve.quoted_identifier`.
"""

import logging
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from kelp.tables.materialization.base import table_exists
from kelp.tables.materialization.resolve import (
    FullRefreshStrategy,
    ResolvedMaterializationInputs,
    quoted_identifier,
)

if TYPE_CHECKING:
    from kelp.service.model_manager import KelpModel

logger = logging.getLogger(__name__)


def prepare_full_refresh(
    *,
    spark: SparkSession,
    resolved: ResolvedMaterializationInputs,
    strategy: FullRefreshStrategy,
) -> None:
    """Reset the target so the following write rebuilds it from scratch.

    ``drop`` removes the table entirely and leaves recreation to the write path,
    from DDL or from the first batch. Note that in Unity Catalog this also discards
    grants, tags, comments and table history, and leaves no table at all if the
    rebuild then fails.

    ``replace`` keeps the table's identity: with kelp metadata the DDL is
    re-applied as ``CREATE OR REPLACE TABLE``, which resets schema and contents
    atomically; without metadata the table is truncated, which resets contents and
    leaves the existing schema for the write to evolve.

    Args:
        spark: Active SparkSession.
        resolved: Resolved materialization inputs.
        strategy: How to reset the target.

    Raises:
        RuntimeError: If the reset statement fails.
    """
    target_name = resolved.target_name
    quoted = quoted_identifier(target_name)

    if strategy == "drop":
        logger.info("Full refresh requested for '%s', dropping table", target_name)
        statement = f"DROP TABLE IF EXISTS {quoted}"
    elif resolved.replace_table_ddl:
        logger.info("Full refresh requested for '%s', replacing table from DDL", target_name)
        statement = resolved.replace_table_ddl
    elif table_exists(spark, target_name):
        logger.info(
            "Full refresh requested for '%s', truncating table (no DDL available)", target_name
        )
        statement = f"TRUNCATE TABLE {quoted}"
    else:
        logger.info(
            "Full refresh requested for '%s' but it does not exist yet; nothing to reset.",
            target_name,
        )
        return

    try:
        spark.sql(statement)
    except Exception as e:
        raise RuntimeError(
            f"Failed to reset table '{target_name}' for full refresh (strategy '{strategy}'): {e!s}"
        ) from e


def perform_maintenance(
    spark: SparkSession,
    target_name: str,
    apply_vacuum: bool = True,
    vacuum_lite: bool = True,
    apply_optimize: bool = True,
) -> None:
    """Run OPTIMIZE and VACUUM on the materialized table.

    Failures are logged and swallowed: upkeep must not fail a write that already
    succeeded.

    Args:
        spark: Active SparkSession.
        target_name: Table to maintain.
        apply_vacuum: Whether to run VACUUM.
        vacuum_lite: Whether VACUUM uses LITE mode.
        apply_optimize: Whether to run OPTIMIZE.
    """
    quoted = quoted_identifier(target_name)

    if apply_optimize:
        try:
            spark.sql(f"OPTIMIZE {quoted}")
            logger.info("OPTIMIZE completed for %s", target_name)
        except Exception as e:  # noqa: BLE001
            logger.warning("OPTIMIZE failed for %s: %s", target_name, str(e)[:500])

    if apply_vacuum:
        query = f"VACUUM {quoted}"
        if vacuum_lite:
            query += " LITE"
        try:
            spark.sql(query)
            logger.info("VACUUM completed for %s", target_name)
        except Exception as e:  # noqa: BLE001
            logger.warning("VACUUM failed for %s: %s", target_name, str(e)[:500])


def sync_metadata(spark: SparkSession, model: "KelpModel") -> None:
    """Sync catalog metadata for the materialized model's table.

    Skipped with a log line when not running on Databricks. Individual failures are
    logged and swallowed for the same reason as :func:`perform_maintenance`.

    Args:
        spark: Active SparkSession.
        model: The materialized model.
    """
    from kelp.utils.databricks import on_databricks

    if not on_databricks():
        logger.info(
            "Skipping metadata sync for model '%s' since not running on Databricks",
            model.name,
        )
        return

    from kelp.catalog import sync_tables

    for query in sync_tables([model.name]):
        try:
            spark.sql(query)
            logger.info("%s | %s", model.name, query)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "Metadata sync query failed for model '%s': %s\nQuery: %s",
                model.name,
                str(e)[:500],
                query,
            )
