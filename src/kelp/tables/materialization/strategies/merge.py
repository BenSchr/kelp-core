"""Merge (SCD type 1) writes via a single Delta merge."""

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from kelp.models.model_mat_config import ColumnSelector, MergeConfig
from kelp.tables.materialization.base import (
    ensure_table_created,
    latest_per_key,
    table_exists,
    with_delete_flag,
)
from kelp.tables.materialization.columns import (
    HELPER_COLUMNS,
    IS_DELETE_COL,
    index,
    resolve,
    without,
)
from kelp.tables.materialization.execute import execute_plan
from kelp.tables.materialization.plan import plan_merge

logger = logging.getLogger(__name__)


def run(
    *,
    spark: SparkSession,
    dataframe: DataFrame,
    target_name: str,
    config: MergeConfig,
    create_table_ddl: str | None = None,
    model_name: str | None = None,
) -> None:
    """Merge the DataFrame into the target, keeping one row per key.

    The batch is first collapsed to one row per key (newest wins when
    ``sequence_by`` is set), then a single merge applies deletes, updates and
    inserts. When the target does not exist yet it is created from the first batch.

    Args:
        spark: Active SparkSession.
        dataframe: Source DataFrame.
        target_name: Target table name/FQN.
        config: Effective merge configuration.
        create_table_ddl: Optional DDL used to create the target when missing.
        model_name: Optional model name for contextual logging.

    Raises:
        ValueError: If key columns are missing from the source DataFrame.
    """
    source_columns = without(dataframe.columns, list(HELPER_COLUMNS))
    keys, missing_keys = resolve(config.keys, index(source_columns))
    if missing_keys:
        raise ValueError(
            "Key column(s) missing from the source DataFrame: " + ", ".join(missing_keys)
        )

    source = with_delete_flag(dataframe, config.when_deleted)
    source = latest_per_key(source, keys, config.sequence_by)

    ensure_table_created(
        spark,
        target_name,
        create_table_ddl=create_table_ddl,
        model_name=model_name,
    )

    if not table_exists(spark, target_name):
        _create_from_first_batch(
            source=source,
            target_name=target_name,
            config=config,
            source_columns=source_columns,
            keys=keys,
        )
        return

    target = DeltaTable.forName(spark, target_name)
    plan = plan_merge(
        config=config,
        source_columns=source_columns,
        target_columns=target.toDF().columns,
    )
    execute_plan(target=target, source=source, plan=plan, model_name=model_name)


def _create_from_first_batch(
    *,
    source: DataFrame,
    target_name: str,
    config: MergeConfig,
    source_columns: list[str],
    keys: list[str],
) -> None:
    """Create the target from the first batch when neither table nor DDL exists."""
    logger.info(
        "Target '%s' does not exist and no DDL is available; creating it from the first batch.",
        target_name,
    )
    write_columns = (config.columns or ColumnSelector()).apply(source_columns, required=keys)
    first_batch = source
    if config.when_deleted:
        first_batch = first_batch.filter(~f.coalesce(f.col(IS_DELETE_COL), f.lit(False)))

    first_batch.select(*write_columns).write.format("delta").options(**config.options).mode(
        "append"
    ).saveAsTable(target_name)
