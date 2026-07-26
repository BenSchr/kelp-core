"""Append and overwrite writes."""

import logging

from pyspark.sql import DataFrame, SparkSession

from kelp.models.model_mat_config import AppendConfig, OverwriteConfig
from kelp.tables.materialization.base import ensure_table_created

logger = logging.getLogger(__name__)


def run(
    *,
    spark: SparkSession,
    dataframe: DataFrame,
    target_name: str,
    config: AppendConfig | OverwriteConfig,
    create_table_ddl: str | None = None,
    model_name: str | None = None,
) -> None:
    """Write the DataFrame with Delta append or overwrite semantics.

    When DDL is available and the target is missing, the table is created from DDL
    first so generated columns and constraints are preserved.

    Args:
        spark: Active SparkSession.
        dataframe: DataFrame to write.
        target_name: Target table name/FQN.
        config: Effective append or overwrite configuration.
        create_table_ddl: Optional DDL used to create the target when missing.
        model_name: Optional model name for contextual logging.
    """
    ensure_table_created(
        spark,
        target_name,
        create_table_ddl=create_table_ddl,
        model_name=model_name,
    )

    writer = dataframe.write.format("delta").options(**config.options)
    if isinstance(config, OverwriteConfig) and config.replace_where:
        writer = writer.option("replaceWhere", config.replace_where)

    writer.mode(config.mode).saveAsTable(target_name)
