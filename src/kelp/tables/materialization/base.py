"""Shared Spark-side helpers for the materialization strategies."""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from kelp.tables.materialization.columns import IS_DELETE_COL, ROW_NUMBER_COL, SEQUENCE_COL

logger = logging.getLogger(__name__)


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """Return whether a table exists in the current Spark catalog.

    Args:
        spark: Active SparkSession.
        table_name: Fully qualified (preferred) or session-resolved table name.

    Returns:
        ``True`` if the table exists, else ``False``.
    """
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:  # noqa: BLE001
        logger.debug("Unable to determine table existence for '%s'.", table_name)
        return False


def ensure_table_created(
    spark: SparkSession,
    target_name: str,
    create_table_ddl: str | None = None,
    model_name: str | None = None,
) -> bool:
    """Create a missing target table from DDL when DDL is available.

    Args:
        spark: Active SparkSession.
        target_name: Target table name/FQN.
        create_table_ddl: Optional DDL statement used to create the target.
        model_name: Optional model name for contextual logging.

    Returns:
        ``True`` when the table exists after this call, else ``False``.

    Raises:
        RuntimeError: If the DDL exists but fails to execute. The DDL may carry
            generated columns and constraints that matter for correctness, so a
            failure is not silently ignored.
    """
    if table_exists(spark, target_name):
        return True

    if not create_table_ddl:
        return False

    try:
        spark.sql(create_table_ddl)
    except Exception as e:
        logger.warning(
            "Failed to create table '%s' for model '%s' using DDL: %s",
            target_name,
            model_name or target_name,
            create_table_ddl,
        )
        raise RuntimeError(
            f"Failed to create table '{target_name}' for model '{model_name or target_name}'."
        ) from e

    return table_exists(spark, target_name)


def with_sequence(dataframe: DataFrame, sequence_by: list[str]) -> DataFrame:
    """Add the effective sequence column.

    A single sequence column is copied as-is; multiple columns become a struct so
    that comparisons follow the configured column order.

    Args:
        dataframe: Source DataFrame.
        sequence_by: Configured sequence columns.

    Returns:
        DataFrame with the sequence helper column, unchanged when ``sequence_by``
        is empty.
    """
    if not sequence_by:
        return dataframe
    if len(sequence_by) == 1:
        return dataframe.withColumn(SEQUENCE_COL, f.col(sequence_by[0]))
    return dataframe.withColumn(SEQUENCE_COL, f.struct(*[f.col(col) for col in sequence_by]))


def with_delete_flag(dataframe: DataFrame, when_deleted: str | None) -> DataFrame:
    """Add the delete-flag column evaluated against the source only.

    Evaluating the predicate up front keeps it unambiguous inside merge clauses,
    where an unqualified column name could resolve to either side.

    Args:
        dataframe: Source DataFrame.
        when_deleted: SQL predicate marking tombstone rows, if configured.

    Returns:
        DataFrame with the delete-flag helper column, unchanged when
        ``when_deleted`` is ``None``.
    """
    if not when_deleted:
        return dataframe
    return dataframe.withColumn(IS_DELETE_COL, f.expr(when_deleted).cast("boolean"))


def latest_per_key(dataframe: DataFrame, keys: list[str], sequence_by: list[str]) -> DataFrame:
    """Reduce the batch to one row per key.

    Delta rejects a merge where several source rows match the same target row, so
    the batch is collapsed first: the newest row per key wins when sequence
    columns are configured, otherwise duplicates are dropped arbitrarily.

    Args:
        dataframe: Source DataFrame.
        keys: Key columns.
        sequence_by: Sequence columns, if any.

    Returns:
        DataFrame with unique keys.
    """
    if not sequence_by:
        return dataframe.dropDuplicates(keys)

    window = Window.partitionBy(*[f.col(key) for key in keys]).orderBy(
        *[f.col(col).desc_nulls_last() for col in sequence_by]
    )
    return (
        dataframe.withColumn(ROW_NUMBER_COL, f.row_number().over(window))
        .filter(f.col(ROW_NUMBER_COL) == 1)
        .drop(ROW_NUMBER_COL)
    )


def assert_no_nulls(dataframe: DataFrame, columns: list[str]) -> None:
    """Fail when any of ``columns`` contains NULL values.

    Args:
        dataframe: DataFrame to check.
        columns: Columns that must be fully populated.

    Raises:
        ValueError: If at least one column contains a NULL value.
    """
    if not columns:
        return

    counts = dataframe.agg(
        *[f.count(f.when(f.col(col).isNull(), f.lit(1))).alias(col) for col in columns]
    ).first()
    if counts is None:
        return

    offending = [col for col in columns if counts[col]]
    if offending:
        raise ValueError(
            "NULL values found in columns that must be populated: " + ", ".join(offending)
        )
