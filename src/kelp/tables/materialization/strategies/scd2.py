"""SCD type 2 writes: full row history per key, reconciled with one Delta merge.

Algorithm
---------
Instead of replaying the batch sequence value by sequence value, the complete
history of every key the batch touches is rebuilt in one window pass and then
reconciled:

1. Determine the keys present in the batch.
2. Read the existing versions of exactly those keys from the target.
3. Union them with the incoming rows into a single per-key event history.
4. Close every version with the next version's sequence value (``lead``), dropping
   incoming versions that did not change and tombstones. The current version keeps
   ``NULL``, or the configured ``open_value`` sentinel.
5. Merge the rebuilt versions on ``(keys, valid_from)``: update the intervals whose
   ``valid_to`` changed, insert the versions that do not exist yet.

Late-arriving rows, several versions per key in one batch, deletes and
multi-column sequences all fall out of this and need no special handling: a late
row is simply a row in the middle of the rebuilt history. Rows that came from the
target are never collapsed, so existing history is preserved verbatim and the
merge can never leave an orphaned version behind. Re-running the same batch is a
no-op.
"""

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window, WindowSpec

from kelp.models.model_mat_config import TARGET_ALIAS, ColumnSelector, Scd2Columns, Scd2Config
from kelp.tables.materialization.base import (
    assert_no_nulls,
    ensure_table_created,
    table_exists,
    with_delete_flag,
    with_sequence,
)
from kelp.tables.materialization.columns import (
    HELPER_COLUMNS,
    IS_DELETE_COL,
    KEEP_COL,
    ORIGIN_COL,
    ROW_NUMBER_COL,
    SEQUENCE_COL,
    SUPERSEDED_AT_COL,
    index,
    resolve,
    without,
)
from kelp.tables.materialization.execute import execute_plan
from kelp.tables.materialization.plan import plan_scd2

logger = logging.getLogger(__name__)

_FROM_TARGET = 1
_FROM_SOURCE = 2


def run(
    *,
    spark: SparkSession,
    dataframe: DataFrame,
    target_name: str,
    config: Scd2Config,
    create_table_ddl: str | None = None,
    model_name: str | None = None,
) -> None:
    """Materialize the DataFrame as SCD2 history.

    Args:
        spark: Active SparkSession.
        dataframe: Source DataFrame.
        target_name: Target table name/FQN.
        config: Effective SCD2 configuration.
        create_table_ddl: Optional DDL used to create the target when missing.
        model_name: Optional model name for contextual logging.

    Raises:
        ValueError: If key or sequence columns are missing or contain NULLs, or if
            the existing target lacks the configured history columns.
    """
    history = config.history
    source_columns = without(dataframe.columns, [*HELPER_COLUMNS, *history.all_names()])
    source_index = index(source_columns)

    keys, missing_keys = resolve(config.keys, source_index)
    if missing_keys:
        raise ValueError(
            "Key column(s) missing from the source DataFrame: " + ", ".join(missing_keys)
        )
    sequence_by, missing_sequence = resolve(config.sequence_by, source_index)
    if missing_sequence:
        raise ValueError(
            "sequence_by column(s) missing from the source DataFrame: "
            + ", ".join(missing_sequence)
        )

    # Ordering is what makes history correct, so NULL keys or sequence values are
    # rejected rather than silently reordered.
    assert_no_nulls(dataframe, [*keys, *sequence_by])

    business_columns = (config.columns or ColumnSelector()).apply(source_columns, required=keys)
    tracked = (config.track_changes or ColumnSelector()).apply(without(business_columns, keys))

    source = with_delete_flag(with_sequence(dataframe, sequence_by), config.when_deleted)
    source_history = _as_history(source, business_columns, origin=_FROM_SOURCE)

    ensure_table_created(
        spark,
        target_name,
        create_table_ddl=create_table_ddl,
        model_name=model_name,
    )

    if not table_exists(spark, target_name):
        versions = build_versions(
            history=source_history,
            keys=keys,
            tracked=tracked,
            history_columns=history,
            carry_forward=config.ignore_null_columns(without(business_columns, keys)),
        )
        logger.info(
            "Target '%s' does not exist; creating it from the first SCD2 batch.", target_name
        )
        versions.write.format("delta").options(**config.options).mode("append").saveAsTable(
            target_name
        )
        return

    target = DeltaTable.forName(spark, target_name)
    target_df = target.toDF()

    existing = _existing_versions(
        target_df=target_df,
        source=source,
        keys=keys,
        business_columns=business_columns,
        history=history,
        where=config.where,
    )

    versions = build_versions(
        history=existing.unionByName(source_history, allowMissingColumns=True),
        keys=keys,
        tracked=tracked,
        history_columns=history,
        carry_forward=config.ignore_null_columns(without(business_columns, keys)),
    )

    plan = plan_scd2(
        config=config,
        write_columns=business_columns,
        target_columns=target_df.columns,
    )
    execute_plan(target=target, source=versions, plan=plan, model_name=model_name)


def _as_history(dataframe: DataFrame, business_columns: list[str], *, origin: int) -> DataFrame:
    """Project a DataFrame onto the shared history shape."""
    delete_flag = (
        f.coalesce(f.col(IS_DELETE_COL), f.lit(False))
        if IS_DELETE_COL in dataframe.columns
        else f.lit(False)
    )
    return dataframe.select(
        *[f.col(col) for col in business_columns],
        f.col(SEQUENCE_COL).alias(SEQUENCE_COL),
        delete_flag.alias(IS_DELETE_COL),
        f.lit(origin).alias(ORIGIN_COL),
    )


def _existing_versions(
    *,
    target_df: DataFrame,
    source: DataFrame,
    keys: list[str],
    business_columns: list[str],
    history: Scd2Columns,
    where: str | None,
) -> DataFrame:
    """Read the full existing history of the keys present in the batch.

    Only the affected keys are read (semi-join against the batch keys), which is
    what keeps the rebuild bounded. Cluster or partition the target by ``keys`` to
    let Delta prune files for this read.
    """
    target_index = index(target_df.columns)
    target_keys = [target_index[key.lower()] for key in keys]

    slice_df = target_df.alias(TARGET_ALIAS)
    if where:
        slice_df = slice_df.filter(where)

    affected_keys = source.select(
        *[f.col(key).alias(target_key) for key, target_key in zip(keys, target_keys, strict=True)]
    ).distinct()

    slice_df = slice_df.join(affected_keys, target_keys, "left_semi")

    present = [col for col in business_columns if col.lower() in target_index]
    return slice_df.select(
        *[f.col(target_index[col.lower()]).alias(col) for col in present],
        f.col(target_index[history.valid_from.lower()]).alias(SEQUENCE_COL),
        f.lit(False).alias(IS_DELETE_COL),
        f.lit(_FROM_TARGET).alias(ORIGIN_COL),
    )


def _carry_forward_nulls(
    history: DataFrame,
    *,
    ordered: WindowSpec,
    columns: list[str],
) -> DataFrame:
    """Replace NULLs with the last non-NULL value of the same key.

    Implements ``ignore_null_updates`` for history: a partial CDC row that omits a
    field keeps the value the previous version had instead of storing NULL.
    """
    fill = set(columns)
    return history.select(
        *[
            f.last(f.col(col), ignorenulls=True).over(ordered).alias(col)
            if col in fill
            else f.col(col)
            for col in history.columns
        ]
    )


def build_versions(
    *,
    history: DataFrame,
    keys: list[str],
    tracked: list[str],
    history_columns: Scd2Columns,
    carry_forward: list[str] | None = None,
) -> DataFrame:
    """Turn a per-key event history into closed SCD2 intervals.

    Args:
        history: Union of the existing versions and the incoming rows, carrying the
            sequence, delete-flag and origin helper columns.
        keys: Business key columns.
        tracked: Columns whose change starts a new version.
        history_columns: Names of the history columns to emit.
        carry_forward: Columns whose NULLs take the previous version's value
            (``ignore_null_updates``).

    Returns:
        One row per surviving version, with ``valid_from``/``valid_to`` (and
        ``is_current`` when configured) set and helper columns dropped. A current
        version's ``valid_to`` is ``NULL`` unless ``open_value`` is configured.
    """
    key_columns = [f.col(key) for key in keys]
    ordered = Window.partitionBy(*key_columns).orderBy(f.col(SEQUENCE_COL).asc())

    # One row per (key, sequence value); the incoming row wins over the stored one.
    tie_break = Window.partitionBy(*key_columns, f.col(SEQUENCE_COL)).orderBy(
        f.col(ORIGIN_COL).desc()
    )
    deduped = (
        history.withColumn(ROW_NUMBER_COL, f.row_number().over(tie_break))
        .filter(f.col(ROW_NUMBER_COL) == 1)
        .drop(ROW_NUMBER_COL)
    )

    if carry_forward:
        deduped = _carry_forward_nulls(deduped, ordered=ordered, columns=carry_forward)

    # Drop incoming versions identical to their predecessor. Stored versions are
    # never collapsed: existing history stays as it is, and the reconciling merge
    # can then never orphan a target row.
    snapshot = f.struct(*[f.col(col) for col in tracked]) if tracked else f.lit(1)
    previous = f.lag(snapshot).over(ordered)
    kept = deduped.withColumn(
        KEEP_COL,
        (f.col(ORIGIN_COL) == f.lit(_FROM_TARGET))
        | previous.isNull()
        | ~previous.eqNullSafe(snapshot)
        | f.col(IS_DELETE_COL)
        | f.coalesce(f.lag(f.col(IS_DELETE_COL)).over(ordered), f.lit(False)),
    ).filter(f.col(KEEP_COL))

    # Close every version with the next one's sequence value. Tombstones close the
    # preceding version and are then dropped.
    superseded_at = f.lead(f.col(SEQUENCE_COL)).over(ordered)
    closed = kept.withColumn(SUPERSEDED_AT_COL, superseded_at).withColumn(
        history_columns.valid_from, f.col(SEQUENCE_COL)
    )

    open_interval = f.col(SUPERSEDED_AT_COL).isNull()
    if history_columns.open_value:
        # A current version carries the configured sentinel instead of NULL.
        sequence_type = kept.schema[SEQUENCE_COL].dataType
        closed = closed.withColumn(
            history_columns.valid_to,
            f.coalesce(
                f.col(SUPERSEDED_AT_COL),
                f.expr(history_columns.open_value).cast(sequence_type),
            ),
        )
    else:
        closed = closed.withColumn(history_columns.valid_to, f.col(SUPERSEDED_AT_COL))

    if history_columns.is_current:
        closed = closed.withColumn(history_columns.is_current, open_interval)

    return closed.filter(~f.col(IS_DELETE_COL)).drop(
        SEQUENCE_COL, IS_DELETE_COL, ORIGIN_COL, KEEP_COL, SUPERSEDED_AT_COL
    )
