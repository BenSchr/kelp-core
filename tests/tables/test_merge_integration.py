"""Integration tests for merge materialization paths not covered by the YAML fixtures."""

from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from kelp.tables import ColumnSelector, MergeConfig, OverwriteConfig, materialize

SCHEMA = "id BIGINT, name STRING, updated_at BIGINT"
CDC_SCHEMA = f"{SCHEMA}, _op STRING"


def _drop(spark: SparkSession, table_name: str) -> None:
    """Drop a table if it exists."""
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def _rows(spark: SparkSession, table_name: str) -> list[tuple]:
    """Return (id, name, updated_at) rows ordered by key."""
    return [
        (row["id"], row["name"], row["updated_at"])
        for row in spark.table(table_name).orderBy("id").collect()
    ]


def _table_property(spark: SparkSession, table_name: str, key: str) -> str | None:
    """Return a table property value, or None when it is not set."""
    rows = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
    return next((row["value"] for row in rows if row["key"] == key), None)


def _write(
    spark: SparkSession,
    table_name: str,
    rows: list[tuple],
    config: MergeConfig | OverwriteConfig,
    schema: str = SCHEMA,
) -> DataFrame:
    """Materialize a batch with the given config."""
    return materialize(
        spark=spark,
        dataframe=spark.createDataFrame(rows, schema),
        name=table_name,
        config=config,
        options={"apply_vacuum": False, "apply_optimize": False},
    )


@pytest.fixture
def standalone(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Run from a directory with no kelp project, so targets must be named in full."""
    monkeypatch.chdir(tmp_path)


def test_merge_creates_target_from_first_batch(
    spark: SparkSession,
    standalone: None,
) -> None:
    """Without a table and without DDL, the first batch creates the target deduplicated."""
    table_name = "default.mrg_first_batch"
    _drop(spark, table_name)

    _write(
        spark,
        table_name,
        [(1, "a", 10), (1, "a2", 11), (2, "b", 10)],
        MergeConfig(keys=["id"], sequence_by=["updated_at"]),
    )

    assert _rows(spark, table_name) == [(1, "a2", 11), (2, "b", 10)]


def test_merge_without_kelp_metadata(
    spark: SparkSession,
    standalone: None,
) -> None:
    """Merge works on a qualified table name with a config built in code."""
    table_name = "default.mrg_standalone"
    _drop(spark, table_name)
    config = MergeConfig(keys=["id"], sequence_by=["updated_at"])

    _write(spark, table_name, [(1, "a", 10), (2, "b", 10)], config)
    _write(spark, table_name, [(1, "a2", 11), (3, "c", 12)], config)

    assert _rows(spark, table_name) == [(1, "a2", 11), (2, "b", 10), (3, "c", 12)]


def test_merge_ignores_out_of_order_rows(
    spark: SparkSession,
    standalone: None,
) -> None:
    """A row older than the stored one does not overwrite it."""
    table_name = "default.mrg_out_of_order"
    _drop(spark, table_name)
    config = MergeConfig(keys=["id"], sequence_by=["updated_at"])

    _write(spark, table_name, [(1, "current", 20)], config)
    _write(spark, table_name, [(1, "stale", 19)], config)

    assert _rows(spark, table_name) == [(1, "current", 20)]


def test_merge_deletes_rows_missing_in_source(
    spark: SparkSession,
    standalone: None,
) -> None:
    """missing_in_source='delete' turns the batch into a full snapshot."""
    table_name = "default.mrg_missing_in_source"
    _drop(spark, table_name)

    _write(spark, table_name, [(1, "a", 10), (2, "b", 10)], MergeConfig(keys=["id"]))
    _write(
        spark,
        table_name,
        [(1, "a2", 11)],
        MergeConfig(keys=["id"], missing_in_source="delete"),
    )

    assert _rows(spark, table_name) == [(1, "a2", 11)]


def test_merge_cdc_delete_and_column_selection(
    spark: SparkSession,
    standalone: None,
) -> None:
    """Tombstones delete matched rows and bookkeeping columns stay out of the target."""
    table_name = "default.mrg_cdc"
    _drop(spark, table_name)
    config = MergeConfig(
        keys=["id"],
        sequence_by=["updated_at"],
        columns=ColumnSelector(exclude=["_op"]),
        when_deleted="_op = 'D'",
    )

    _write(
        spark,
        table_name,
        [(1, "a", 10, "U"), (2, "b", 10, "U")],
        config,
        schema=CDC_SCHEMA,
    )
    _write(
        spark,
        table_name,
        [(1, "a2", 11, "U"), (2, None, 12, "D"), (3, "c", 12, "U")],
        config,
        schema=CDC_SCHEMA,
    )

    assert "_op" not in spark.table(table_name).columns
    assert _rows(spark, table_name) == [(1, "a2", 11), (3, "c", 12)]


def test_merge_insert_only_columns_are_preserved(
    spark: SparkSession,
    standalone: None,
) -> None:
    """An insert-only column keeps its original value on later updates."""
    table_name = "default.mrg_insert_only"
    _drop(spark, table_name)
    config = MergeConfig(keys=["id"], insert_only_columns=["updated_at"])

    _write(spark, table_name, [(1, "a", 10)], config)
    _write(spark, table_name, [(1, "a2", 99)], config)

    assert _rows(spark, table_name) == [(1, "a2", 10)]


def test_overwrite_replace_where_scopes_the_write(
    spark: SparkSession,
    standalone: None,
) -> None:
    """replaceWhere replaces only the matching partition of the target."""
    table_name = "default.ov_replace_where"
    _drop(spark, table_name)

    spark.createDataFrame([(1, "a", 10), (2, "b", 20)], SCHEMA).write.format("delta").partitionBy(
        "updated_at"
    ).mode("overwrite").saveAsTable(table_name)

    _write(
        spark,
        table_name,
        [(3, "c", 20)],
        OverwriteConfig(replace_where="updated_at = 20"),
    )

    assert _rows(spark, table_name) == [(1, "a", 10), (3, "c", 20)]


def test_full_refresh_can_be_denied(
    spark: SparkSession,
    standalone: None,
) -> None:
    """allow_full_refresh=false protects the target from a requested rebuild."""
    table_name = "default.mrg_protected"
    _drop(spark, table_name)
    config = MergeConfig(keys=["id"], allow_full_refresh=False)

    _write(spark, table_name, [(1, "a", 10)], config)
    materialize(
        spark=spark,
        dataframe=spark.createDataFrame([(2, "b", 20)], SCHEMA),
        name=table_name,
        config=config,
        full_refresh=True,
        options={"apply_vacuum": False, "apply_optimize": False},
    )

    assert _rows(spark, table_name) == [(1, "a", 10), (2, "b", 20)]


def test_full_refresh_rebuilds_the_target(
    spark: SparkSession,
    standalone: None,
) -> None:
    """The default drop strategy discards the previous contents and the table itself."""
    table_name = "default.mrg_refreshed"
    _drop(spark, table_name)
    config = MergeConfig(keys=["id"])

    _write(spark, table_name, [(1, "a", 10)], config)
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('kelp.test.marker' = 'keep')")

    materialize(
        spark=spark,
        dataframe=spark.createDataFrame([(2, "b", 20)], SCHEMA),
        name=table_name,
        config=config,
        full_refresh=True,
        options={"apply_vacuum": False, "apply_optimize": False},
    )

    assert _rows(spark, table_name) == [(2, "b", 20)]
    # The table was dropped and rebuilt, so anything attached to it is gone.
    assert _table_property(spark, table_name, "kelp.test.marker") is None


def test_full_refresh_replace_keeps_the_table(
    spark: SparkSession,
    standalone: None,
) -> None:
    """The replace strategy resets contents without dropping the table."""
    table_name = "default.mrg_replaced"
    _drop(spark, table_name)
    config = MergeConfig(keys=["id"])

    _write(spark, table_name, [(1, "a", 10)], config)
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('kelp.test.marker' = 'keep')")

    materialize(
        spark=spark,
        dataframe=spark.createDataFrame([(2, "b", 20)], SCHEMA),
        name=table_name,
        config=config,
        full_refresh=True,
        full_refresh_strategy="replace",
        options={"apply_vacuum": False, "apply_optimize": False},
    )

    assert _rows(spark, table_name) == [(2, "b", 20)]
    # Without DDL the table is truncated, so it keeps its identity and properties.
    assert _table_property(spark, table_name, "kelp.test.marker") == "keep"


def test_full_refresh_replace_on_a_missing_table_is_a_no_op(
    spark: SparkSession,
    standalone: None,
) -> None:
    """Replacing a target that does not exist yet still materializes the batch."""
    table_name = "default.mrg_replaced_missing"
    _drop(spark, table_name)

    materialize(
        spark=spark,
        dataframe=spark.createDataFrame([(1, "a", 10)], SCHEMA),
        name=table_name,
        config=MergeConfig(keys=["id"]),
        full_refresh=True,
        full_refresh_strategy="replace",
        options={"apply_vacuum": False, "apply_optimize": False},
    )

    assert _rows(spark, table_name) == [(1, "a", 10)]


def test_merge_rejects_missing_keys(
    spark: SparkSession,
    standalone: None,
) -> None:
    """A key that does not exist in the batch fails before anything is written."""
    table_name = "default.mrg_missing_keys"
    _drop(spark, table_name)

    with pytest.raises(ValueError, match="Key column"):
        _write(spark, table_name, [(1, "a", 10)], MergeConfig(keys=["order_id"]))
