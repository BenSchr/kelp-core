"""Integration tests for SCD2 materialization against a real Delta target.

Scenarios cover ordered changes, out-of-order (late arriving) rows, deletes,
several versions per key in one batch, multi-column sequences, replay idempotence
and the metadata-free path.
"""

from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from kelp.config import init
from kelp.tables import ColumnSelector, Scd2Columns, Scd2Config, materialize

SOURCE_SCHEMA = "id BIGINT, name STRING, city STRING, sequence_num BIGINT"
CDC_SOURCE_SCHEMA = f"{SOURCE_SCHEMA}, _op STRING"


@pytest.fixture
def materializations_project_dir(fixtures_dir: Path) -> Path:
    """Return fixture project root for materialization integration tests."""
    return fixtures_dir / "materializations_project"


@pytest.fixture
def initialized_materializations_context(
    monkeypatch: pytest.MonkeyPatch,
    materializations_project_dir: Path,
) -> None:
    """Initialize kelp context from the real fixture project."""
    monkeypatch.chdir(materializations_project_dir)
    init(project_file_path=str(materializations_project_dir / "kelp_project.yml"), refresh=True)


def _drop(spark: SparkSession, table_name: str) -> None:
    """Drop a table if it exists."""
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def _versions(spark: SparkSession, table_name: str) -> list[tuple]:
    """Return history rows as (id, name, city, sequence_num, start, end), ordered."""
    df = spark.table(table_name).orderBy("id", "__START_AT")
    return [
        (
            row["id"],
            row["name"],
            row["city"],
            row["sequence_num"],
            row["__START_AT"],
            row["__END_AT"],
        )
        for row in df.collect()
    ]


def _write(
    spark: SparkSession,
    table_name: str,
    rows: list[tuple],
    schema: str = SOURCE_SCHEMA,
    config: Scd2Config | None = None,
    name: str | None = None,
) -> DataFrame:
    """Materialize a batch of source rows as SCD2 history."""
    source_df = spark.createDataFrame(rows, schema)
    return materialize(
        spark=spark,
        dataframe=source_df,
        name=name or table_name,
        config=config,
        options={"apply_vacuum": False, "apply_optimize": False},
    )


def test_scd2_creates_table_and_tracks_changes(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """The first batch bootstraps the table; the next one closes the changed version."""
    table_name = "mat_scd2_customers"
    _drop(spark, table_name)

    _write(spark, table_name, [(1, "Alice", "Berlin", 100), (2, "Bob", "Hamburg", 100)])

    assert _versions(spark, table_name) == [
        (1, "Alice", "Berlin", 100, 100, None),
        (2, "Bob", "Hamburg", 100, 100, None),
    ]

    _write(spark, table_name, [(1, "Alice", "Munich", 103)])

    assert _versions(spark, table_name) == [
        (1, "Alice", "Berlin", 100, 100, 103),
        (1, "Alice", "Munich", 103, 103, None),
        (2, "Bob", "Hamburg", 100, 100, None),
    ]


def test_scd2_ignores_unchanged_rows(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """A row whose tracked columns did not change does not create a new version."""
    table_name = "mat_scd2_customers"
    _drop(spark, table_name)

    _write(spark, table_name, [(1, "Alice", "Berlin", 100)])
    _write(spark, table_name, [(1, "Alice", "Berlin", 101)])

    assert _versions(spark, table_name) == [(1, "Alice", "Berlin", 100, 100, None)]


def test_scd2_ignore_null_updates_carries_values_forward(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """With ignore_null_updates, a partial CDC row keeps the previous version's values."""
    table_name = "default.mat_scd2_partial"
    _drop(spark, table_name)
    config = Scd2Config(
        keys=["id"],
        sequence_by=["sequence_num"],
        track_changes=ColumnSelector(exclude=["sequence_num"]),
        ignore_null_updates=True,
    )

    _write(spark, table_name, [(1, "Alice", "Berlin", 100)], config=config)
    _write(spark, table_name, [(1, None, "Munich", 103)], config=config)

    assert _versions(spark, table_name) == [
        (1, "Alice", "Berlin", 100, 100, 103),
        (1, "Alice", "Munich", 103, 103, None),
    ]


def test_scd2_stores_nulls_by_default(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """By default a NULL in the source is a real value change."""
    table_name = "default.mat_scd2_partial"
    _drop(spark, table_name)
    config = Scd2Config(
        keys=["id"],
        sequence_by=["sequence_num"],
        track_changes=ColumnSelector(exclude=["sequence_num"]),
    )

    _write(spark, table_name, [(1, "Alice", "Berlin", 100)], config=config)
    _write(spark, table_name, [(1, None, "Munich", 103)], config=config)

    assert _versions(spark, table_name) == [
        (1, "Alice", "Berlin", 100, 100, 103),
        (1, None, "Munich", 103, 103, None),
    ]


def test_scd2_replaying_a_batch_is_a_no_op(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """Re-running the same batch leaves the history untouched."""
    table_name = "mat_scd2_customers"
    _drop(spark, table_name)

    batch = [(1, "Alice", "Berlin", 100), (1, "Alice", "Munich", 103)]
    _write(spark, table_name, batch)
    before = _versions(spark, table_name)
    _write(spark, table_name, batch)

    assert _versions(spark, table_name) == before


def test_scd2_multiple_versions_in_one_batch(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """Several changes for one key in a single batch produce a closed chain."""
    table_name = "mat_scd2_customers"
    _drop(spark, table_name)

    _write(
        spark,
        table_name,
        [
            (1, "Alice", "Berlin", 100),
            (1, "Alice", "Munich", 103),
            (1, "Alice", "Zurich", 104),
        ],
    )

    assert _versions(spark, table_name) == [
        (1, "Alice", "Berlin", 100, 100, 103),
        (1, "Alice", "Munich", 103, 103, 104),
        (1, "Alice", "Zurich", 104, 104, None),
    ]


def test_scd2_late_arriving_row_is_inserted_into_history(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """An out-of-order row lands before the stored version and splits the interval.

    Mirrors the ``normal_scd2_late`` scenario: sequence 99 arrives after 100 is
    already the current version, together with a newer row at 101.
    """
    table_name = "mat_scd2_customers"
    _drop(spark, table_name)

    _write(spark, table_name, [(1, "Alice", "Berlin", 100)])
    _write(
        spark,
        table_name,
        [
            (1, "Alice", "Paris", 99),
            (1, "Alice", "Munich", 101),
            (2, "Bob", "Hamburg", 100),
        ],
    )

    assert _versions(spark, table_name) == [
        (1, "Alice", "Paris", 99, 99, 100),
        (1, "Alice", "Berlin", 100, 100, 101),
        (1, "Alice", "Munich", 101, 101, None),
        (2, "Bob", "Hamburg", 100, 100, None),
    ]


def test_scd2_late_arriving_row_splits_a_closed_interval(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """A late row inside an already closed interval splits it in two."""
    table_name = "mat_scd2_customers"
    _drop(spark, table_name)

    _write(spark, table_name, [(1, "Alice", "Berlin", 100), (1, "Alice", "Zurich", 104)])
    _write(spark, table_name, [(1, "Alice", "Munich", 102)])

    assert _versions(spark, table_name) == [
        (1, "Alice", "Berlin", 100, 100, 102),
        (1, "Alice", "Munich", 102, 102, 104),
        (1, "Alice", "Zurich", 104, 104, None),
    ]


def test_scd2_delete_closes_the_current_version(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """A tombstone closes the open interval without inserting a version of its own."""
    table_name = "default.mat_scd2_cdc_customers"
    _drop(spark, table_name)

    config = Scd2Config(
        keys=["id"],
        sequence_by=["sequence_num"],
        columns=ColumnSelector(exclude=["_op"]),
        track_changes=ColumnSelector(exclude=["sequence_num"]),
        when_deleted="_op = 'D'",
    )

    _write(
        spark,
        table_name,
        [(1, "Alice", "Berlin", 100, "U"), (2, "Bob", "Hamburg", 100, "U")],
        schema=CDC_SOURCE_SCHEMA,
        config=config,
    )
    _write(
        spark,
        table_name,
        [(1, None, None, 105, "D")],
        schema=CDC_SOURCE_SCHEMA,
        config=config,
    )

    assert _versions(spark, table_name) == [
        (1, "Alice", "Berlin", 100, 100, 105),
        (2, "Bob", "Hamburg", 100, 100, None),
    ]


def test_scd2_reinsert_after_delete_starts_a_new_version(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """A row that returns after a delete opens a new interval even with identical data."""
    table_name = "default.mat_scd2_cdc_customers"
    _drop(spark, table_name)

    config = Scd2Config(
        keys=["id"],
        sequence_by=["sequence_num"],
        columns=ColumnSelector(exclude=["_op"]),
        track_changes=ColumnSelector(exclude=["sequence_num"]),
        when_deleted="_op = 'D'",
    )

    _write(
        spark,
        table_name,
        [
            (1, "Alice", "Berlin", 100, "U"),
            (1, None, None, 105, "D"),
            (1, "Alice", "Berlin", 110, "U"),
        ],
        schema=CDC_SOURCE_SCHEMA,
        config=config,
    )

    assert _versions(spark, table_name) == [
        (1, "Alice", "Berlin", 100, 100, 105),
        (1, "Alice", "Berlin", 110, 110, None),
    ]


def test_scd2_multi_column_sequence(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """Two sequence columns order versions as a struct, and bound the intervals as one."""
    table_name = "default.mat_scd2_multi_sequence"
    _drop(spark, table_name)

    schema = StructType(
        [
            StructField("tenant_id", LongType(), False),
            StructField("id", LongType(), False),
            StructField("city", StringType(), True),
            StructField("sequence_num", LongType(), False),
            StructField("event_rank", LongType(), False),
        ]
    )
    config = Scd2Config(
        keys=["tenant_id", "id"],
        sequence_by=["sequence_num", "event_rank"],
        track_changes=ColumnSelector(include=["city"]),
    )

    spark.createDataFrame(
        [
            (10, 1, "Berlin", 100, 1),
            (10, 1, "Munich", 103, 1),
        ],
        schema,
    ).transform(
        lambda df: materialize(
            spark=spark,
            dataframe=df,
            name=table_name,
            config=config,
            options={"apply_vacuum": False, "apply_optimize": False},
        )
    )

    spark.createDataFrame([(10, 1, "Zurich", 103, 2)], schema).transform(
        lambda df: materialize(
            spark=spark,
            dataframe=df,
            name=table_name,
            config=config,
            options={"apply_vacuum": False, "apply_optimize": False},
        )
    )

    rows = [
        (row["city"], tuple(row["__START_AT"]), tuple(row["__END_AT"]) if row["__END_AT"] else None)
        for row in spark.table(table_name).orderBy("__START_AT").collect()
    ]
    assert rows == [
        ("Berlin", (100, 1), (103, 1)),
        ("Munich", (103, 1), (103, 2)),
        ("Zurich", (103, 2), None),
    ]


def test_scd2_custom_history_columns_and_is_current(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """History column names are configurable and is_current is maintained."""
    table_name = "default.mat_scd2_named_history"
    _drop(spark, table_name)

    config = Scd2Config(
        keys=["id"],
        sequence_by=["sequence_num"],
        track_changes=ColumnSelector(exclude=["sequence_num"]),
        history=Scd2Columns(valid_from="valid_from", valid_to="valid_to", is_current="is_current"),
    )

    _write(spark, table_name, [(1, "Alice", "Berlin", 100)], config=config)
    _write(spark, table_name, [(1, "Alice", "Munich", 103)], config=config)

    rows = [
        (row["city"], row["valid_from"], row["valid_to"], row["is_current"])
        for row in spark.table(table_name).orderBy("valid_from").collect()
    ]
    assert rows == [
        ("Berlin", 100, 103, False),
        ("Munich", 103, None, True),
    ]


def test_scd2_open_value_replaces_null_valid_to(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """An open_value sentinel closes current versions instead of leaving NULL."""
    table_name = "default.mat_scd2_open_value"
    _drop(spark, table_name)

    config = Scd2Config(
        keys=["id"],
        sequence_by=["sequence_num"],
        track_changes=ColumnSelector(exclude=["sequence_num"]),
        history=Scd2Columns(is_current="is_current", open_value="9999999999"),
    )

    _write(spark, table_name, [(1, "Alice", "Berlin", 100)], config=config)

    assert [
        (row["city"], row["__START_AT"], row["__END_AT"], row["is_current"])
        for row in spark.table(table_name).collect()
    ] == [("Berlin", 100, 9999999999, True)]

    # The sentinel is replaced by the real bound once the version is superseded.
    _write(spark, table_name, [(1, "Alice", "Munich", 103)], config=config)

    assert [
        (row["city"], row["__START_AT"], row["__END_AT"], row["is_current"])
        for row in spark.table(table_name).orderBy("__START_AT").collect()
    ] == [
        ("Berlin", 100, 103, False),
        ("Munich", 103, 9999999999, True),
    ]


def test_scd2_open_value_with_timestamp_sequence(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """The sentinel is cast to the sequence type, so date-like bounds work."""
    table_name = "default.mat_scd2_open_ts"
    _drop(spark, table_name)

    config = Scd2Config(
        keys=["id"],
        sequence_by=["updated_at"],
        track_changes=ColumnSelector(include=["city"]),
        history=Scd2Columns(
            valid_from="valid_from", valid_to="valid_to", open_value="date'2999-12-31'"
        ),
    )
    schema = "id BIGINT, city STRING, updated_at DATE"

    _write(
        spark,
        table_name,
        [(1, "Berlin", date(2026, 1, 1))],
        schema=schema,
        config=config,
    )
    _write(
        spark,
        table_name,
        [(1, "Munich", date(2026, 3, 1))],
        schema=schema,
        config=config,
    )

    assert [
        (row["city"], row["valid_from"], row["valid_to"])
        for row in spark.table(table_name).orderBy("valid_from").collect()
    ] == [
        ("Berlin", date(2026, 1, 1), date(2026, 3, 1)),
        ("Munich", date(2026, 3, 1), date(2999, 12, 31)),
    ]


def test_scd2_rejects_null_sequence_values(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """NULL keys or sequence values would silently reorder history, so they are rejected."""
    table_name = "mat_scd2_customers"
    _drop(spark, table_name)

    with pytest.raises(ValueError, match="NULL values"):
        _write(spark, table_name, [(1, "Alice", "Berlin", None)])


def test_scd2_works_without_kelp_metadata(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """SCD2 runs on a qualified table name with no kelp project in sight."""
    monkeypatch.chdir(tmp_path)
    table_name = "default.mat_scd2_standalone"
    _drop(spark, table_name)

    config = Scd2Config(
        keys=["id"], sequence_by=["sequence_num"], track_changes=ColumnSelector(include=["city"])
    )

    _write(spark, table_name, [(1, "Alice", "Berlin", 100)], config=config)
    _write(spark, table_name, [(1, "Alice", "Munich", 103)], config=config)

    assert _versions(spark, table_name) == [
        (1, "Alice", "Berlin", 100, 100, 103),
        (1, "Alice", "Munich", 103, 103, None),
    ]
