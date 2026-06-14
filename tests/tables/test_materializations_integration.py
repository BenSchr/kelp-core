"""Integration tests for append/overwrite/merge materializations using real YAML fixtures."""

from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from kelp.config import init
from kelp.models.model_mat_config import ModelMaterializationConfig
from kelp.tables import MaterializedContext, materialize, materialized


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


def _csv_df(spark: SparkSession, path: Path) -> DataFrame:
    """Load a CSV fixture into a DataFrame with inferred schema."""
    return spark.read.option("header", "true").option("inferSchema", "true").csv(str(path))


def _drop_table_if_exists(spark: SparkSession, table_name: str) -> None:
    """Drop a Spark table if it exists."""
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def _normalized_rows(df: DataFrame) -> list[tuple]:
    """Normalize DataFrame rows for deterministic equality checks."""
    ordered_columns = sorted(df.columns)
    return [tuple(row[c] for c in ordered_columns) for row in df.select(*ordered_columns).collect()]


def _assert_table_matches_csv(
    spark: SparkSession,
    table_name: str,
    expected_csv_path: Path,
) -> None:
    """Assert table contents match expected CSV regardless of row order."""
    actual_df = spark.table(table_name)
    expected_df = _csv_df(spark, expected_csv_path)

    assert set(actual_df.columns) == set(expected_df.columns)
    assert sorted(_normalized_rows(actual_df)) == sorted(_normalized_rows(expected_df))


def test_append_materialization_from_yaml(
    spark: SparkSession,
    materializations_project_dir: Path,
    initialized_materializations_context: None,
) -> None:
    """Append mode should add fixture source rows to existing target rows."""

    table_name = "mat_append_orders"
    _drop_table_if_exists(spark, table_name)

    target_df = _csv_df(spark, materializations_project_dir / "data/append/target.csv")
    target_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    source_df = _csv_df(spark, materializations_project_dir / "data/append/source.csv")
    materialize(
        spark=spark,
        dataframe=source_df,
        name=table_name,
        config=None,
    )

    _assert_table_matches_csv(
        spark,
        table_name,
        materializations_project_dir / "data/append/result.csv",
    )


def test_materialize_creates_table_when_missing(
    spark: SparkSession,
    materializations_project_dir: Path,
    initialized_materializations_context: None,
) -> None:
    """Materialize should create a missing target table during append writes."""
    table_name = "mat_runtime_create_orders"
    _drop_table_if_exists(spark, table_name)

    source_df = _csv_df(spark, materializations_project_dir / "data/append/source.csv")
    materialize(
        spark=spark,
        dataframe=source_df,
        name=table_name,
        config=ModelMaterializationConfig(write_mode="append"),
    )

    assert spark.catalog.tableExists(table_name)
    _assert_table_matches_csv(
        spark,
        table_name,
        materializations_project_dir / "data/append/source.csv",
    )


def test_overwrite_materialization_from_yaml(
    spark: SparkSession,
    materializations_project_dir: Path,
    initialized_materializations_context: None,
) -> None:
    """Overwrite mode should replace pre-existing target data with source rows."""
    table_name = "mat_overwrite_orders"
    _drop_table_if_exists(spark, table_name)

    target_df = _csv_df(spark, materializations_project_dir / "data/overwrite/target.csv")
    target_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    source_df = _csv_df(spark, materializations_project_dir / "data/overwrite/source.csv")
    materialize(
        spark=spark,
        dataframe=source_df,
        name=table_name,
        config=None,
    )

    _assert_table_matches_csv(
        spark, table_name, materializations_project_dir / "data/overwrite/result.csv"
    )


def test_merge_materialization_from_yaml(
    spark: SparkSession,
    materializations_project_dir: Path,
    initialized_materializations_context: None,
) -> None:
    """Merge mode should upsert source rows into the target using YAML unique_keys."""
    table_name = "mat_merge_orders"
    _drop_table_if_exists(spark, table_name)

    target_df = _csv_df(spark, materializations_project_dir / "data/merge/target.csv")
    target_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    source_df = _csv_df(spark, materializations_project_dir / "data/merge/source.csv")
    materialize(
        spark=spark,
        dataframe=source_df,
        name=table_name,
        config=None,
    )

    _assert_table_matches_csv(
        spark,
        table_name,
        materializations_project_dir / "data/merge/result.csv",
    )


def test_materialized_decorator_integration(
    spark: SparkSession,
    materializations_project_dir: Path,
    initialized_materializations_context: None,
) -> None:
    """Decorator should call materialize and persist rows using model-backed config."""
    table_name = "mat_append_orders"
    _drop_table_if_exists(spark, table_name)

    target_df = _csv_df(spark, materializations_project_dir / "data/append/target.csv")
    target_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    @materialized(name=table_name)
    def build_df() -> DataFrame:
        return _csv_df(spark, materializations_project_dir / "data/append/source.csv")

    result_df = build_df()
    assert result_df.count() == 2

    _assert_table_matches_csv(
        spark,
        table_name,
        materializations_project_dir / "data/append/result.csv",
    )


def test_materialized_decorator_without_model_uses_runtime_config(
    spark: SparkSession,
    materializations_project_dir: Path,
    initialized_materializations_context: None,
) -> None:
    """Decorator should fully work with runtime config when no model exists."""
    table_name = "mat_runtime_decorator_orders"
    _drop_table_if_exists(spark, table_name)

    target_df = _csv_df(spark, materializations_project_dir / "data/append/target.csv")
    target_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    @materialized(
        name=table_name,
        config=ModelMaterializationConfig(write_mode="append"),
        apply_vacuum=False,
        apply_optimize=False,
    )
    def build_df() -> DataFrame:
        return _csv_df(spark, materializations_project_dir / "data/append/source.csv")

    result_df = build_df()
    assert result_df.count() == 2

    _assert_table_matches_csv(
        spark,
        table_name,
        materializations_project_dir / "data/append/result.csv",
    )


def test_materialized_decorator_merge_schema_evolution_updates_existing_rows(
    spark: SparkSession,
    initialized_materializations_context: None,
) -> None:
    """Decorator merge should evolve schema and apply new column values on matched updates."""
    table_name = "mat_runtime_merge_schema_evolution_orders"
    _drop_table_if_exists(spark, table_name)

    spark.createDataFrame(
        [
            (1, "old_1", 10),
            (2, "old_2", 20),
        ],
        "id BIGINT, name STRING, updated_at BIGINT",
    ).write.format("delta").mode("overwrite").saveAsTable(table_name)

    @materialized(
        name=table_name,
        config=ModelMaterializationConfig(
            write_mode="merge",
            unique_keys=["id"],
            sequence_by=["updated_at"],
            merge_with_schema_evolution=True,
        ),
        apply_vacuum=False,
        apply_optimize=False,
    )
    def build_df() -> DataFrame:
        return spark.createDataFrame(
            [
                (1, "new_1", 11, "gold"),
                (2, "stale_2", 19, "stale"),
                (3, "new_3", 30, "new"),
            ],
            "id BIGINT, name STRING, updated_at BIGINT, status STRING",
        )

    result_df = build_df()
    assert result_df.count() == 3

    actual_df = spark.table(table_name)
    assert "status" in actual_df.columns

    rows = {
        row["id"]: (row["name"], row["updated_at"], row["status"])
        for row in actual_df.select("id", "name", "updated_at", "status").collect()
    }

    assert rows == {
        1: ("new_1", 11, "gold"),
        2: ("old_2", 20, None),
        3: ("new_3", 30, "new"),
    }


def test_materialized_decorator_ctx_incremental_append(
    spark: SparkSession,
    materializations_project_dir: Path,
    initialized_materializations_context: None,
) -> None:
    """Decorator ctx should support incremental append filtered by max target timestamp."""
    table_name = "mat_append_incremental_orders"
    _drop_table_if_exists(spark, table_name)

    target_df = _csv_df(spark, materializations_project_dir / "data/append_incremental/target.csv")
    target_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    @materialized(name=table_name, apply_vacuum=False, apply_optimize=False)
    def build_incremental_df(ctx: MaterializedContext) -> DataFrame:
        source_df = _csv_df(
            spark, materializations_project_dir / "data/append_incremental/source.csv"
        )
        if ctx.is_incremental():
            max_ts = (
                ctx.spark.table(ctx.this)
                .agg(f.max(f.col("event_ts")).alias("max_event_ts"))
                .collect()[0]["max_event_ts"]
            )
            return source_df.filter(f.col("event_ts") > f.lit(max_ts))
        return source_df

    from kelp.tables import Runner

    runner = Runner()
    result_df = runner.run_one(table_name)
    # result_df = build_incremental_df()
    assert result_df.count() == 2

    _assert_table_matches_csv(
        spark,
        table_name,
        materializations_project_dir / "data/append_incremental/result.csv",
    )

    import logging

    logger = logging.getLogger(__name__)
    logger.info(runner.runlog)
    for entry in runner.runlog.entries:
        logger.info(entry)


def test_runner_multi_step_refinement_pipeline(
    spark: SparkSession,
    materializations_project_dir: Path,
    initialized_materializations_context: None,
) -> None:
    """Runner should execute dependent steps where each step refines prior-step data."""
    from kelp.tables.materialization.runner import _REGISTRY

    append_table = "mat_append_orders"
    overwrite_table = "mat_overwrite_orders"
    merge_table = "mat_merge_orders"

    _drop_table_if_exists(spark, append_table)
    _drop_table_if_exists(spark, overwrite_table)
    _drop_table_if_exists(spark, merge_table)

    spark.createDataFrame([], "id BIGINT, name STRING").write.format("delta").mode(
        "overwrite"
    ).saveAsTable(append_table)
    spark.createDataFrame([], "id BIGINT, name STRING").write.format("delta").mode(
        "overwrite"
    ).saveAsTable(overwrite_table)
    spark.createDataFrame([], "id BIGINT, name STRING, updated_at BIGINT").write.format(
        "delta"
    ).mode("overwrite").saveAsTable(merge_table)

    registry_snapshot = dict(_REGISTRY)
    _REGISTRY.clear()

    try:

        @materialized(
            name=append_table,
            apply_vacuum=False,
            apply_optimize=False,
        )
        def bronze_step(ctx: MaterializedContext) -> DataFrame:
            source_df = _csv_df(spark, materializations_project_dir / "data/append/source.csv")
            return source_df.select(
                f.col("id").cast("bigint").alias("id"),
                f.lower(f.col("name")).alias("name"),
            )

        @materialized(
            name=overwrite_table,
            depends_on=[append_table],
            apply_vacuum=False,
            apply_optimize=False,
        )
        def silver_step(ctx: MaterializedContext) -> DataFrame:
            previous_df = spark.read.table(append_table)
            return (
                previous_df.filter(f.col("id") > f.lit(1))
                .withColumn("name", f.concat(f.lit("silver_"), f.col("name")))
                .withColumn("id", f.col("id").cast("bigint"))
                .select("id", "name")
            )

        @materialized(
            name=merge_table,
            depends_on=[overwrite_table],
            apply_vacuum=False,
            apply_optimize=False,
        )
        def gold_step(ctx: MaterializedContext) -> DataFrame:
            previous_df = spark.read.table(overwrite_table)
            return (
                previous_df.withColumn("name", f.concat(f.col("name"), f.lit("_gold")))
                .withColumn("id", f.col("id").cast("bigint"))
                .withColumn("updated_at", (f.col("id") * f.lit(100)).cast("bigint"))
                .withColumn("_op", f.lit("U"))
                .select("id", "name", "updated_at", "_op")
            )

        # Keep local references to avoid function redefinition lint warnings.
        _ = bronze_step, silver_step, gold_step

        from kelp.tables import Runner

        runner = Runner()

        assert runner.plan_all() == [append_table, overwrite_table, merge_table]

        runner.run_all()

        assert [entry.model for entry in runner.runlog.entries] == [
            append_table,
            overwrite_table,
            merge_table,
        ]
        assert all(entry.status == "success" for entry in runner.runlog.entries)

        append_rows = {row["id"]: row["name"] for row in spark.table(append_table).collect()}
        assert append_rows == {1: "alice", 2: "bob"}

        overwrite_rows = {row["id"]: row["name"] for row in spark.table(overwrite_table).collect()}
        assert overwrite_rows == {2: "silver_bob"}

        merge_rows = [
            (row["id"], row["name"], row["updated_at"])
            for row in spark.table(merge_table).collect()
        ]
        assert merge_rows == [(2, "silver_bob_gold", 200)]

    finally:
        _REGISTRY.clear()
        _REGISTRY.update(registry_snapshot)
