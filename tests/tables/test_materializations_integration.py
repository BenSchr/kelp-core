"""Integration tests for append/overwrite/merge materializations using real YAML fixtures."""

from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from kelp.config import init
from kelp.tables import MaterializedContext, materialized
from kelp.tables.materialization.factory import materialize


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
        table_name=table_name,
        config=None,
    )

    _assert_table_matches_csv(
        spark,
        table_name,
        materializations_project_dir / "data/append/result.csv",
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
        table_name=table_name,
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
        table_name=table_name,
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

    from kelp.tables.materialization.runner import Runner

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
