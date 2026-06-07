"""Integration tests for DQX quality checks during materialization."""

from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from kelp.config import init
from kelp.models.model import DQXQuality
from kelp.tables import materialize
from kelp.tables.materialization import factory as materialization_factory


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


def _row_count(spark: SparkSession, table_name: str) -> int:
    """Return row count for the given table."""
    return spark.table(table_name).count()


def _patch_dqx_config(
    monkeypatch: pytest.MonkeyPatch,
    target_table: str,
    dqx_quality: DQXQuality,
) -> None:
    """Patch the resolved DQX quality for a specific table during materialization."""
    original = materialization_factory._resolve_materialization_inputs

    def _patched(*, table_name: str, config):
        resolved = original(table_name=table_name, config=config)
        if table_name == target_table:
            resolved.dqx_quality = dqx_quality
        return resolved

    monkeypatch.setattr(materialization_factory, "_resolve_materialization_inputs", _patched)


def test_materialize_quality_checks_drop_with_quarantine(
    spark: SparkSession,
    materializations_project_dir: Path,
    initialized_materializations_context: None,
) -> None:
    """Drop mode with quarantine enabled should write bad rows to quarantine table."""
    table_name = "mat_append_quality_orders"
    quarantine_table = f"{table_name}_quarantine"

    _drop_table_if_exists(spark, table_name)
    _drop_table_if_exists(spark, quarantine_table)
    _drop_table_if_exists(spark, "default.dqx_metrics")

    target_df = _csv_df(spark, materializations_project_dir / "data/append/target.csv")
    target_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    source_df = _csv_df(spark, materializations_project_dir / "data/append/source.csv")
    materialize(
        spark=spark,
        dataframe=source_df,
        name=table_name,
        config=None,
        apply_vacuum=False,
        apply_optimize=False,
    )

    assert _row_count(spark, table_name) == 2
    assert spark.catalog.tableExists(quarantine_table)
    assert _row_count(spark, quarantine_table) == 1


def test_materialize_quality_checks_drop_without_quarantine(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    materializations_project_dir: Path,
    initialized_materializations_context: None,
) -> None:
    """Drop mode without quarantine should silently discard bad rows."""
    table_name = "mat_append_quality_orders"
    quarantine_table = f"{table_name}_quarantine"

    _drop_table_if_exists(spark, table_name)
    _drop_table_if_exists(spark, quarantine_table)

    target_df = _csv_df(spark, materializations_project_dir / "data/append/target.csv")
    target_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    _patch_dqx_config(
        monkeypatch,
        table_name,
        DQXQuality(
            engine="dqx",
            checks=[
                {
                    "check": {
                        "function": "is_not_equal_to",
                        "arguments": {"column": "id", "value": 1},
                    }
                }
            ],
            spark_violation_action="drop",
            spark_quarantine=False,
        ),
    )

    source_df = _csv_df(spark, materializations_project_dir / "data/append/source.csv")
    materialize(
        spark=spark,
        dataframe=source_df,
        name=table_name,
        config=None,
        apply_vacuum=False,
        apply_optimize=False,
    )

    assert _row_count(spark, table_name) == 2
    assert not spark.catalog.tableExists(quarantine_table)


def test_materialize_quality_checks_error_mode_raises(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    materializations_project_dir: Path,
    initialized_materializations_context: None,
) -> None:
    """Error violation action should raise before writing when bad rows are present."""
    table_name = "mat_append_quality_orders"
    quarantine_table = f"{table_name}_quarantine"

    _drop_table_if_exists(spark, table_name)
    _drop_table_if_exists(spark, quarantine_table)

    target_df = _csv_df(spark, materializations_project_dir / "data/append/target.csv")
    target_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    _patch_dqx_config(
        monkeypatch,
        table_name,
        DQXQuality(
            engine="dqx",
            checks=[
                {
                    "check": {
                        "function": "is_not_equal_to",
                        "arguments": {"column": "id", "value": 1},
                    }
                }
            ],
            spark_violation_action="error",
            spark_quarantine=False,
        ),
    )

    source_df = _csv_df(spark, materializations_project_dir / "data/append/source.csv")

    with pytest.raises(ValueError, match="Data quality checks failed"):
        materialize(
            spark=spark,
            dataframe=source_df,
            name=table_name,
            config=None,
            apply_vacuum=False,
            apply_optimize=False,
        )

    assert _row_count(spark, table_name) == 1
    assert not spark.catalog.tableExists(quarantine_table)


def test_materialize_quality_checks_can_be_disabled_at_runtime(
    spark: SparkSession,
    materializations_project_dir: Path,
    initialized_materializations_context: None,
) -> None:
    """Runtime flag should skip all configured quality checks."""
    table_name = "mat_append_quality_orders"
    quarantine_table = f"{table_name}_quarantine"

    _drop_table_if_exists(spark, table_name)
    _drop_table_if_exists(spark, quarantine_table)

    target_df = _csv_df(spark, materializations_project_dir / "data/append/target.csv")
    target_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    source_df = _csv_df(spark, materializations_project_dir / "data/append/source.csv")
    materialize(
        spark=spark,
        dataframe=source_df,
        name=table_name,
        config=None,
        apply_quality_checks=False,
        apply_vacuum=False,
        apply_optimize=False,
    )

    assert _row_count(spark, table_name) == 3
    assert not spark.catalog.tableExists(quarantine_table)
