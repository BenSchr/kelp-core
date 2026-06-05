"""Global pytest fixtures and configuration."""

import os
from collections.abc import Generator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from kelp.meta.context import MetaContextStore


@pytest.fixture(autouse=True)
def reset_context_and_env():
    """Reset the global context before each test.

    Ensures test isolation by clearing the context store.
    """
    # Clear context before test
    MetaContextStore.clear_all()
    os.environ.pop("KELP_PROJECT_FILE", None)
    os.environ.pop("KELP_TARGET", None)
    os.environ.pop("KELP_MANIFEST_FILE", None)

    yield

    # Clear context after test
    MetaContextStore.clear_all()
    os.environ.pop("KELP_PROJECT_FILE", None)
    os.environ.pop("KELP_TARGET", None)
    os.environ.pop("KELP_MANIFEST_FILE", None)


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def simple_project_dir(fixtures_dir: Path) -> Path:
    """Return the path to a simple test project."""
    return fixtures_dir / "simple_project"


@pytest.fixture
def multi_target_project_dir(fixtures_dir: Path) -> Path:
    """Return the path to a multi-target test project."""
    return fixtures_dir / "multi_target_project"


@pytest.fixture
def minimal_project_dir(fixtures_dir: Path) -> Path:
    """Return the path to a minimal test project."""
    return fixtures_dir / "minimal_project"


@pytest.fixture
def functions_abacs_project_dir(fixtures_dir: Path) -> Path:
    """Return the path to a test project with functions and ABAC policies."""
    return fixtures_dir / "functions_abacs_project"


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Create a single-core local SparkSession with Delta support for table tests."""

    import shutil

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse_dir = Path("/tmp/kelp-tests-warehouse")  # noqa: S108
    if warehouse_dir.exists():
        shutil.rmtree(warehouse_dir)
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    # Kill DQX internal SparkSession if it exists to avoid conflicts with our test SparkSession
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()

    builder = (
        SparkSession.builder.master("local[4]")
        .appName("kelp-spark-test")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "1")
        # .config("spark.default.parallelism", "1")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.ui.enabled", "false")
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
