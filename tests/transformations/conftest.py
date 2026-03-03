"""Shared fixtures for transformation tests — includes a real local SparkSession."""

from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Create a minimal local SparkSession for transformation tests.

    Session-scoped to avoid repeated JVM startup overhead. All extra services
    (UI, adaptive query execution broadcast thresholds) are disabled or
    minimised for speed and determinism.
    """
    session = (
        SparkSession.builder.master("local[1]")
        .appName("kelp-transformations-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )
    yield session
    session.stop()
