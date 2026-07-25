"""Factory for remote-metadata fetchers, keyed by engine.

Mirrors :mod:`kelp.catalog.query_builders.factory`: one lookup for every way
Kelp knows how to read back the current state of a catalog entity, so
callers don't need to know which concrete engine is behind ``"spark"`` vs
``"sdk"``.

Usage::

    from kelp.catalog.remote_fetchers import RemoteFetcherFactory

    factory = RemoteFetcherFactory()
    table = factory.get_table_fetcher("sdk").fetch("catalog.schema.table", profile="analytics")
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_ENGINE = "sdk"
_KNOWN_ENGINES = ("spark", "sdk")


class RemoteFetcherFactory:
    """Resolve the table/metric-view fetcher for a given engine.

    ``"spark"`` requires an active Spark session (e.g. running
    inside a Databricks job/notebook) and uses
    ``DESCRIBE EXTENDED … AS JSON`` plus a single
    ``information_schema.column_tags`` query per entity instead of one API
    call per column — see :mod:`kelp.catalog.remote_fetchers.spark` for
    caveats. ``"sdk"`` (default) calls the Databricks SDK's REST APIs and works from
    anywhere with a configured profile/token — it is what the CLI passes
    explicitly.

    The ``spark`` engine's module is only imported lazily, on first request,
    since ``pyspark`` is not a core ``kelp-core`` dependency — requesting
    the ``"sdk"`` engine never requires it to be installed.
    """

    def get_table_fetcher(self, engine: str = DEFAULT_ENGINE) -> Any:
        """Return a table fetcher for *engine*.

        Raises:
            KeyError: If *engine* is not one of ``"spark"``/``"sdk"``.

        """
        if engine == "spark":
            from kelp.catalog.remote_fetchers.spark import SparkTableFetcher

            return SparkTableFetcher()
        if engine == "sdk":
            from kelp.catalog.remote_fetchers.sdk import SdkTableFetcher

            return SdkTableFetcher()
        raise KeyError(
            f"No table fetcher registered for engine '{engine}'. Known: {list(_KNOWN_ENGINES)}"
        )

    def get_metric_view_fetcher(self, engine: str = DEFAULT_ENGINE) -> Any:
        """Return a metric view fetcher for *engine*.

        Raises:
            KeyError: If *engine* is not one of ``"spark"``/``"sdk"``.

        """
        if engine == "spark":
            from kelp.catalog.remote_fetchers.spark import SparkMetricViewFetcher

            return SparkMetricViewFetcher()
        if engine == "sdk":
            from kelp.catalog.remote_fetchers.sdk import SdkMetricViewFetcher

            return SdkMetricViewFetcher()
        raise KeyError(
            f"No metric view fetcher registered for engine '{engine}'. Known: {list(_KNOWN_ENGINES)}"
        )
