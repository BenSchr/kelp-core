"""Remote-metadata fetchers for Unity Catalog entities, keyed by engine.

Public API::

    from kelp.catalog.remote_fetchers import RemoteFetcherFactory

    factory = RemoteFetcherFactory()
    table = factory.get_table_fetcher("sdk").fetch("catalog.schema.table", profile="analytics")

``"spark"`` (default) requires ``pyspark`` and an active Spark session —
the normal situation when sync runs inside a Databricks job/notebook.
``"sdk"`` only needs the ``databricks-sdk`` dependency ``kelp-core``
already requires and works from anywhere; the CLI passes it explicitly.
Both engines' modules are imported lazily by the factory, so importing
this package never forces a ``pyspark`` dependency on callers.
"""

from kelp.catalog.remote_fetchers.factory import DEFAULT_ENGINE, RemoteFetcherFactory

__all__ = [
    "DEFAULT_ENGINE",
    "RemoteFetcherFactory",
]
