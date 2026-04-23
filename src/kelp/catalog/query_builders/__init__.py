"""Unity Catalog table query builders — Interface + Factory.

Public API::

    from kelp.catalog.query_builders import UCQueryBuilderFactory, Capability, CAPABILITY_TABLE

    # Drop-in replacement for UCQueryBuilder
    factory = UCQueryBuilderFactory()
    queries = factory.build("catalog.schema.table", diff, "managed")

    # Introspect capabilities per table type
    for table_type, caps in CAPABILITY_TABLE.items():
        print(table_type, sorted(c.value for c in caps))

"""

from __future__ import annotations

from kelp.catalog.query_builders.base import BaseTableQueryBuilder, Capability
from kelp.catalog.query_builders.factory import CAPABILITY_TABLE, UCQueryBuilderFactory
from kelp.catalog.query_builders.managed import ManagedTableQueryBuilder
from kelp.catalog.query_builders.materialized_view import MaterializedViewQueryBuilder
from kelp.catalog.query_builders.streaming_table import StreamingTableQueryBuilder
from kelp.catalog.query_builders.view import ViewQueryBuilder

__all__ = [
    "CAPABILITY_TABLE",
    "BaseTableQueryBuilder",
    "Capability",
    "ManagedTableQueryBuilder",
    "MaterializedViewQueryBuilder",
    "StreamingTableQueryBuilder",
    "UCQueryBuilderFactory",
    "ViewQueryBuilder",
]
