"""Factory for Unity Catalog table query builders.

Usage (drop-in replacement for ``UCQueryBuilder``)::

    from kelp.catalog.query_builders.factory import UCQueryBuilderFactory

    factory = UCQueryBuilderFactory()
    queries = factory.build("catalog.schema.my_table", diff, "managed")

The factory also exposes a static :attr:`CAPABILITY_TABLE` for introspection::

    from kelp.catalog.query_builders.factory import UCQueryBuilderFactory, CAPABILITY_TABLE

    for table_type, caps in CAPABILITY_TABLE.items():
        print(table_type, caps)
"""

from __future__ import annotations

import logging

from kelp.catalog.query_builders.base import BaseTableQueryBuilder, Capability
from kelp.catalog.query_builders.managed import ManagedTableQueryBuilder
from kelp.catalog.query_builders.materialized_view import MaterializedViewQueryBuilder
from kelp.catalog.query_builders.streaming_table import StreamingTableQueryBuilder
from kelp.catalog.query_builders.view import ViewQueryBuilder
from kelp.catalog.uc_models import TableDiff

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Capability table — maps each known table type to its supported capabilities.
# This is intentionally a module-level constant so it can be imported and
# rendered (e.g. as documentation) without instantiating anything.
# ---------------------------------------------------------------------------

CAPABILITY_TABLE: dict[str, frozenset[Capability]] = {
    "managed": ManagedTableQueryBuilder.capabilities,
    "view": ViewQueryBuilder.capabilities,
    "materialized_view": MaterializedViewQueryBuilder.capabilities,
    "streaming_table": StreamingTableQueryBuilder.capabilities,
}

_REGISTRY: dict[str, type[BaseTableQueryBuilder]] = {
    "managed": ManagedTableQueryBuilder,
    "view": ViewQueryBuilder,
    "materialized_view": MaterializedViewQueryBuilder,
    "streaming_table": StreamingTableQueryBuilder,
}


class UCQueryBuilderFactory:
    """Factory that resolves the correct :class:`BaseTableQueryBuilder` for a
    given table type and provides the same ``build`` entry-point as the
    original :class:`~kelp.catalog.uc_query_builder.UCQueryBuilder`.

    The factory is stateless and safe to share across threads.

    Example::

        factory = UCQueryBuilderFactory()

        # Equivalent to the old UCQueryBuilder().build(fqn, diff, table_type)
        queries = factory.build("catalog.schema.table", diff, "managed")

        # Retrieve a typed builder for direct use
        builder = factory.get_builder("view")
        queries = builder.build("catalog.schema.my_view", diff)

    """

    def get_builder(self, table_type: str) -> BaseTableQueryBuilder:
        """Return the builder instance for *table_type*.

        Args:
            table_type: Logical table-type key (``"managed"``, ``"view"``,
                ``"materialized_view"``, or ``"streaming_table"``).

        Returns:
            A concrete :class:`BaseTableQueryBuilder` subclass instance.

        Raises:
            KeyError: If *table_type* is not registered.

        """
        builder_cls = _REGISTRY.get(table_type)
        if builder_cls is None:
            raise KeyError(
                f"No query builder registered for table type '{table_type}'. "
                f"Known types: {sorted(_REGISTRY)}"
            )
        return builder_cls()

    def build(self, fqn: str, diff: TableDiff, table_type: str) -> list[str]:
        """Build all SQL queries for the given diff.

        This is a drop-in replacement for
        ``UCQueryBuilder().build(fqn, diff, table_type)``.

        Args:
            fqn: Fully-qualified table name (``catalog.schema.table``).
            diff: Diff produced by ``TableDiffCalculator``.
            table_type: Logical table-type key.

        Returns:
            Ordered list of SQL strings to execute.

        Raises:
            KeyError: If *table_type* is not registered.

        """
        builder = self.get_builder(table_type)
        return builder.build(fqn, diff)
