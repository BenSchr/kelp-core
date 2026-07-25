"""Factory for Unity Catalog query builders.

Covers every syncable catalog entity — tables (by table type), metric
views, functions, and ABAC policies — behind one lookup keyed by a logical
"kind" string.

Usage::

    from kelp.catalog.query_builders.factory import UCQueryBuilderFactory

    factory = UCQueryBuilderFactory()
    queries = factory.build("catalog.schema.my_table", diff, "managed")

    # Entities that need constructor args (e.g. MetricViewQueryBuilder needs
    # a RemoteCatalogConfig) pass them as kwargs to get_builder:
    builder = factory.get_builder("metric_view", config=remote_catalog_config)
    queries = builder.build(metric_view, remote)

The factory also exposes a static :attr:`CAPABILITY_TABLE` for introspecting
the four *table* builders (metric views, functions, and ABAC policies don't
have per-type ``Capability`` variance, so they're not part of this table)::

    from kelp.catalog.query_builders.factory import UCQueryBuilderFactory, CAPABILITY_TABLE

    for table_type, caps in CAPABILITY_TABLE.items():
        print(table_type, caps)
"""

import logging
from typing import Any

from kelp.catalog.query_builders.abac import AbacPolicyQueryBuilder
from kelp.catalog.query_builders.base import Capability
from kelp.catalog.query_builders.function import FunctionQueryBuilder
from kelp.catalog.query_builders.managed import ManagedTableQueryBuilder
from kelp.catalog.query_builders.materialized_view import MaterializedViewQueryBuilder
from kelp.catalog.query_builders.metric_view import MetricViewQueryBuilder
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

_REGISTRY: dict[str, type[Any]] = {
    "managed": ManagedTableQueryBuilder,
    "view": ViewQueryBuilder,
    "materialized_view": MaterializedViewQueryBuilder,
    "streaming_table": StreamingTableQueryBuilder,
    "metric_view": MetricViewQueryBuilder,
    "function": FunctionQueryBuilder,
    "abac": AbacPolicyQueryBuilder,
}


class UCQueryBuilderFactory:
    """Factory that resolves the correct query builder for a given entity kind.

    The factory is stateless and safe to share across threads.

    Example::

        factory = UCQueryBuilderFactory()
        queries = factory.build("catalog.schema.table", diff, "managed")

        # Retrieve a typed builder for direct use
        builder = factory.get_builder("view")
        queries = builder.build("catalog.schema.my_view", diff)

    """

    def get_builder(self, kind: str, **kwargs: Any) -> Any:
        """Return a builder instance for *kind*.

        Args:
            kind: Logical entity key — one of the four table types
                (``"managed"``, ``"view"``, ``"materialized_view"``,
                ``"streaming_table"``), or ``"metric_view"``, ``"function"``,
                ``"abac"``.
            **kwargs: Forwarded to the builder's constructor. Table builders,
                ``FunctionQueryBuilder``, and ``AbacPolicyQueryBuilder`` take
                none; ``MetricViewQueryBuilder`` requires ``config``.

        Returns:
            A builder instance exposing its own ``build(...)`` method — the
            table builders share :meth:`BaseTableQueryBuilder.build`, while
            metric view/function/abac builders take entity-shaped arguments
            instead of a ``TableDiff`` since they aren't diffed the same way.

        Raises:
            KeyError: If *kind* is not registered.

        """
        builder_cls = _REGISTRY.get(kind)
        if builder_cls is None:
            raise KeyError(
                f"No query builder registered for '{kind}'. Known kinds: {sorted(_REGISTRY)}"
            )
        return builder_cls(**kwargs)

    def build(self, fqn: str, diff: TableDiff, table_type: str) -> list[str]:
        """Build all SQL queries for a table diff.

        Convenience wrapper over :meth:`get_builder` for the table-builder
        case, where every builder shares the same ``build(fqn, diff)`` shape.

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
