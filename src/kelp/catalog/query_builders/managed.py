"""Query builder for managed (Delta) tables — full capability set."""

from kelp.catalog.query_builders.base import BaseTableQueryBuilder, Capability


class ManagedTableQueryBuilder(BaseTableQueryBuilder):
    """Query builder for managed Delta tables.

    Supports all capabilities: description, tags, properties, column
    descriptions, column tags, clustering, constraints, and auto TTL.
    All SQL uses the default ``TABLE`` syntax from the base class.
    """

    capabilities: frozenset[Capability] = frozenset(Capability)
