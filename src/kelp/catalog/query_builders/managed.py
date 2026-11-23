"""Query builder for managed (Delta) tables — full capability set."""

from kelp.catalog.query_builders.base import BaseTableQueryBuilder, Capability


class ManagedTableQueryBuilder(BaseTableQueryBuilder):
    """Query builder for managed Delta tables.

    Supports all table capabilities, including direct grants.
    All SQL uses the default ``TABLE`` syntax from the base class.
    """

    capabilities: frozenset[Capability] = frozenset(Capability)
