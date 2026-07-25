"""Query builder for materialized views."""

from kelp.catalog.query_builders.base import BaseTableQueryBuilder, Capability


class MaterializedViewQueryBuilder(BaseTableQueryBuilder):
    """Query builder for materialized views.

    Supports description, tags, column descriptions, and column tags.
    Table description uses ``COMMENT ON TABLE`` (Databricks requires TABLE
    rather than MATERIALIZED VIEW in the COMMENT ON syntax).
    Properties, clustering, and constraints are not supported.
    """

    capabilities: frozenset[Capability] = frozenset(
        {
            Capability.TABLE_DESCRIPTION,
            Capability.TABLE_TAGS,
            Capability.COLUMN_DESCRIPTION,
            Capability.COLUMN_TAGS,
        }
    )
    securable = "MATERIALIZED VIEW"
    comment_on_type = "TABLE"
