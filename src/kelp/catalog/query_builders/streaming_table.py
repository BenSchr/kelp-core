"""Query builder for streaming tables."""

from kelp.catalog.query_builders._sql import ALTER_COLUMN, SET_COMMENT, esc
from kelp.catalog.query_builders.base import BaseTableQueryBuilder, Capability


class StreamingTableQueryBuilder(BaseTableQueryBuilder):
    """Query builder for streaming tables.

    Supports description, tags, and column-level changes.  Table
    descriptions use ``COMMENT ON TABLE`` (same as managed tables); column
    descriptions use the ``ALTER STREAMING TABLE … ALTER COLUMN … COMMENT``
    syntax.  Properties, clustering, and constraints are not supported.
    """

    capabilities: frozenset[Capability] = frozenset(
        {
            Capability.TABLE_DESCRIPTION,
            Capability.TABLE_TAGS,
            Capability.COLUMN_DESCRIPTION,
            Capability.COLUMN_TAGS,
        }
    )
    securable = "STREAMING TABLE"
    comment_on_type = "TABLE"

    def column_description_queries(self, fqn: str, col_name: str, description: str) -> list[str]:
        """Streaming tables require ``ALTER STREAMING TABLE … ALTER COLUMN … COMMENT``."""
        action = ALTER_COLUMN.format(
            col=col_name, action=SET_COMMENT.format(comment=esc(description))
        )
        return [self._alter(fqn, action)]
