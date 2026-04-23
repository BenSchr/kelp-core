"""Query builder for streaming tables."""

from __future__ import annotations

import logging

from kelp.catalog.query_builders._sql import (
    ALTER_COLUMN,
    BASE_ALTER,
    SET_COMMENT,
    SET_TAGS,
    UNSET_TAGS,
    esc,
    key_list,
    kv_tags,
)
from kelp.catalog.query_builders.base import BaseTableQueryBuilder, Capability
from kelp.catalog.uc_models import DictDiff, TableDiff

logger = logging.getLogger(__name__)


class StreamingTableQueryBuilder(BaseTableQueryBuilder):
    """Query builder for streaming tables.

    Supports tags and column-level changes only.  Table-level description
    is **not** supported (Databricks does not allow ``COMMENT ON`` for
    streaming tables).  Column descriptions use the
    ``ALTER STREAMING TABLE … ALTER COLUMN … COMMENT`` syntax.
    Properties, clustering, and constraints are not supported.
    """

    capabilities: frozenset[Capability] = frozenset(
        {
            Capability.TABLE_TAGS,
            Capability.COLUMN_DESCRIPTION,
            Capability.COLUMN_TAGS,
        }
    )

    def table_tag_queries(self, fqn: str, tag_diff: DictDiff) -> list[str]:
        """Generate ``ALTER STREAMING TABLE … SET/UNSET TAGS`` statements."""
        if not tag_diff.has_changes:
            return []
        queries: list[str] = []
        if tag_diff.updates:
            query = BASE_ALTER.format(
                table_type="STREAMING TABLE",
                fqn=fqn,
                action=SET_TAGS.format(tags=kv_tags(tag_diff.updates)),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)
        if tag_diff.deletes:
            query = BASE_ALTER.format(
                table_type="STREAMING TABLE",
                fqn=fqn,
                action=UNSET_TAGS.format(tags=key_list(tag_diff.deletes)),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)
        return queries

    def column_queries(self, fqn: str, diff: TableDiff) -> list[str]:
        """Generate per-column description and tag statements for streaming tables."""
        queries: list[str] = []
        for col_name, col_diff in diff.columns.items():
            if col_diff.description is not None:
                queries.extend(
                    self._column_description_queries(fqn, col_name, col_diff.description)
                )
            if col_diff.tags is not None and col_diff.tags.has_changes:
                queries.extend(self._column_tag_queries(fqn, col_name, col_diff.tags))
        return queries

    def _column_description_queries(self, fqn: str, col_name: str, description: str) -> list[str]:
        """Streaming tables require ``ALTER STREAMING TABLE … ALTER COLUMN … COMMENT``."""
        query = BASE_ALTER.format(
            table_type="STREAMING TABLE",
            fqn=fqn,
            action=ALTER_COLUMN.format(
                col=col_name,
                action=SET_COMMENT.format(comment=esc(description)),
            ),
        )
        logger.debug("Generated: %s", query)
        return [query]

    def _column_tag_queries(self, fqn: str, col_name: str, tag_diff: DictDiff) -> list[str]:
        queries: list[str] = []
        if tag_diff.updates:
            query = BASE_ALTER.format(
                table_type="STREAMING TABLE",
                fqn=fqn,
                action=ALTER_COLUMN.format(
                    col=col_name,
                    action=SET_TAGS.format(tags=kv_tags(tag_diff.updates)),
                ),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)
        if tag_diff.deletes:
            query = BASE_ALTER.format(
                table_type="STREAMING TABLE",
                fqn=fqn,
                action=ALTER_COLUMN.format(
                    col=col_name,
                    action=UNSET_TAGS.format(tags=key_list(tag_diff.deletes)),
                ),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)
        return queries
