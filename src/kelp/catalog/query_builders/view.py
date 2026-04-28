"""Query builder for views."""

from __future__ import annotations

import logging

from kelp.catalog.query_builders._sql import (
    BASE_ALTER,
    COMMENT_ON,
    SET_TAG_ON,
    SET_TAGS,
    UNSET_TAG_ON,
    UNSET_TAGS,
    esc,
    key_list,
    kv_tags,
)
from kelp.catalog.query_builders.base import BaseTableQueryBuilder, Capability
from kelp.catalog.uc_models import DictDiff, TableDiff

logger = logging.getLogger(__name__)


class ViewQueryBuilder(BaseTableQueryBuilder):
    """Query builder for views.

    Supports description, tags, column descriptions, and column tags.
    Column tag mutations use the ``SET TAG ON`` / ``UNSET TAG ON`` syntax
    required for views instead of ``ALTER VIEW … ALTER COLUMN``.
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

    def description_queries(self, fqn: str, diff: TableDiff) -> list[str]:
        """Generate ``COMMENT ON VIEW`` statement."""
        if diff.table_description is None:
            return []
        query = COMMENT_ON.format(type="VIEW", path=fqn, comment=esc(diff.table_description))
        logger.debug("Generated: %s", query)
        return [query]

    def table_tag_queries(self, fqn: str, tag_diff: DictDiff) -> list[str]:
        """Generate ``ALTER VIEW … SET/UNSET TAGS`` statements."""
        if not tag_diff.has_changes:
            return []
        queries: list[str] = []
        if tag_diff.updates:
            query = BASE_ALTER.format(
                table_type="VIEW",
                fqn=fqn,
                action=SET_TAGS.format(tags=kv_tags(tag_diff.updates)),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)
        if tag_diff.deletes:
            query = BASE_ALTER.format(
                table_type="VIEW",
                fqn=fqn,
                action=UNSET_TAGS.format(tags=key_list(tag_diff.deletes)),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)
        return queries

    def column_queries(self, fqn: str, diff: TableDiff) -> list[str]:
        """Generate per-column ``COMMENT ON COLUMN`` and ``SET/UNSET TAG ON`` statements."""
        queries: list[str] = []
        for col_name, col_diff in diff.columns.items():
            if col_diff.description is not None:
                query = COMMENT_ON.format(
                    type="COLUMN",
                    path=f"{fqn}.{col_name}",
                    comment=esc(col_diff.description),
                )
                logger.debug("Generated: %s", query)
                queries.append(query)
            if col_diff.tags is not None and col_diff.tags.has_changes:
                queries.extend(self._column_tag_queries(fqn, col_name, col_diff.tags))
        return queries

    def _column_tag_queries(self, fqn: str, col_name: str, tag_diff: DictDiff) -> list[str]:
        """Views require ``SET TAG ON`` / ``UNSET TAG ON`` for column tag mutations.

        Order: deletes → creates → updates (skipping keys already in creates).
        """
        queries: list[str] = []
        col_path = f"{fqn}.{col_name}"

        for key in tag_diff.deletes:
            query = UNSET_TAG_ON.format(type="COLUMN", path=col_path, key=key)
            logger.debug("Generated: %s", query)
            queries.append(query)

        for key, value in tag_diff.creates.items():
            query = SET_TAG_ON.format(type="COLUMN", path=col_path, key=key, value=esc(value))
            logger.debug("Generated: %s", query)
            queries.append(query)

        for key, value in tag_diff.updates.items():
            if key in tag_diff.creates:
                continue  # already emitted above
            unset = UNSET_TAG_ON.format(type="COLUMN", path=col_path, key=key)
            logger.debug("Generated (pre-unset): %s", unset)
            queries.append(unset)
            query = SET_TAG_ON.format(type="COLUMN", path=col_path, key=key, value=esc(value))
            logger.debug("Generated: %s", query)
            queries.append(query)

        return queries
