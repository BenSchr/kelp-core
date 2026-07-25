"""Query builder for views."""

import logging

from kelp.catalog.query_builders._sql import SET_TAG_ON, UNSET_TAG_ON, esc
from kelp.catalog.query_builders.base import BaseTableQueryBuilder, Capability
from kelp.catalog.uc_models import DictDiff

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
    securable = "VIEW"
    comment_on_type = "VIEW"

    def column_tag_queries(self, fqn: str, col_name: str, tag_diff: DictDiff) -> list[str]:
        """Views require ``SET TAG ON`` / ``UNSET TAG ON`` for column tag mutations.

        Order: deletes → creates → updates (skipping keys already in creates).
        """
        queries: list[str] = []
        col_path = f"{fqn}.{col_name}"

        queries.extend(
            UNSET_TAG_ON.format(type="COLUMN", path=col_path, key=key) for key in tag_diff.deletes
        )

        for key, value in tag_diff.creates.items():
            queries.append(
                SET_TAG_ON.format(type="COLUMN", path=col_path, key=key, value=esc(value))
            )

        for key, value in tag_diff.updates.items():
            if key in tag_diff.creates:
                continue  # already emitted above
            queries.append(UNSET_TAG_ON.format(type="COLUMN", path=col_path, key=key))
            queries.append(
                SET_TAG_ON.format(type="COLUMN", path=col_path, key=key, value=esc(value))
            )

        for query in queries:
            logger.debug("Generated: %s", query)
        return queries
