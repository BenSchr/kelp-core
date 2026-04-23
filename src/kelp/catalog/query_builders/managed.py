"""Query builder for managed (Delta) tables — full capability set."""

from __future__ import annotations

import logging

from kelp.catalog.query_builders._sql import (
    ADD_FK,
    ADD_PK,
    ALTER_COLUMN,
    BASE_ALTER,
    CLUSTER_BY_AUTO,
    CLUSTER_BY_COLS,
    CLUSTER_BY_NONE,
    COMMENT_ON,
    DROP_CONSTRAINT,
    SET_TAGS,
    SET_TBLPROPERTIES,
    UNSET_TAGS,
    UNSET_TBLPROPERTIES,
    esc,
    key_list,
    kv_tags,
)
from kelp.catalog.query_builders.base import BaseTableQueryBuilder, Capability
from kelp.catalog.uc_models import (
    Constraint,
    ConstraintFKDiff,
    ConstraintPKDiff,
    DictDiff,
    TableDiff,
)
from kelp.models.model import ForeignKeyConstraint

logger = logging.getLogger(__name__)


class ManagedTableQueryBuilder(BaseTableQueryBuilder):
    """Query builder for managed Delta tables.

    Supports all capabilities: description, tags, properties, column
    descriptions, column tags, clustering, and constraints.
    """

    capabilities: frozenset[Capability] = frozenset(
        {
            Capability.TABLE_DESCRIPTION,
            Capability.TABLE_TAGS,
            Capability.TABLE_PROPERTIES,
            Capability.COLUMN_DESCRIPTION,
            Capability.COLUMN_TAGS,
            Capability.CLUSTER_BY,
            Capability.CONSTRAINTS,
        }
    )

    def description_queries(self, fqn: str, diff: TableDiff) -> list[str]:
        """Generate ``COMMENT ON TABLE`` statement."""
        if diff.table_description is None:
            return []
        query = COMMENT_ON.format(type="TABLE", path=fqn, comment=esc(diff.table_description))
        logger.debug("Generated: %s", query)
        return [query]

    def table_tag_queries(self, fqn: str, tag_diff: DictDiff) -> list[str]:
        """Generate ``ALTER TABLE … SET/UNSET TAGS`` statements."""
        if not tag_diff.has_changes:
            return []
        queries: list[str] = []
        if tag_diff.updates:
            query = BASE_ALTER.format(
                table_type="TABLE",
                fqn=fqn,
                action=SET_TAGS.format(tags=kv_tags(tag_diff.updates)),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)
        if tag_diff.deletes:
            query = BASE_ALTER.format(
                table_type="TABLE",
                fqn=fqn,
                action=UNSET_TAGS.format(tags=key_list(tag_diff.deletes)),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)
        return queries

    def table_property_queries(self, fqn: str, prop_diff: DictDiff) -> list[str]:
        """Generate ``ALTER TABLE … SET/UNSET TBLPROPERTIES`` statements."""
        if not prop_diff.has_changes:
            return []
        queries: list[str] = []
        if prop_diff.updates:
            props_str = ", ".join(f"'{esc(k)}'='{esc(v)}'" for k, v in prop_diff.updates.items())
            query = BASE_ALTER.format(
                table_type="TABLE",
                fqn=fqn,
                action=SET_TBLPROPERTIES.format(props=props_str),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)
        if prop_diff.deletes:
            query = BASE_ALTER.format(
                table_type="TABLE",
                fqn=fqn,
                action=UNSET_TBLPROPERTIES.format(props=key_list(prop_diff.deletes)),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)
        return queries

    def column_queries(self, fqn: str, diff: TableDiff) -> list[str]:
        """Generate per-column ``COMMENT ON COLUMN`` and ``SET/UNSET TAGS`` statements."""
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
        queries: list[str] = []
        if tag_diff.updates:
            query = BASE_ALTER.format(
                table_type="TABLE",
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
                table_type="TABLE",
                fqn=fqn,
                action=ALTER_COLUMN.format(
                    col=col_name,
                    action=UNSET_TAGS.format(tags=key_list(tag_diff.deletes)),
                ),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)
        return queries

    def cluster_by_queries(self, fqn: str, diff: TableDiff) -> list[str]:
        """Generate ``ALTER TABLE … CLUSTER BY`` statement."""
        if not diff.cluster_by_changed:
            return []
        if diff.cluster_by_auto:
            query = CLUSTER_BY_AUTO.format(fqn=fqn)
        elif diff.cluster_by_cols:
            query = CLUSTER_BY_COLS.format(fqn=fqn, cols=", ".join(diff.cluster_by_cols))
        else:
            query = CLUSTER_BY_NONE.format(fqn=fqn)
        logger.debug("Generated: %s", query)
        return [query]

    def constraint_queries(
        self,
        fqn: str,
        pk_diff: ConstraintPKDiff,
        fk_diff: ConstraintFKDiff,
    ) -> list[str]:
        """Generate ``ADD/DROP CONSTRAINT`` statements for PK and FK constraints."""
        queries: list[str] = []
        if pk_diff.delete is not None:
            queries.extend(self._drop_constraint(fqn, pk_diff.delete.name))
        if pk_diff.update is not None:
            queries.extend(self._drop_constraint(fqn, pk_diff.update.name))
            queries.extend(self._add_primary_key(fqn, pk_diff.update))
        if pk_diff.create is not None:
            queries.extend(self._add_primary_key(fqn, pk_diff.create))
        for fk in fk_diff.delete:
            queries.extend(self._drop_constraint(fqn, fk.name))
        for fk in fk_diff.update:
            queries.extend(self._drop_constraint(fqn, fk.name))
            if isinstance(fk, ForeignKeyConstraint):
                queries.extend(self._add_foreign_key(fqn, fk))
        for fk in fk_diff.create:
            if isinstance(fk, ForeignKeyConstraint):
                queries.extend(self._add_foreign_key(fqn, fk))
        return queries

    def _drop_constraint(self, fqn: str, name: str) -> list[str]:
        query = DROP_CONSTRAINT.format(fqn=fqn, name=name)
        logger.debug("Generated: %s", query)
        return [query]

    def _add_primary_key(self, fqn: str, constraint: Constraint) -> list[str]:
        query = ADD_PK.format(fqn=fqn, name=constraint.name, cols=", ".join(constraint.columns))
        logger.debug("Generated: %s", query)
        return [query]

    def _add_foreign_key(self, fqn: str, constraint: ForeignKeyConstraint) -> list[str]:
        query = ADD_FK.format(
            fqn=fqn,
            name=constraint.name,
            cols=", ".join(constraint.columns),
            ref_table=constraint.reference_table,
            ref_cols=", ".join(constraint.reference_columns),
        )
        logger.debug("Generated: %s", query)
        return [query]
