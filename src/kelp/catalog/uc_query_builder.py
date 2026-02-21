"""Pure SQL generation for Unity Catalog table synchronisation (v2)."""

from __future__ import annotations

import logging

from kelp.catalog.uc_models import (
    Constraint,
    ConstraintFKDiff,
    ConstraintPKDiff,
    DictDiff,
    TableDiff,
)

logger = logging.getLogger(__name__)

_BASE_ALTER = "ALTER {table_type} {fqn} {action}"
_ALTER_COLUMN = "ALTER COLUMN {col} {action}"

_COMMENT_ON = "COMMENT ON {type} {path} IS '{comment}'"
_SET_COMMENT = "COMMENT '{comment}'"

_SET_TAGS = "SET TAGS ({tags})"
_UNSET_TAGS = "UNSET TAGS ({tags})"
_SET_TAG_ON = "SET TAG ON {type} {path} `{key}`=`{value}`"
_UNSET_TAG_ON = "UNSET TAG ON {type} {path} `{key}`"

_SET_TBLPROPERTIES = "SET TBLPROPERTIES ({props})"
_UNSET_TBLPROPERTIES = "UNSET TBLPROPERTIES ({props})"

_CLUSTER_BY_AUTO = "ALTER TABLE {fqn} CLUSTER BY AUTO"
_CLUSTER_BY_NONE = "ALTER TABLE {fqn} CLUSTER BY NONE"
_CLUSTER_BY_COLS = "ALTER TABLE {fqn} CLUSTER BY ({cols})"

_ADD_PK = "ALTER TABLE {fqn} ADD CONSTRAINT {name} PRIMARY KEY ({cols})"
_ADD_FK = (
    "ALTER TABLE {fqn} ADD CONSTRAINT {name} "
    "FOREIGN KEY ({cols}) REFERENCES {ref_table} ({ref_cols})"
)
_DROP_CONSTRAINT = "ALTER TABLE {fqn} DROP CONSTRAINT {name}"

_NO_TABLE_COMMENT_TYPES: frozenset[str] = frozenset({"streaming_table"})

_NO_PROPERTIES_TYPES: frozenset[str] = frozenset(
    {"view", "materialized_view", "streaming_table"})
_NO_CLUSTER_TYPES: frozenset[str] = frozenset(
    {"view", "materialized_view", "streaming_table"})
_NO_CONSTRAINT_TYPES: frozenset[str] = frozenset(
    {"view", "materialized_view", "streaming_table"})

_UC_TYPE: dict[str, str] = {
    "managed": "TABLE",
    "view": "VIEW",
    "materialized_view": "MATERIALIZED VIEW",
    "streaming_table": "STREAMING TABLE",
}


def _esc(value: str) -> str:
    """Escape single-quotes inside a SQL string literal."""
    return value.replace("'", "''")


def _kv_tags(tags: dict[str, str]) -> str:
    """Format a dict as ``'k1'='v1', 'k2'='v2'`` for SET TAGS."""
    return ", ".join(f"'{_esc(k)}'='{_esc(v)}'" for k, v in tags.items())


def _key_list(keys: list[str]) -> str:
    """Format a list of keys as ``'k1', 'k2'`` for UNSET TAGS/TBLPROPERTIES."""
    return ", ".join(f"'{_esc(k)}'" for k in keys)


class UCQueryBuilder:
    """Translate a TableDiff into SQL statements.

    The builder is stateless and safe to share across threads.
    """

    def build(self, fqn: str, diff: TableDiff, table_type: str) -> list[str]:
        """Return the ordered list of SQL queries for the given diff.

        Args:
            fqn: Fully-qualified table name (catalog.schema.table).
            diff: Diff produced by TableDiffCalculator.
            table_type: Logical table type key (e.g. "managed").

        Returns:
            Ordered list of SQL strings to execute.
        """
        queries: list[str] = []

        queries.extend(self._description_queries(fqn, diff, table_type))
        queries.extend(self._table_tag_queries(
            fqn, diff.table_tags, table_type))
        queries.extend(self._table_property_queries(
            fqn, diff.table_properties, table_type))
        queries.extend(self._column_queries(fqn, diff, table_type))
        queries.extend(self._cluster_by_queries(fqn, diff, table_type))
        queries.extend(self._constraint_queries(
            fqn, diff.constraint_pk, diff.constraint_fk, table_type))

        return queries

    def _description_queries(self, fqn: str, diff: TableDiff, table_type: str) -> list[str]:
        if diff.table_description is None:
            return []

        if table_type in _NO_TABLE_COMMENT_TYPES:
            logger.warning(
                "COMMENT ON is not supported for %s; skipping %s", table_type, fqn
            )
            return []

        if table_type == "materialized_view":
            table_type = "managed"

        sql_type = _UC_TYPE.get(table_type, "TABLE")
        query = _COMMENT_ON.format(
            type=sql_type,
            path=fqn,
            comment=_esc(diff.table_description),
        )
        logger.debug("Generated: %s", query)
        return [query]

    def _table_tag_queries(self, fqn: str, tag_diff: DictDiff, table_type: str) -> list[str]:
        if not tag_diff.has_changes:
            return []

        sql_type = _UC_TYPE.get(table_type, "TABLE")
        queries: list[str] = []

        if tag_diff.updates:
            query = _BASE_ALTER.format(
                table_type=sql_type,
                fqn=fqn,
                action=_SET_TAGS.format(tags=_kv_tags(tag_diff.updates)),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)

        if tag_diff.deletes:
            query = _BASE_ALTER.format(
                table_type=sql_type,
                fqn=fqn,
                action=_UNSET_TAGS.format(tags=_key_list(tag_diff.deletes)),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)

        return queries

    def _table_property_queries(
        self, fqn: str, prop_diff: DictDiff, table_type: str
    ) -> list[str]:
        if not prop_diff.has_changes:
            return []

        if table_type in _NO_PROPERTIES_TYPES:
            logger.warning(
                "TBLPROPERTIES not supported for %s tables; skipping %s",
                table_type,
                fqn,
            )
            return []

        queries: list[str] = []

        if prop_diff.updates:
            props_str = ", ".join(
                f"'{_esc(k)}'='{_esc(v)}'" for k, v in prop_diff.updates.items()
            )
            query = _BASE_ALTER.format(
                table_type="TABLE",
                fqn=fqn,
                action=_SET_TBLPROPERTIES.format(props=props_str),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)

        if prop_diff.deletes:
            query = _BASE_ALTER.format(
                table_type="TABLE",
                fqn=fqn,
                action=_UNSET_TBLPROPERTIES.format(
                    props=_key_list(prop_diff.deletes)),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)

        return queries

    def _column_queries(self, fqn: str, diff: TableDiff, table_type: str) -> list[str]:
        queries: list[str] = []
        for col_name, col_diff in diff.columns.items():
            if col_diff.description is not None:
                queries.extend(
                    self._column_description_queries(
                        fqn, col_name, col_diff.description, table_type
                    )
                )
            if col_diff.tags is not None and col_diff.tags.has_changes:
                queries.extend(
                    self._column_tag_queries(
                        fqn, col_name, col_diff.tags, table_type)
                )
        return queries

    def _column_description_queries(
        self, fqn: str, col_name: str, description: str, table_type: str
    ) -> list[str]:
        if table_type == "streaming_table":
            query = _BASE_ALTER.format(
                table_type="STREAMING TABLE",
                fqn=fqn,
                action=_ALTER_COLUMN.format(
                    col=col_name,
                    action=_SET_COMMENT.format(comment=_esc(description)),
                ),
            )
        else:
            query = _COMMENT_ON.format(
                type="COLUMN",
                path=f"{fqn}.{col_name}",
                comment=_esc(description),
            )
        logger.debug("Generated: %s", query)
        return [query]

    def _column_tag_queries(
        self, fqn: str, col_name: str, tag_diff: DictDiff, table_type: str
    ) -> list[str]:
        """Generate column tag SQL.

        Views require the SET TAG ON / UNSET TAG ON syntax; other table types
        use ALTER TABLE ... ALTER COLUMN ... SET TAGS.
        """
        queries: list[str] = []

        if table_type == "view":
            for key in tag_diff.deletes:
                query = _UNSET_TAG_ON.format(
                    type="COLUMN", path=f"{fqn}.{col_name}", key=key)
                logger.debug("Generated: %s", query)
                queries.append(query)
            for key, value in tag_diff.updates.items():
                unset = _UNSET_TAG_ON.format(
                    type="COLUMN", path=f"{fqn}.{col_name}", key=key)
                logger.debug("Generated (pre-unset): %s", unset)
                queries.append(unset)
                query = _SET_TAG_ON.format(
                    type="COLUMN",
                    path=f"{fqn}.{col_name}",
                    key=key,
                    value=_esc(value),
                )
                logger.debug("Generated: %s", query)
                queries.append(query)
            return queries

        sql_type = _UC_TYPE.get(table_type, "TABLE")

        if tag_diff.updates:
            query = _BASE_ALTER.format(
                table_type=sql_type,
                fqn=fqn,
                action=_ALTER_COLUMN.format(
                    col=col_name,
                    action=_SET_TAGS.format(tags=_kv_tags(tag_diff.updates)),
                ),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)

        if tag_diff.deletes:
            query = _BASE_ALTER.format(
                table_type=sql_type,
                fqn=fqn,
                action=_ALTER_COLUMN.format(
                    col=col_name,
                    action=_UNSET_TAGS.format(
                        tags=_key_list(tag_diff.deletes)),
                ),
            )
            logger.debug("Generated: %s", query)
            queries.append(query)

        return queries

    def _cluster_by_queries(self, fqn: str, diff: TableDiff, table_type: str) -> list[str]:
        if not diff.cluster_by_changed:
            return []

        if table_type in _NO_CLUSTER_TYPES:
            logger.warning(
                "CLUSTER BY not supported for %s tables; skipping %s",
                table_type,
                fqn,
            )
            return []

        cluster_by_auto = diff.cluster_by_auto
        cluster_by_cols = diff.cluster_by_cols

        if cluster_by_auto:
            query = _CLUSTER_BY_AUTO.format(fqn=fqn)
        elif cluster_by_cols:
            query = _CLUSTER_BY_COLS.format(
                fqn=fqn, cols=", ".join(cluster_by_cols))
        else:
            query = _CLUSTER_BY_NONE.format(fqn=fqn)

        logger.debug("Generated: %s", query)
        return [query]

    def _constraint_queries(
        self, fqn: str, pk_diff: ConstraintPKDiff, fk_diff: ConstraintFKDiff, table_type: str
    ) -> list[str]:
        queries: list[str] = []
        if table_type in _NO_CONSTRAINT_TYPES:
            logger.warning(
                "Constraints not supported for %s tables; skipping constraint changes on %s",
                table_type,
                fqn,)
            return queries

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
            queries.extend(self._add_foreign_key(fqn, fk))
        for fk in fk_diff.create:
            queries.extend(self._add_foreign_key(fqn, fk))

        return queries

    def _drop_constraint(self, fqn: str, name: str) -> list[str]:
        query = _DROP_CONSTRAINT.format(fqn=fqn, name=name)
        logger.debug("Generated: %s", query)
        return [query]

    def _add_primary_key(self, fqn: str, constraint: Constraint) -> list[str]:
        query = _ADD_PK.format(
            fqn=fqn,
            name=constraint.name,
            cols=", ".join(constraint.columns),
        )
        logger.debug("Generated: %s", query)
        return [query]

    def _add_foreign_key(self, fqn: str, constraint: Constraint) -> list[str]:
        query = _ADD_FK.format(
            fqn=fqn,
            name=constraint.name,
            cols=", ".join(constraint.columns),
            ref_table=constraint.reference_table,
            ref_cols=", ".join(constraint.reference_columns),
        )
        logger.debug("Generated: %s", query)
        return [query]
