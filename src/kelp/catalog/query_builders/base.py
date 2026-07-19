"""Base class for Unity Catalog table query builders.

All SQL generation shared between table types lives here, parameterized by
two class attributes (``securable`` for ``ALTER`` statements and
``comment_on_type`` for ``COMMENT ON``) and gated by :attr:`capabilities`.
Concrete subclasses only declare their capability set, the securable
keywords, and override the few methods whose SQL syntax genuinely differs
(e.g. view column tags, streaming-table column comments).
"""

import logging
from enum import StrEnum

from kelp.catalog.query_builders._sql import (
    ADD_FK,
    ADD_PK,
    ALTER_COLUMN,
    AUTO_TTL,
    BASE_ALTER,
    CLUSTER_BY_AUTO,
    CLUSTER_BY_COLS,
    CLUSTER_BY_NONE,
    COMMENT_ON,
    DROP_AUTO_TTL,
    DROP_CONSTRAINT,
    SET_TAGS,
    SET_TBLPROPERTIES,
    UNSET_TAGS,
    UNSET_TBLPROPERTIES,
    esc,
    key_list,
    kv_tags,
)
from kelp.catalog.uc_models import (
    Constraint,
    ConstraintFKDiff,
    ConstraintPKDiff,
    DictDiff,
    TableDiff,
)
from kelp.models.model import ForeignKeyConstraint

logger = logging.getLogger(__name__)


class Capability(StrEnum):
    """Declarative flags describing which SQL operations a builder supports.

    These are used by :class:`UCQueryBuilderFactory` to build the capability
    table so callers can introspect what each table type supports without
    instantiating a builder.

    Attributes:
        TABLE_DESCRIPTION: ``COMMENT ON <type> <fqn> IS '...'`` statement.
        TABLE_TAGS: ``ALTER <type> <fqn> SET/UNSET TAGS`` statements.
        TABLE_PROPERTIES: ``ALTER TABLE <fqn> SET/UNSET TBLPROPERTIES`` statements.
        COLUMN_DESCRIPTION: Per-column comment statements.
        COLUMN_TAGS: Per-column tag set/unset statements.
        CLUSTER_BY: ``ALTER TABLE <fqn> CLUSTER BY ...`` statement.
        CONSTRAINTS: ``ADD/DROP CONSTRAINT`` statements.
        AUTO_TTL: ``ALTER TABLE <fqn> DELETE ROWS <expiration_days> DAYS AFTER <time_column_name>`` statement.
    """

    TABLE_DESCRIPTION = "table_description"
    TABLE_TAGS = "table_tags"
    TABLE_PROPERTIES = "table_properties"
    COLUMN_DESCRIPTION = "column_description"
    COLUMN_TAGS = "column_tags"
    CLUSTER_BY = "cluster_by"
    CONSTRAINTS = "constraints"
    AUTO_TTL = "auto_ttl"


class BaseTableQueryBuilder:
    """Shared per-table-type SQL generation.

    Every statement kind is implemented here once; :meth:`build` only calls
    the methods whose :class:`Capability` the concrete subclass declares.

    Attributes:
        capabilities: Frozen set of :class:`Capability` values this builder
            supports. Declared on each concrete subclass.
        securable: Keyword used in ``ALTER <securable> <fqn> …`` statements
            (e.g. ``"TABLE"``, ``"VIEW"``, ``"STREAMING TABLE"``).
        comment_on_type: Keyword used in ``COMMENT ON <type> <fqn> …``
            statements. Usually matches ``securable``, but e.g. materialized
            views require ``COMMENT ON TABLE``.

    """

    capabilities: frozenset[Capability] = frozenset()
    securable: str = "TABLE"
    comment_on_type: str = "TABLE"

    def build(self, fqn: str, diff: TableDiff) -> list[str]:
        """Build all SQL queries required for the given diff.

        Calls each capability's generator in dependency order, skipping
        capabilities this builder doesn't declare.

        Args:
            fqn: Fully-qualified table name (``catalog.schema.table``).
            diff: Diff produced by :class:`~kelp.catalog.uc_diff.TableDiffCalculator`.

        Returns:
            Ordered list of SQL strings ready for execution.

        """
        caps = self.capabilities
        queries: list[str] = []
        if Capability.TABLE_DESCRIPTION in caps:
            queries.extend(self.description_queries(fqn, diff))
        if Capability.TABLE_TAGS in caps:
            queries.extend(self.table_tag_queries(fqn, diff.table_tags))
        if Capability.TABLE_PROPERTIES in caps:
            queries.extend(self.table_property_queries(fqn, diff.table_properties))
        if caps & {Capability.COLUMN_DESCRIPTION, Capability.COLUMN_TAGS}:
            queries.extend(self.column_queries(fqn, diff))
        if Capability.CLUSTER_BY in caps:
            queries.extend(self.cluster_by_queries(fqn, diff))
        if Capability.CONSTRAINTS in caps:
            queries.extend(self.constraint_queries(fqn, diff.constraint_pk, diff.constraint_fk))
        if Capability.AUTO_TTL in caps:
            queries.extend(self.auto_ttl_queries(fqn, diff))

        return queries

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _alter(self, fqn: str, action: str) -> str:
        """Format and log an ``ALTER <securable> <fqn> <action>`` statement."""
        query = BASE_ALTER.format(table_type=self.securable, fqn=fqn, action=action)
        logger.debug("Generated: %s", query)
        return query

    # ------------------------------------------------------------------
    # Statement generators — subclasses override only for syntax deltas
    # ------------------------------------------------------------------

    def description_queries(self, fqn: str, diff: TableDiff) -> list[str]:
        """Generate a ``COMMENT ON <comment_on_type>`` statement.

        Args:
            fqn: Fully-qualified table name.
            diff: Full table diff (uses ``diff.table_description``).

        Returns:
            Zero or one SQL string.

        """
        if diff.table_description is None:
            return []
        query = COMMENT_ON.format(
            type=self.comment_on_type, path=fqn, comment=esc(diff.table_description)
        )
        logger.debug("Generated: %s", query)
        return [query]

    def table_tag_queries(self, fqn: str, tag_diff: DictDiff) -> list[str]:
        """Generate ``ALTER <securable> … SET/UNSET TAGS`` statements.

        Args:
            fqn: Fully-qualified table name.
            tag_diff: Tag diff (updates and deletes).

        Returns:
            Zero, one, or two SQL strings (SET then UNSET).

        """
        queries: list[str] = []
        if tag_diff.updates:
            queries.append(self._alter(fqn, SET_TAGS.format(tags=kv_tags(tag_diff.updates))))
        if tag_diff.deletes:
            queries.append(self._alter(fqn, UNSET_TAGS.format(tags=key_list(tag_diff.deletes))))
        return queries

    def table_property_queries(self, fqn: str, prop_diff: DictDiff) -> list[str]:
        """Generate ``ALTER <securable> … SET/UNSET TBLPROPERTIES`` statements.

        Args:
            fqn: Fully-qualified table name.
            prop_diff: Property diff (updates and deletes).

        Returns:
            Zero, one, or two SQL strings.

        """
        queries: list[str] = []
        if prop_diff.updates:
            queries.append(
                self._alter(fqn, SET_TBLPROPERTIES.format(props=kv_tags(prop_diff.updates)))
            )
        if prop_diff.deletes:
            queries.append(
                self._alter(fqn, UNSET_TBLPROPERTIES.format(props=key_list(prop_diff.deletes)))
            )
        return queries

    def column_queries(self, fqn: str, diff: TableDiff) -> list[str]:
        """Generate all column-level description and tag statements.

        Args:
            fqn: Fully-qualified table name.
            diff: Full table diff (uses ``diff.columns``).

        Returns:
            One statement per changed column attribute.

        """
        caps = self.capabilities
        queries: list[str] = []
        for col_name, col_diff in diff.columns.items():
            if Capability.COLUMN_DESCRIPTION in caps and col_diff.description is not None:
                queries.extend(self.column_description_queries(fqn, col_name, col_diff.description))
            if (
                Capability.COLUMN_TAGS in caps
                and col_diff.tags is not None
                and col_diff.tags.has_changes
            ):
                queries.extend(self.column_tag_queries(fqn, col_name, col_diff.tags))
        return queries

    def column_description_queries(self, fqn: str, col_name: str, description: str) -> list[str]:
        """Generate a ``COMMENT ON COLUMN`` statement.

        Args:
            fqn: Fully-qualified table name.
            col_name: Column name.
            description: New column description.

        Returns:
            One SQL string.

        """
        query = COMMENT_ON.format(type="COLUMN", path=f"{fqn}.{col_name}", comment=esc(description))
        logger.debug("Generated: %s", query)
        return [query]

    def column_tag_queries(self, fqn: str, col_name: str, tag_diff: DictDiff) -> list[str]:
        """Generate ``ALTER <securable> … ALTER COLUMN … SET/UNSET TAGS`` statements.

        Args:
            fqn: Fully-qualified table name.
            col_name: Column name.
            tag_diff: Tag diff for the column (updates and deletes).

        Returns:
            Zero, one, or two SQL strings.

        """
        queries: list[str] = []
        if tag_diff.updates:
            action = ALTER_COLUMN.format(
                col=col_name, action=SET_TAGS.format(tags=kv_tags(tag_diff.updates))
            )
            queries.append(self._alter(fqn, action))
        if tag_diff.deletes:
            action = ALTER_COLUMN.format(
                col=col_name, action=UNSET_TAGS.format(tags=key_list(tag_diff.deletes))
            )
            queries.append(self._alter(fqn, action))
        return queries

    def cluster_by_queries(self, fqn: str, diff: TableDiff) -> list[str]:
        """Generate an ``ALTER TABLE … CLUSTER BY`` statement.

        Args:
            fqn: Fully-qualified table name.
            diff: Full table diff (uses ``diff.cluster_by_*`` fields).

        Returns:
            Zero or one SQL string.

        """
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
        """Generate ``ADD/DROP CONSTRAINT`` statements for PK and FK changes.

        Args:
            fqn: Fully-qualified table name.
            pk_diff: Primary-key constraint diff.
            fk_diff: Foreign-key constraint diffs.

        Returns:
            Ordered list of DROP / ADD CONSTRAINT statements.

        """
        queries: list[str] = []
        if pk_diff.delete is not None:
            queries.append(self._drop_constraint(fqn, pk_diff.delete.name))
        if pk_diff.update is not None:
            queries.append(self._drop_constraint(fqn, pk_diff.update.name))
            queries.append(self._add_primary_key(fqn, pk_diff.update))
        if pk_diff.create is not None:
            queries.append(self._add_primary_key(fqn, pk_diff.create))
        queries.extend(self._drop_constraint(fqn, fk.name) for fk in fk_diff.delete)
        for fk in fk_diff.update:
            queries.append(self._drop_constraint(fqn, fk.name))
            if isinstance(fk, ForeignKeyConstraint):
                queries.append(self._add_foreign_key(fqn, fk))
        queries.extend(
            self._add_foreign_key(fqn, fk)
            for fk in fk_diff.create
            if isinstance(fk, ForeignKeyConstraint)
        )
        return queries

    def _drop_constraint(self, fqn: str, name: str) -> str:
        query = DROP_CONSTRAINT.format(fqn=fqn, name=name)
        logger.debug("Generated: %s", query)
        return query

    def _add_primary_key(self, fqn: str, constraint: Constraint) -> str:
        query = ADD_PK.format(fqn=fqn, name=constraint.name, cols=", ".join(constraint.columns))
        logger.debug("Generated: %s", query)
        return query

    def _add_foreign_key(self, fqn: str, constraint: ForeignKeyConstraint) -> str:
        query = ADD_FK.format(
            fqn=fqn,
            name=constraint.name,
            cols=", ".join(constraint.columns),
            ref_table=constraint.reference_table,
            ref_cols=", ".join(constraint.reference_columns),
        )
        logger.debug("Generated: %s", query)
        return query

    def auto_ttl_queries(self, fqn: str, diff: TableDiff) -> list[str]:
        """Generate an automatic-TTL statement.

        Args:
            fqn: Fully-qualified table name.
            diff: Full table diff (uses ``diff.auto_ttl_changed`` and ``diff.auto_ttl``).

        Returns:
            Zero or one SQL string.

        """
        if not diff.auto_ttl_changed:
            return []
        if diff.auto_ttl is None:
            query = DROP_AUTO_TTL.format(fqn=fqn)
        else:
            query = AUTO_TTL.format(
                fqn=fqn,
                expiration=diff.auto_ttl.expire_in_days,
                time_col=diff.auto_ttl.timestamp_column,
            )
        logger.debug("Generated: %s", query)
        return [query]
