"""Base interface for Unity Catalog table query builders."""

from __future__ import annotations

from enum import StrEnum

from kelp.catalog.uc_models import (
    ConstraintFKDiff,
    ConstraintPKDiff,
    DictDiff,
    TableDiff,
)


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
    """

    TABLE_DESCRIPTION = "table_description"
    TABLE_TAGS = "table_tags"
    TABLE_PROPERTIES = "table_properties"
    COLUMN_DESCRIPTION = "column_description"
    COLUMN_TAGS = "column_tags"
    CLUSTER_BY = "cluster_by"
    CONSTRAINTS = "constraints"


class BaseTableQueryBuilder:
    """Interface for per-table-type SQL generation.

    Each concrete subclass implements the methods that correspond to its
    supported :class:`Capability` values.  All methods default to returning
    an empty list, so subclasses only need to override what they support.

    The :meth:`build` method is the single entry-point: it calls every
    sub-method in order and assembles the final statement list.

    Attributes:
        capabilities: Frozen set of :class:`Capability` values this builder
            supports.  Populated on each concrete subclass.

    """

    capabilities: frozenset[Capability] = frozenset()

    # ------------------------------------------------------------------
    # Main orchestrator (concrete - not intended to be overridden)
    # ------------------------------------------------------------------

    def build(self, fqn: str, diff: TableDiff) -> list[str]:
        """Build all SQL queries required for the given diff.

        Calls every sub-method in dependency order and concatenates the
        results into a single ordered list.

        Args:
            fqn: Fully-qualified table name (``catalog.schema.table``).
            diff: Diff produced by :class:`~kelp.catalog.uc_diff.TableDiffCalculator`.

        Returns:
            Ordered list of SQL strings ready for execution.

        """
        queries: list[str] = []
        queries.extend(self.description_queries(fqn, diff))
        queries.extend(self.table_tag_queries(fqn, diff.table_tags))
        queries.extend(self.table_property_queries(fqn, diff.table_properties))
        queries.extend(self.column_queries(fqn, diff))
        queries.extend(self.cluster_by_queries(fqn, diff))
        queries.extend(self.constraint_queries(fqn, diff.constraint_pk, diff.constraint_fk))
        return queries

    # ------------------------------------------------------------------
    # Sub-methods - all default to no-op; subclasses override as needed
    # ------------------------------------------------------------------

    def description_queries(self, fqn: str, diff: TableDiff) -> list[str]:
        """Generate SQL to update the table-level description.

        Args:
            fqn: Fully-qualified table name.
            diff: Full table diff (use ``diff.table_description``).

        Returns:
            Zero or one SQL string.

        """
        return []

    def table_tag_queries(self, fqn: str, tag_diff: DictDiff) -> list[str]:
        """Generate SQL to update table-level tags.

        Args:
            fqn: Fully-qualified table name.
            tag_diff: Tag diff (updates and deletes).

        Returns:
            Zero, one, or two SQL strings (SET then UNSET).

        """
        return []

    def table_property_queries(self, fqn: str, prop_diff: DictDiff) -> list[str]:
        """Generate SQL to update TBLPROPERTIES.

        Args:
            fqn: Fully-qualified table name.
            prop_diff: Property diff (updates and deletes).

        Returns:
            Zero, one, or two SQL strings.

        """
        return []

    def column_queries(self, fqn: str, diff: TableDiff) -> list[str]:
        """Generate SQL for all column-level description and tag changes.

        Args:
            fqn: Fully-qualified table name.
            diff: Full table diff (use ``diff.columns``).

        Returns:
            One statement per changed column attribute.

        """
        return []

    def cluster_by_queries(self, fqn: str, diff: TableDiff) -> list[str]:
        """Generate SQL to update the CLUSTER BY configuration.

        Args:
            fqn: Fully-qualified table name.
            diff: Full table diff (use ``diff.cluster_by_*`` fields).

        Returns:
            Zero or one SQL string.

        """
        return []

    def constraint_queries(
        self,
        fqn: str,
        pk_diff: ConstraintPKDiff,
        fk_diff: ConstraintFKDiff,
    ) -> list[str]:
        """Generate SQL for primary-key and foreign-key constraint changes.

        Args:
            fqn: Fully-qualified table name.
            pk_diff: Primary-key constraint diff.
            fk_diff: Foreign-key constraint diffs.

        Returns:
            Ordered list of DROP / ADD CONSTRAINT statements.

        """
        return []
