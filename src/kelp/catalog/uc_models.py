"""Dataclass models for Unity Catalog table sync (v2).

Column, Table, and constraint definitions are re-exported from the core
Kelp models to avoid duplicate model definitions.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from kelp.models.project_config import RemoteCatalogConfig
from kelp.models.table import (
    Column,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    Table,
)

Constraint = PrimaryKeyConstraint | ForeignKeyConstraint

_EXPORTED_MODELS = (Column, Table, RemoteCatalogConfig)

__all__ = [
    "Column",
    "ColumnDiff",
    "Constraint",
    "ConstraintFKDiff",
    "ConstraintPKDiff",
    "DictDiff",
    "RemoteCatalogConfig",
    "Table",
    "TableDiff",
]


@dataclass
class DictDiff:
    """Difference between two string-to-string dictionaries.

    Attributes:
        updates: Keys whose values must be created or changed.
        deletes: Keys that must be removed from the remote.
    """

    updates: dict[str, str] = field(default_factory=dict)
    deletes: list[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """Return True when at least one update or delete is present."""
        return bool(self.updates or self.deletes)


@dataclass
class ColumnDiff:
    """Diff result for a single column.

    Attributes:
        description: New description value, or None if unchanged.
        tags: Tag diff, or None if unchanged.
    """

    description: str | None = None
    tags: DictDiff | None = None

    @property
    def has_changes(self) -> bool:
        """Return True when at least one field needs updating."""
        return self.description is not None or (
            self.tags is not None and self.tags.has_changes
        )


@dataclass
class ConstraintPKDiff:
    """Diff result for the table's primary-key constraint.

    Attributes:
        create: Constraint to add (did not exist remotely).
        update: Constraint whose columns changed (drop-then-recreate).
        delete: Constraint to remove (no longer exists locally).
    """

    create: Constraint | None = None
    update: Constraint | None = None
    delete: Constraint | None = None


@dataclass
class ConstraintFKDiff:
    """Diff result for foreign-key constraints on a table.

    Attributes:
        create: New constraints to add.
        update: Constraints whose definition changed (drop-then-recreate).
        delete: Constraints to remove.
    """

    create: list[Constraint] = field(default_factory=list)
    update: list[Constraint] = field(default_factory=list)
    delete: list[Constraint] = field(default_factory=list)


@dataclass
class TableDiff:
    """Complete diff between a local and a remote table definition.

    Attributes:
        table_description: New description value, or None if unchanged.
        table_tags: Tag additions/updates/deletions.
        table_properties: Property additions/updates/deletions.
        columns: Per-column diffs keyed by column name.
        constraint_pk: Primary-key constraint diff.
        constraint_fk: Foreign-key constraint diffs.
        cluster_by_changed: True if clustering configuration changed.
        cluster_by_cols: New clustering columns (when changed).
        cluster_by_auto: New clustering auto flag (when changed).
    """

    table_description: str | None = None
    table_tags: DictDiff = field(default_factory=DictDiff)
    table_properties: DictDiff = field(default_factory=DictDiff)
    columns: dict[str, ColumnDiff] = field(default_factory=dict)
    constraint_pk: ConstraintPKDiff = field(default_factory=ConstraintPKDiff)
    constraint_fk: ConstraintFKDiff = field(default_factory=ConstraintFKDiff)
    cluster_by_changed: bool = False
    cluster_by_cols: list[str] | None = None
    cluster_by_auto: bool | None = None
