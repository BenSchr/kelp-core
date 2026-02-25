"""Pure diff logic for Unity Catalog table synchronisation (v2)."""

from __future__ import annotations

import logging

from kelp.catalog.uc_models import (
    Column,
    ColumnDiff,
    Constraint,
    ConstraintFKDiff,
    ConstraintPKDiff,
    DictDiff,
    RemoteCatalogConfig,
    Table,
    TableDiff,
)
from kelp.models.table import ForeignKeyConstraint, PrimaryKeyConstraint

logger = logging.getLogger(__name__)


class TableDiffCalculator:
    """Calculate the diff between a local and a remote table.

    Args:
        config: Sync configuration controlling which fields are managed and
            whether the sync mode is "replace" or "merge" ("append" is
            treated as "merge" to align with project config values).

    """

    def __init__(self, config: RemoteCatalogConfig) -> None:
        self._config = config

    def calculate(self, local: Table, remote: Table) -> TableDiff:
        """Return a TableDiff describing every change needed.

        Args:
            local: Desired state from the project catalog.
            remote: Current state fetched from Unity Catalog.

        Returns:
            A populated TableDiff instance.

        """
        diff = TableDiff()

        diff.table_description = self._diff_description(local, remote)
        diff.table_tags = self.diff_dicts(
            local.tags,
            remote.tags,
            self._config.managed_table_tags,
            self._config.table_tag_mode,
        )
        diff.table_properties = self.diff_dicts(
            local.table_properties,
            remote.table_properties,
            self._config.managed_table_properties,
            self._config.table_property_mode,
        )
        diff.columns = self._diff_columns(local, remote)
        diff.constraint_pk, diff.constraint_fk = self._diff_constraints(local, remote)
        diff.cluster_by_changed = (
            local.cluster_by != remote.cluster_by or local.cluster_by_auto != remote.cluster_by_auto
        )
        diff.cluster_by_cols = local.cluster_by
        diff.cluster_by_auto = local.cluster_by_auto

        return diff

    @staticmethod
    def _diff_description(local: Table, remote: Table) -> str | None:
        """Return the new description if it changed, otherwise None."""
        if local.description != remote.description:
            return local.description
        return None

    @staticmethod
    def _in_scope(key: str, managed: list[str]) -> bool:
        """Return True when key should be managed.

        Args:
            key: The tag/property key to check.
            managed: Allowlist of keys the adapter can manage.

        """
        return (not managed) or (key in managed)

    def diff_dicts(
        self,
        local: dict[str, str],
        remote: dict[str, str],
        managed: list[str],
        mode: str,
    ) -> DictDiff:
        """Compare two dictionaries and return a DictDiff.

        Args:
            local: Desired state.
            remote: Current state.
            managed: Keys the adapter is allowed to manage.
            mode: "replace" removes remote keys absent locally; "merge"/"append" only adds/updates.

        Returns:
            DictDiff with updates and deletes populated.

        """
        normalized_mode = "replace" if mode == "replace" else "merge"

        local_keys = set(local.keys()) if local else set()
        remote_keys = set(remote.keys()) if remote else set()

        updates: dict[str, str] = {}
        deletes: list[str] = []

        for key in local_keys:
            if key not in remote_keys or local[key] != remote[key]:
                updates[key] = local[key]

        if normalized_mode == "replace":
            deletes.extend(
                [key for key in remote_keys - local_keys if self._in_scope(key, managed)],
            )

        return DictDiff(updates=updates, deletes=deletes)

    def _diff_columns(self, local: Table, remote: Table) -> dict[str, ColumnDiff]:
        """Produce per-column diffs for description and tag changes.

        Columns present only remotely or only locally are skipped.

        Args:
            local: Local table definition.
            remote: Remote table state.

        Returns:
            Mapping of column name to ColumnDiff.

        """
        local_map: dict[str, Column] = {c.name: c for c in local.columns}
        result: dict[str, ColumnDiff] = {}

        for rc in remote.columns:
            lc = local_map.get(rc.name)
            if lc is None:
                continue

            col_diff = ColumnDiff()

            if rc.description != lc.description:
                col_diff.description = lc.description

            tag_diff = self.diff_dicts(
                lc.tags,
                rc.tags,
                self._config.managed_column_tags,
                self._config.column_tag_mode,
            )
            if tag_diff.has_changes:
                col_diff.tags = tag_diff

            if col_diff.has_changes:
                result[rc.name] = col_diff

        return result

    def _diff_constraints(
        self,
        local: Table,
        remote: Table,
    ) -> tuple[ConstraintPKDiff, ConstraintFKDiff]:
        """Compute primary-key and foreign-key constraint diffs.

        Args:
            local: Local table definition.
            remote: Remote table state.

        Returns:
            Tuple of (ConstraintPKDiff, ConstraintFKDiff).

        """
        pk_diff = ConstraintPKDiff()
        fk_diff = ConstraintFKDiff()

        local_map: dict[str, Constraint] = {c.name: c for c in local.constraints}
        remote_map: dict[str, Constraint] = {c.name: c for c in remote.constraints}

        fk_update_names: set[str] = set()

        for rc_name, rc in remote_map.items():
            lc = local_map.get(rc_name)

            if lc is None:
                if isinstance(rc, PrimaryKeyConstraint):
                    pk_diff.delete = rc
                elif isinstance(rc, ForeignKeyConstraint):
                    fk_diff.delete.append(rc)
                continue

            if lc.type != rc.type:
                logger.warning(
                    "Constraint type change detected for '%s' (local=%s, remote=%s); "
                    "manual intervention required — skipping.",
                    rc_name,
                    lc.type,
                    rc.type,
                )
                continue

            if isinstance(lc, PrimaryKeyConstraint) and isinstance(rc, PrimaryKeyConstraint):
                if lc.columns != rc.columns:
                    pk_diff.update = lc
            elif isinstance(lc, ForeignKeyConstraint) and isinstance(rc, ForeignKeyConstraint):
                definition_changed = (
                    lc.columns != rc.columns
                    or lc.reference_table != rc.reference_table
                    or lc.reference_columns != rc.reference_columns
                )
                if definition_changed and rc_name not in fk_update_names:
                    fk_diff.update.append(lc)
                    fk_update_names.add(rc_name)

        for lc_name, lc in local_map.items():
            if remote_map.get(lc_name) is None:
                if isinstance(lc, PrimaryKeyConstraint):
                    pk_diff.create = lc
                elif isinstance(lc, ForeignKeyConstraint):
                    fk_diff.create.append(lc)

        return pk_diff, fk_diff
