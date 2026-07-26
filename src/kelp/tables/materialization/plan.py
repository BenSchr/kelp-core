"""Pure planning layer: config plus column names in, merge plan out.

The plan is a description of a Delta ``MERGE`` — conditions, value maps and
clause order — with no Spark objects involved, so the full option matrix can be
asserted in fast unit tests. :mod:`kelp.tables.materialization.execute` turns a
plan into ``DeltaMergeBuilder`` calls.
"""

from dataclasses import dataclass, field
from typing import Literal

from kelp.models.model_mat_config import (
    SOURCE_ALIAS,
    TARGET_ALIAS,
    ColumnSelector,
    MergeConfig,
    Scd2Columns,
    Scd2Config,
)
from kelp.tables.materialization.columns import (
    HELPER_COLUMNS,
    IS_DELETE_COL,
    change_condition,
    combine,
    index,
    newer_condition,
    qualified,
    resolve,
    without,
)


@dataclass(frozen=True)
class MergeAction:
    """One ``WHEN ...`` clause of a Delta merge.

    Attributes:
        clause: Which merge clause the action belongs to.
        action: What the clause does.
        condition: Optional SQL condition guarding the clause.
        values: Column assignments for update/insert actions.
    """

    clause: Literal["matched", "not_matched", "not_matched_by_source"]
    action: Literal["update", "insert", "delete"]
    condition: str | None = None
    values: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class MergePlan:
    """A fully resolved Delta merge.

    Attributes:
        merge_condition: The ``ON`` condition.
        actions: Clauses in the order they must be registered.
        write_columns: Source columns written to the target.
        schema_evolution: Whether to enable merge schema evolution.
        warnings: Human-readable notes about ignored or unknown configuration.
    """

    merge_condition: str
    actions: tuple[MergeAction, ...]
    write_columns: tuple[str, ...] = ()
    schema_evolution: bool = True
    warnings: tuple[str, ...] = ()


def _key_condition(keys: list[str], source: dict[str, str], target: dict[str, str]) -> str:
    """Build the key equality part of a merge condition."""
    return " AND ".join(
        f"{qualified(SOURCE_ALIAS, source[key.lower()])} = "
        f"{qualified(TARGET_ALIAS, target[key.lower()])}"
        for key in keys
    )


def _selector_warnings(
    selector: ColumnSelector | None, label: str, available: dict[str, str]
) -> list[str]:
    """Report selector entries that match no available column."""
    if selector is None:
        return []
    _, missing = resolve(selector.names(), available)
    if not missing:
        return []
    return [f"{label} references unknown column(s): {', '.join(missing)}"]


def _write_candidates(
    source_columns: list[str],
    target_columns: list[str],
    *,
    schema_evolution: bool,
) -> list[str]:
    """Return the source columns eligible to be written to the target."""
    target = index(target_columns)
    writable = [col for col in source_columns if col.lower() in target]
    if schema_evolution:
        writable += [col for col in source_columns if col.lower() not in target]
    return without(writable, list(HELPER_COLUMNS))


def plan_merge(
    *,
    config: MergeConfig,
    source_columns: list[str],
    target_columns: list[str],
) -> MergePlan:
    """Plan an merge (SCD type 1) merge.

    Args:
        config: Effective merge configuration.
        source_columns: Columns of the incoming DataFrame, helper columns excluded.
        target_columns: Columns of the existing target table.

    Returns:
        The merge plan.

    Raises:
        ValueError: If a key or sequence column is missing on either side.
    """
    source = index(source_columns)
    target = index(target_columns)
    warnings: list[str] = []

    keys, missing_source_keys = resolve(config.keys, source)
    if missing_source_keys:
        raise ValueError(
            "Key column(s) missing from the source DataFrame: " + ", ".join(missing_source_keys)
        )
    _, missing_target_keys = resolve(config.keys, target)
    if missing_target_keys:
        raise ValueError(
            "Key column(s) missing from the target table: " + ", ".join(missing_target_keys)
        )
    _, missing_sequence = resolve(config.sequence_by, source)
    if missing_sequence:
        raise ValueError(
            "sequence_by column(s) missing from the source DataFrame: "
            + ", ".join(missing_sequence)
        )

    selector = config.columns or ColumnSelector()
    warnings += _selector_warnings(config.columns, "columns", source)
    warnings += _selector_warnings(config.track_changes, "track_changes", source)
    warnings += _selector_warnings(
        config.ignore_null_updates_columns, "ignore_null_updates_columns", source
    )

    candidates = _write_candidates(
        source_columns, target_columns, schema_evolution=config.schema_evolution
    )
    write_columns = selector.apply(candidates, required=keys)

    update_columns = without(write_columns, [*keys, *config.insert_only_columns])
    comparable = [col for col in update_columns if col.lower() in target]
    tracked = (config.track_changes or ColumnSelector()).apply(comparable)

    sequence_guard = newer_condition(config.sequence_by, source=source, target=target)
    not_deleted = f"NOT {qualified(SOURCE_ALIAS, IS_DELETE_COL)}" if config.when_deleted else None
    sql = config.sql_conditions

    merge_condition = combine(_key_condition(keys, source, target), config.where) or "1 = 1"

    actions: list[MergeAction] = []

    if config.when_deleted:
        actions.append(
            MergeAction(
                clause="matched",
                action="delete",
                condition=combine(qualified(SOURCE_ALIAS, IS_DELETE_COL), sequence_guard),
            )
        )

    if update_columns:
        matched = (sql.when_matched if sql else None) or change_condition(tracked)
        ignore_null = {col.lower() for col in config.ignore_null_columns(update_columns)}
        update_values = {}
        for col in update_columns:
            source_ref = qualified(SOURCE_ALIAS, col)
            if col.lower() in ignore_null and col.lower() in target:
                update_values[f"`{col}`"] = (
                    f"coalesce({source_ref}, {qualified(TARGET_ALIAS, col)})"
                )
            else:
                update_values[f"`{col}`"] = source_ref
        actions.append(
            MergeAction(
                clause="matched",
                action="update",
                condition=combine(not_deleted, matched, sequence_guard),
                values=update_values,
            )
        )

    if write_columns:
        actions.append(
            MergeAction(
                clause="not_matched",
                action="insert",
                condition=combine(not_deleted, sql.when_not_matched if sql else None),
                values={f"`{col}`": qualified(SOURCE_ALIAS, col) for col in write_columns},
            )
        )

    if config.missing_in_source == "delete":
        actions.append(
            MergeAction(
                clause="not_matched_by_source",
                action="delete",
                condition=sql.when_not_matched_by_source if sql else None,
            )
        )
    elif sql and sql.when_not_matched_by_source:
        warnings.append(
            "sql_conditions.when_not_matched_by_source is ignored unless missing_in_source='delete'"
        )

    return MergePlan(
        merge_condition=merge_condition,
        actions=tuple(actions),
        write_columns=tuple(write_columns),
        schema_evolution=config.schema_evolution,
        warnings=tuple(warnings),
    )


def plan_scd2(
    *,
    config: Scd2Config,
    write_columns: list[str],
    target_columns: list[str],
) -> MergePlan:
    """Plan the reconciling merge for SCD2 versions.

    The source of this merge is the rebuilt version history produced by
    :func:`kelp.tables.materialization.strategies.scd2.build_versions`, so the
    merge only has to close intervals whose ``valid_to`` changed and insert
    versions that do not exist yet.

    Args:
        config: Effective SCD2 configuration.
        write_columns: Business columns of the rebuilt versions.
        target_columns: Columns of the existing target table.

    Returns:
        The merge plan.

    Raises:
        ValueError: If the target lacks the configured history columns.
    """
    target = index(target_columns)
    history: Scd2Columns = config.history

    _, missing_history = resolve(history.all_names(), target)
    if missing_history:
        raise ValueError(
            f"SCD2 target is missing history column(s): {', '.join(missing_history)}. "
            "Add them to the model (kelp generates them for mode: scd2) or let kelp "
            "create the table."
        )

    source = index([*write_columns, *history.all_names()])
    keys, missing_keys = resolve(config.keys, source)
    if missing_keys:
        raise ValueError("Key column(s) missing from the SCD2 versions: " + ", ".join(missing_keys))

    valid_from = history.valid_from
    valid_to = history.valid_to

    merge_condition = combine(
        _key_condition(keys, source, target),
        f"{qualified(SOURCE_ALIAS, valid_from)} <=> {qualified(TARGET_ALIAS, valid_from)}",
        config.where,
    )

    close_values = {f"`{valid_to}`": qualified(SOURCE_ALIAS, valid_to)}
    changed = f"NOT ({qualified(SOURCE_ALIAS, valid_to)} <=> {qualified(TARGET_ALIAS, valid_to)})"
    if history.is_current:
        close_values[f"`{history.is_current}`"] = qualified(SOURCE_ALIAS, history.is_current)
        changed = (
            f"({changed}) OR NOT ({qualified(SOURCE_ALIAS, history.is_current)} <=> "
            f"{qualified(TARGET_ALIAS, history.is_current)})"
        )

    insert_columns = [*write_columns, *history.all_names()]

    return MergePlan(
        merge_condition=merge_condition or "1 = 1",
        actions=(
            MergeAction(
                clause="matched",
                action="update",
                condition=changed,
                values=close_values,
            ),
            MergeAction(
                clause="not_matched",
                action="insert",
                values={f"`{col}`": qualified(SOURCE_ALIAS, col) for col in insert_columns},
            ),
        ),
        write_columns=tuple(write_columns),
        schema_evolution=config.schema_evolution,
    )
