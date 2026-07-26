"""Unit tests for the pure merge planner — no Spark session involved."""

import pytest

from kelp.models.model_mat_config import (
    ColumnSelector,
    MergeConfig,
    Scd2Columns,
    Scd2Config,
    SqlConditions,
)
from kelp.tables.materialization.plan import MergeAction, MergePlan, plan_merge, plan_scd2

SOURCE = ["id", "name", "city", "updated_at", "_op"]
TARGET = ["id", "name", "city", "updated_at"]


def _action(plan: MergePlan, clause: str, action: str) -> MergeAction:
    """Return the single action matching a clause/action pair."""
    matches = [a for a in plan.actions if a.clause == clause and a.action == action]
    assert len(matches) == 1, f"expected exactly one {clause}/{action} action, got {len(matches)}"
    return matches[0]


def test_merge_plan_defaults() -> None:
    """The default plan updates changed rows and inserts unmatched ones."""
    plan = plan_merge(
        config=MergeConfig(keys=["id"]),
        source_columns=TARGET,
        target_columns=TARGET,
    )

    assert plan.merge_condition == "source.`id` = target.`id`"
    assert plan.write_columns == ("id", "name", "city", "updated_at")

    update = _action(plan, "matched", "update")
    assert update.condition == (
        "NOT (source.`name` <=> target.`name`) OR "
        "NOT (source.`city` <=> target.`city`) OR "
        "NOT (source.`updated_at` <=> target.`updated_at`)"
    )
    assert update.values == {
        "`name`": "source.`name`",
        "`city`": "source.`city`",
        "`updated_at`": "source.`updated_at`",
    }

    insert = _action(plan, "not_matched", "insert")
    assert insert.condition is None
    assert insert.values == {
        "`id`": "source.`id`",
        "`name`": "source.`name`",
        "`city`": "source.`city`",
        "`updated_at`": "source.`updated_at`",
    }
    assert plan.schema_evolution is True


def test_merge_plan_columns_selector_limits_write_set() -> None:
    """Unselected source columns reach neither the update nor the insert."""
    plan = plan_merge(
        config=MergeConfig(keys=["id"], columns=ColumnSelector(exclude=["_op", "city"])),
        source_columns=SOURCE,
        target_columns=TARGET,
    )

    assert plan.write_columns == ("id", "name", "updated_at")
    assert "`city`" not in _action(plan, "matched", "update").values
    assert "`_op`" not in _action(plan, "not_matched", "insert").values


def test_merge_plan_track_changes_narrows_change_detection() -> None:
    """Only tracked columns take part in the derived change condition."""
    plan = plan_merge(
        config=MergeConfig(keys=["id"], track_changes=ColumnSelector(include=["city"])),
        source_columns=TARGET,
        target_columns=TARGET,
    )

    assert _action(plan, "matched", "update").condition == "NOT (source.`city` <=> target.`city`)"


def test_merge_plan_insert_only_columns_are_not_updated() -> None:
    """Insert-only columns are written once and never overwritten."""
    plan = plan_merge(
        config=MergeConfig(keys=["id"], insert_only_columns=["city"]),
        source_columns=TARGET,
        target_columns=TARGET,
    )

    assert "`city`" not in _action(plan, "matched", "update").values
    assert "`city`" in _action(plan, "not_matched", "insert").values


def test_merge_plan_sequence_guard_blocks_out_of_order_rows() -> None:
    """A sequence column adds a "source is newer" guard to updates and deletes."""
    plan = plan_merge(
        config=MergeConfig(keys=["id"], sequence_by=["updated_at"], when_deleted="_op = 'D'"),
        source_columns=SOURCE,
        target_columns=TARGET,
    )

    guard = "source.`updated_at` > target.`updated_at`"
    assert guard in (_action(plan, "matched", "update").condition or "")
    assert _action(plan, "matched", "delete").condition == (
        f"(source.`__kelp_is_delete`) AND ({guard})"
    )


def test_merge_plan_multi_column_sequence_compares_structs() -> None:
    """Multi-column sequences are compared as structs so ordering is well defined."""
    plan = plan_merge(
        config=MergeConfig(keys=["id"], sequence_by=["updated_at", "city"]),
        source_columns=TARGET,
        target_columns=TARGET,
    )

    assert (
        "struct(source.`updated_at`, source.`city`) > struct(target.`updated_at`, target.`city`)"
        in (_action(plan, "matched", "update").condition or "")
    )


def test_merge_plan_delete_clause_precedes_update() -> None:
    """Delta evaluates matched clauses in order, so deletes must be registered first."""
    plan = plan_merge(
        config=MergeConfig(keys=["id"], when_deleted="_op = 'D'"),
        source_columns=SOURCE,
        target_columns=TARGET,
    )

    matched = [a.action for a in plan.actions if a.clause == "matched"]
    assert matched == ["delete", "update"]
    assert "NOT source.`__kelp_is_delete`" in (_action(plan, "matched", "update").condition or "")
    assert _action(plan, "not_matched", "insert").condition == "NOT source.`__kelp_is_delete`"


def test_merge_plan_ignore_null_updates_coalesces() -> None:
    """Ignoring NULL updates coalesces the source value onto the stored one."""
    plan = plan_merge(
        config=MergeConfig(keys=["id"], ignore_null_updates=True),
        source_columns=TARGET,
        target_columns=TARGET,
    )

    update = _action(plan, "matched", "update")
    assert update.values["`name`"] == "coalesce(source.`name`, target.`name`)"
    assert update.values["`city`"] == "coalesce(source.`city`, target.`city`)"


def test_merge_plan_ignore_null_updates_can_be_scoped() -> None:
    """The rule can be narrowed to a subset of columns, like AUTO CDC allows."""
    plan = plan_merge(
        config=MergeConfig(
            keys=["id"],
            ignore_null_updates=True,
            ignore_null_updates_columns=ColumnSelector(include=["name"]),
        ),
        source_columns=TARGET,
        target_columns=TARGET,
    )

    update = _action(plan, "matched", "update")
    assert update.values["`name`"] == "coalesce(source.`name`, target.`name`)"
    assert update.values["`city`"] == "source.`city`"


def test_merge_plan_schema_evolution_off_drops_new_columns() -> None:
    """Without schema evolution, source-only columns are not written."""
    plan = plan_merge(
        config=MergeConfig(keys=["id"], schema_evolution=False),
        source_columns=[*TARGET, "status"],
        target_columns=TARGET,
    )

    assert "status" not in plan.write_columns
    assert plan.schema_evolution is False


def test_merge_plan_new_columns_are_written_but_not_compared() -> None:
    """A source-only column is written, but cannot be compared against the target yet."""
    plan = plan_merge(
        config=MergeConfig(keys=["id"]),
        source_columns=[*TARGET, "status"],
        target_columns=TARGET,
    )

    assert "status" in plan.write_columns
    assert "status" in _action(plan, "matched", "update").values["`status`"]
    assert "status" not in (_action(plan, "matched", "update").condition or "")


def test_merge_plan_where_narrows_merge_condition() -> None:
    """A where predicate is ANDed onto the key condition."""
    plan = plan_merge(
        config=MergeConfig(keys=["id"], where="target.`city` = 'Berlin'"),
        source_columns=TARGET,
        target_columns=TARGET,
    )

    assert plan.merge_condition == "(source.`id` = target.`id`) AND (target.`city` = 'Berlin')"


def test_merge_plan_missing_in_source_delete() -> None:
    """Deleting rows missing from the source adds a not-matched-by-source clause."""
    plan = plan_merge(
        config=MergeConfig(keys=["id"], missing_in_source="delete"),
        source_columns=TARGET,
        target_columns=TARGET,
    )

    assert _action(plan, "not_matched_by_source", "delete").condition is None


def test_merge_plan_sql_overrides_replace_derived_conditions() -> None:
    """The escape hatch replaces the derived change condition."""
    plan = plan_merge(
        config=MergeConfig(
            keys=["id"],
            sql_conditions=SqlConditions(
                when_matched="source.`name` <> target.`name`", when_not_matched="true"
            ),
        ),
        source_columns=TARGET,
        target_columns=TARGET,
    )

    assert _action(plan, "matched", "update").condition == "source.`name` <> target.`name`"
    assert _action(plan, "not_matched", "insert").condition == "true"


def test_merge_plan_warns_on_unknown_selector_columns() -> None:
    """A typo in a selector is reported instead of silently doing nothing."""
    plan = plan_merge(
        config=MergeConfig(keys=["id"], columns=ColumnSelector(exclude=["nmae"])),
        source_columns=TARGET,
        target_columns=TARGET,
    )

    assert any("nmae" in warning for warning in plan.warnings)


def test_merge_plan_warns_when_by_source_condition_is_unused() -> None:
    """A by-source condition without the delete policy has no effect."""
    plan = plan_merge(
        config=MergeConfig(
            keys=["id"], sql_conditions=SqlConditions(when_not_matched_by_source="true")
        ),
        source_columns=TARGET,
        target_columns=TARGET,
    )

    assert any("missing_in_source" in warning for warning in plan.warnings)


@pytest.mark.parametrize(
    ("source_columns", "target_columns", "message"),
    [
        (["name"], TARGET, "source DataFrame"),
        (TARGET, ["name"], "target table"),
    ],
)
def test_merge_plan_rejects_missing_keys(
    source_columns: list[str], target_columns: list[str], message: str
) -> None:
    """Keys must exist on both sides before any write happens."""
    with pytest.raises(ValueError, match=message):
        plan_merge(
            config=MergeConfig(keys=["id"]),
            source_columns=source_columns,
            target_columns=target_columns,
        )


def test_scd2_plan_closes_intervals_and_inserts_versions() -> None:
    """The SCD2 merge updates changed interval ends and inserts new versions."""
    config = Scd2Config(keys=["id"], sequence_by=["sequence_num"])
    plan = plan_scd2(
        config=config,
        write_columns=["id", "name", "city", "sequence_num"],
        target_columns=["id", "name", "city", "sequence_num", "__START_AT", "__END_AT"],
    )

    assert plan.merge_condition == (
        "(source.`id` = target.`id`) AND (source.`__START_AT` <=> target.`__START_AT`)"
    )

    update = _action(plan, "matched", "update")
    assert update.condition == "NOT (source.`__END_AT` <=> target.`__END_AT`)"
    assert update.values == {"`__END_AT`": "source.`__END_AT`"}

    insert = _action(plan, "not_matched", "insert")
    assert set(insert.values) == {
        "`id`",
        "`name`",
        "`city`",
        "`sequence_num`",
        "`__START_AT`",
        "`__END_AT`",
    }


def test_scd2_plan_maintains_is_current_when_configured() -> None:
    """An is_current flag is closed together with the interval."""
    config = Scd2Config(
        keys=["id"],
        sequence_by=["sequence_num"],
        history=Scd2Columns(valid_from="valid_from", valid_to="valid_to", is_current="is_current"),
    )
    plan = plan_scd2(
        config=config,
        write_columns=["id", "name"],
        target_columns=["id", "name", "valid_from", "valid_to", "is_current"],
    )

    update = _action(plan, "matched", "update")
    assert update.values == {
        "`valid_to`": "source.`valid_to`",
        "`is_current`": "source.`is_current`",
    }
    assert "is_current" in (update.condition or "")


def test_scd2_plan_requires_history_columns_on_target() -> None:
    """A target without history columns fails with an actionable message."""
    with pytest.raises(ValueError, match="__START_AT"):
        plan_scd2(
            config=Scd2Config(keys=["id"], sequence_by=["sequence_num"]),
            write_columns=["id", "name"],
            target_columns=["id", "name"],
        )
