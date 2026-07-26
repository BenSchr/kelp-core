"""Execution layer: turn a :class:`~kelp.tables.materialization.plan.MergePlan` into Delta calls."""

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.column import Column

from kelp.models.model_mat_config import SOURCE_ALIAS, TARGET_ALIAS
from kelp.tables.materialization.plan import MergePlan

logger = logging.getLogger(__name__)


def execute_plan(
    *,
    target: DeltaTable,
    source: DataFrame,
    plan: MergePlan,
    model_name: str | None = None,
) -> None:
    """Run the merge described by ``plan``.

    Args:
        target: Delta table to merge into.
        source: Source DataFrame.
        plan: Resolved merge plan.
        model_name: Optional name used for contextual logging.

    Raises:
        ValueError: If the plan contains an unsupported clause/action combination.
    """
    for warning in plan.warnings:
        logger.warning("%s: %s", model_name or "materialization", warning)

    logger.debug("Merge condition for '%s': %s", model_name, plan.merge_condition)

    builder = target.alias(TARGET_ALIAS).merge(source.alias(SOURCE_ALIAS), plan.merge_condition)

    for action in plan.actions:
        # The plan holds SQL as plain strings; Delta accepts str or Column here.
        values: dict[str, str | Column] = dict(action.values)
        if action.clause == "matched":
            if action.action == "delete":
                builder = builder.whenMatchedDelete(condition=action.condition)
            elif action.action == "update":
                builder = builder.whenMatchedUpdate(condition=action.condition, set=values)
            else:
                raise ValueError(f"Unsupported matched action: {action.action}")
        elif action.clause == "not_matched":
            if action.action != "insert":
                raise ValueError(f"Unsupported not-matched action: {action.action}")
            if action.condition is None:
                builder = builder.whenNotMatchedInsert(values=values)
            else:
                builder = builder.whenNotMatchedInsert(condition=action.condition, values=values)
        else:
            if action.action != "delete":
                raise ValueError(f"Unsupported not-matched-by-source action: {action.action}")
            builder = builder.whenNotMatchedBySourceDelete(condition=action.condition)

    if plan.schema_evolution:
        builder = builder.withSchemaEvolution()

    builder.execute()
