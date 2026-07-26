"""Materialization primitives for Kelp table writes.

Usable with or without kelp metadata: an unqualified name is resolved against a
model's ``materialization:`` block, a qualified table name is written as given.
"""

from kelp.models.model_mat_config import (
    AppendConfig,
    ColumnSelector,
    MaterializationConfig,
    MaterializationOptions,
    MergeConfig,
    OverwriteConfig,
    Scd2Columns,
    Scd2Config,
    SqlConditions,
)
from kelp.tables.materialization.decorator import MaterializedContext, materialized
from kelp.tables.materialization.orchestrator import materialize, materialize_resolved
from kelp.tables.materialization.resolve import (
    FullRefreshStrategy,
    ResolvedMaterializationInputs,
    resolve_materialization_inputs,
)
from kelp.tables.materialization.runner import REGISTRY, ModelRegistry, ModelSpec, Runner

__all__ = [
    "REGISTRY",
    "AppendConfig",
    "ColumnSelector",
    "FullRefreshStrategy",
    "MaterializationConfig",
    "MaterializationOptions",
    "MaterializedContext",
    "MergeConfig",
    "ModelRegistry",
    "ModelSpec",
    "OverwriteConfig",
    "ResolvedMaterializationInputs",
    "Runner",
    "Scd2Columns",
    "Scd2Config",
    "SqlConditions",
    "materialize",
    "materialize_resolved",
    "materialized",
    "resolve_materialization_inputs",
]
