"""Materialization primitives for Kelp table writes."""

from kelp.tables.materialization.decorator import MaterializedContext, materialized
from kelp.tables.materialization.factory import materialize

__all__ = [
    "MaterializedContext",
    "materialize",
    "materialized",
]
