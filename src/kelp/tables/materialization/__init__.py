"""Materialization primitives for Kelp table writes."""

from kelp.tables.materialization.decorator import MaterializedContext, materialized
from kelp.tables.materialization.factory import materialize
from kelp.tables.materialization.runner import Runner

__all__ = ["MaterializedContext", "Runner", "materialize", "materialized"]
