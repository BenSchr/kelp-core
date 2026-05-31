"""Materialization primitives for Kelp table writes."""

from kelp.tables.materialization.append_overwrite import AppendOverwriteMaterializer
from kelp.tables.materialization.decorator import materialized
from kelp.tables.materialization.factory import materialize
from kelp.tables.materialization.merge import MergeMaterializer

__all__ = [
    "AppendOverwriteMaterializer",
    "MergeMaterializer",
    "materialize",
    "materialized",
]
