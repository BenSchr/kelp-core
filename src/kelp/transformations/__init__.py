"""DataFrame transformations backed by Kelp metadata.

Currently provides :func:`apply_schema` for schema enforcement and
:func:`apply_func` for applying Unity Catalog functions.
"""

from kelp.transformations.functions import apply_func
from kelp.transformations.schema import apply_schema

__all__ = [
    "apply_func",
    "apply_schema",
]
