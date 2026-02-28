"""DataFrame transformations backed by Kelp metadata.

Currently provides :func:`apply_schema` for schema enforcement.
"""

from kelp.transformations.schema import apply_schema

__all__ = [
    "apply_schema",
]
