"""Top-level kelp package exports.

Expose a small ergonomic surface so callers can do:

    import kelp
    kelp.init(...)

We re-export a thin public API from :mod:`kelp.api` which delegates to
internal runtime implementations.
"""

from kelp import catalog, meta, tables, transformations
from kelp.config import get_context, init

__all__ = [
    "catalog",
    "get_context",
    "init",
    "meta",
    "tables",
    "transformations",
]
