"""Top-level kelp package exports.

Expose a small ergonomic surface so callers can do:

    import kelp
    kelp.init(...)

We re-export a thin public API from :mod:`kelp.api` which delegates to
internal runtime implementations.
"""

from kelp.config import init, get_context
from kelp import catalog

__all__ = ["init", "get_context", "catalog"]
