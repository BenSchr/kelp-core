"""Reusable metadata backend public API for framework integration."""

from kelp.meta.catalog_index import MetaCatalog
from kelp.meta.context import MetaRuntimeContext
from kelp.meta.framework import MetaFramework
from kelp.meta.runtime import build_runtime_context, get_context, init_runtime
from kelp.meta.spec import MetaObjectSpec, MetaProjectSpec

__all__ = [
    "MetaCatalog",
    "MetaFramework",
    "MetaObjectSpec",
    "MetaProjectSpec",
    "MetaRuntimeContext",
    "build_runtime_context",
    "get_context",
    "init_runtime",
]
