"""Isolated runtime context storage for reusable meta frameworks."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from kelp.meta.catalog_index import MetaCatalog


class MetaContextExistsError(Exception):
    """Raised when setting a context that already exists."""


class MetaContextMissingError(Exception):
    """Raised when requesting a missing framework context."""


class MetaRuntimeContext(BaseModel):
    """Runtime context for one framework namespace.

    Attributes:
        framework_id: Framework identifier owning this context.
        project_root: Project root directory.
        project_file_path: Resolved project file path.
        target: Selected target name, if any.
        runtime_vars: Resolved runtime variables.
        project_settings: Framework-specific settings payload.
        catalog: Loaded catalog payload for framework metadata objects.
    """

    framework_id: str = Field(description="Owning framework identifier")
    project_root: str = Field(description="Project root path")
    project_file_path: str = Field(description="Project YAML file path")
    target: str | None = Field(default=None, description="Selected target")
    runtime_vars: dict[str, Any] = Field(default_factory=dict, description="Resolved variables")
    project_settings: Any = Field(
        default=None,
        description="Framework-specific settings Pydantic model",
    )
    catalog: dict[str, list[Any]] = Field(
        default_factory=dict,
        description="Loaded metadata catalog payload",
    )
    generated_from_manifest: bool = Field(
        default=False,
        description="Whether this context was loaded from a manifest file",
    )
    manifest_file_path: str | None = Field(
        default=None,
        description="Path to the manifest file if loaded from one",
    )

    # Private memoized MetaCatalog wrapper (not serialized)
    _catalog_index_cache: MetaCatalog | None = None

    model_config = {"arbitrary_types_allowed": True}

    @property
    def catalog_index(self) -> MetaCatalog:
        """Get indexed catalog for efficient name-based lookups.

        The MetaCatalog instance is memoized so lazy-built name indices
        and filter-result caches persist across repeated accesses.

        Returns:
            MetaCatalog with lazy-built indices for each object type.
        """
        if self._catalog_index_cache is None:
            object.__setattr__(self, "_catalog_index_cache", MetaCatalog(self.catalog))
        return self._catalog_index_cache  # ty:ignore[invalid-return-type]


class MetaContextStore:
    """Thread-safe context store with framework-isolated namespaces."""

    _lock: ClassVar[threading.Lock] = threading.Lock()
    _contexts: ClassVar[dict[str, MetaRuntimeContext]] = {}

    @classmethod
    def get(cls, framework_id: str) -> MetaRuntimeContext | None:
        """Return context for a framework if available.

        Args:
            framework_id: Framework namespace key.

        Returns:
            Existing runtime context or None.

        """
        with cls._lock:
            return cls._contexts.get(framework_id)

    @classmethod
    def set(
        cls,
        framework_id: str,
        ctx: MetaRuntimeContext,
        *,
        overwrite: bool = False,
    ) -> None:
        """Store context for a framework.

        Args:
            framework_id: Framework namespace key.
            ctx: Context object to store.
            overwrite: Whether to overwrite existing context.

        Raises:
            MetaContextExistsError: If context already exists and overwrite=False.

        """
        with cls._lock:
            if framework_id in cls._contexts and not overwrite:
                raise MetaContextExistsError(
                    f"Context for framework '{framework_id}' already exists.",
                )
            cls._contexts[framework_id] = ctx

    @classmethod
    def clear(cls, framework_id: str) -> None:
        """Clear context for a framework.

        Args:
            framework_id: Framework namespace key.

        """
        with cls._lock:
            cls._contexts.pop(framework_id, None)

    @classmethod
    def clear_all(cls) -> None:
        """Clear all framework contexts."""
        with cls._lock:
            cls._contexts = {}

    @classmethod
    def get_or_create(
        cls,
        framework_id: str,
        factory: Callable[[], MetaRuntimeContext],
        *,
        refresh: bool = False,
        store_in_global: bool = True,
    ) -> MetaRuntimeContext:
        """Return existing context or create one for a framework.

        Args:
            framework_id: Framework namespace key.
            factory: No-arg factory that builds a new context.
            refresh: If True, always rebuild context.
            store_in_global: If True, save the created context.

        Returns:
            Existing or newly created framework context.

        """
        with cls._lock:
            if not refresh and framework_id in cls._contexts:
                return cls._contexts[framework_id]

        ctx = factory()

        with cls._lock:
            if store_in_global:
                if not refresh and framework_id in cls._contexts:
                    return cls._contexts[framework_id]
                cls._contexts[framework_id] = ctx

        return ctx
