"""Framework-specific API for metadata management.

This module provides a clean API class that each framework can instantiate
to manage their metadata catalog, context, and initialization.
"""

from __future__ import annotations

from typing import Any, ClassVar

from kelp.meta.context import MetaRuntimeContext
from kelp.meta.runtime import get_context, init_runtime
from kelp.meta.spec import MetaProjectSpec


class MetaFramework:
    """Framework-specific API for metadata management.

    This class provides a clean interface for frameworks to:
    - Define their metadata specifications
    - Initialize runtime contexts
    - Access framework-specific contexts

    Each framework should define a subclass that sets ``spec`` as a class
    attribute and then expose the class methods as their public API.

    Example:
        >>> from kelp.meta import MetaFramework, MetaProjectSpec, MetaObjectSpec
        >>> from kelp.models.model import Model
        >>> spec = MetaProjectSpec(
        ...     framework_id="myframework",
        ...     project_header="myframework_project",
        ...     project_settings_model=MyConfig,
        ...     object_specs=(
        ...         MetaObjectSpec(
        ...             root_key="my_models",
        ...             project_config_key="models",
        ...             path_attr="models_path",
        ...             catalog_attr="models",
        ...             model_class=Model,
        ...             model_label="Model",
        ...         ),
        ...     ),
        ... )
        >>> class MyFramework(MetaFramework):
        ...     spec = spec
        >>> ctx = MyFramework.init()
        >>> ctx = MyFramework.get_context()
    """

    spec: ClassVar[MetaProjectSpec]

    @classmethod
    def _get_spec(cls) -> MetaProjectSpec:
        if not hasattr(cls, "spec"):
            raise RuntimeError("MetaFramework subclass must define 'spec'.")
        return cls.spec

    @classmethod
    def init(
        cls,
        project_file_path: str | None = None,
        target: str | None = None,
        init_vars: dict[str, Any] | None = None,
        manifest_file_path: str | None = None,
        refresh: bool = False,
        store_in_global: bool = True,
    ) -> MetaRuntimeContext:
        """Initialize and store framework runtime context.

        This discovers the project configuration, loads metadata files,
        resolves variables, and stores the resulting context for later access.

        When ``manifest_file_path`` is provided, the context is loaded directly
        from a pre-built manifest JSON file, skipping all discovery,
        rendering, and variable resolution.

        Args:
            project_file_path: Path to project file or directory.
                If None, auto-discovers from current working directory.
            target: Target environment name (e.g., "dev", "prod") to load
                target-specific variables from project file.
            init_vars: Runtime variable overrides (highest priority).
            manifest_file_path: Path to a manifest JSON file. When provided,
                skips all source file loading and uses the snapshot instead.
            refresh: If True, recreate context even if one already exists.
            store_in_global: Whether to store context globally.

        Returns:
            The initialized runtime context containing project settings,
            resolved variables, and metadata catalog.

        Raises:
            FileNotFoundError: If project file cannot be discovered.
            ValueError: If configuration is invalid or manifest is incompatible.
        """
        spec = cls._get_spec()
        return init_runtime(
            spec=spec,
            project_file_path=project_file_path,
            target=target,
            init_vars=init_vars,
            manifest_file_path=manifest_file_path,
            refresh=refresh,
            store_in_global=store_in_global,
        )

    @classmethod
    def get_context(cls, init: bool = False) -> MetaRuntimeContext:
        """Get framework runtime context, optionally auto-initializing.

        If the context hasn't been initialized yet, you can either:
        - Set init=True to auto-initialize from current directory
        - Call init() explicitly with specific parameters first

        Args:
            init: If True and context doesn't exist, auto-initialize from
                current working directory with default settings.

        Returns:
            The framework's runtime context.

        Raises:
            RuntimeError: If context hasn't been initialized and init=False.
        """
        spec = cls._get_spec()
        ctx = get_context(framework_id=spec.framework_id)

        if ctx is None:
            if init:
                return cls.init()
            raise RuntimeError(
                f"Context for framework '{spec.framework_id}' has not been initialized. "
                "Call init() first or use get_context(init=True) to auto-initialize."
            )

        return ctx
