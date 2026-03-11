"""Generic metadata framework specifications.

This module defines the Pydantic specs used by the reusable ``kelp.meta``
backend. Frameworks provide their own project header and settings model,
while sharing variable resolution and metadata loading behavior.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


def _noop_preprocess(item: dict[str, Any], project_root: str | None) -> dict[str, Any]:
    """Default preprocessing hook that returns the item unchanged.

    Args:
        item: Parsed metadata item dictionary.
        project_root: Project root path when available.

    Returns:
        Unchanged item dictionary.

    """
    _ = project_root
    return item


class MetaObjectSpec(BaseModel):
    """Specification for one metadata object type.

    Attributes:
        root_key: YAML root key containing item list (for example ``kelp_models``).
        project_config_key: Key in project settings containing hierarchy defaults.
        path_attr: Attribute in framework settings with metadata path(s).
        catalog_attr: Output catalog key where parsed objects are stored.
        model_class: Pydantic model class used for validation.
        model_label: Human-readable label for diagnostics.
        preprocess: Optional hook to transform raw item payload before validation.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    root_key: str = Field(description="YAML root key containing metadata items")
    project_config_key: str = Field(description="Project settings key for hierarchy defaults")
    path_attr: str = Field(description="Framework settings attribute with metadata path(s)")
    catalog_attr: str = Field(description="Catalog payload key for validated items")
    model_class: type[Any] = Field(description="Model class used to validate parsed items")
    model_label: str = Field(description="Human-readable model label for error messages")
    preprocess: Callable[[dict[str, Any], str | None], dict[str, Any]] = Field(
        default=_noop_preprocess,
        description="Optional preprocessing hook before model validation",
    )


class MetaProjectSpec(BaseModel):
    """Specification describing one framework's project format.

    Framework settings are intentionally isolated under ``project_header``
    (for example ``kelp_project`` or ``xy_project``). Shared sections like
    ``vars`` and ``targets`` remain framework-agnostic and are handled by
    the generic loading pipeline.

    Attributes:
        framework_id: Unique framework identifier for isolated context storage.
        project_header: Top-level YAML key containing framework settings.
        project_settings_model: Pydantic model class for framework settings.
        object_specs: Metadata object specs to load and validate.
        project_filename: Default project file name used for discovery.
        vars_key: Shared vars section key.
        targets_key: Shared targets section key.
        vars_overwrite_key: Optional path key for overwrite vars file.
        target_var_name: Built-in variable name used for selected target.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    framework_id: str = Field(description="Unique framework ID")
    project_header: str = Field(description="Top-level key containing framework settings")
    project_settings_model: type[BaseModel] = Field(
        description="Framework-specific Pydantic settings model",
    )
    object_specs: tuple[MetaObjectSpec, ...] = Field(
        default_factory=tuple,
        description="Object specs loaded by this framework",
    )
    project_filename: str = Field(
        default="kelp_project.yml",
        description="Default project file name used for discovery",
    )
    vars_key: str = Field(
        default="vars",
        description="Shared key containing root variables",
    )
    targets_key: str = Field(
        default="targets",
        description="Shared key containing environment targets",
    )
    vars_overwrite_key: str = Field(
        default="vars_overwrite",
        description="Shared key containing overwrite vars file path",
    )
    target_var_name: str = Field(
        default="target",
        description="Built-in variable name storing selected target",
    )
    resolve_runtime_settings: bool = Field(
        default=False,
        description=(
            "Whether init_runtime should resolve target/project_file via meta settings "
            "sources (args/spark/env/defaults)."
        ),
    )
    settings_env_prefix: str = Field(
        default="KELP_",
        description="Environment variable prefix for runtime settings resolution",
    )
    settings_spark_prefix: str = Field(
        default="kelp",
        description="Spark conf prefix for runtime settings resolution",
    )
    target_setting_key: str = Field(
        default="target",
        description="Settings key used to resolve runtime target",
    )
    project_file_setting_key: str = Field(
        default="project_file",
        description="Settings key used to resolve project file path",
    )
