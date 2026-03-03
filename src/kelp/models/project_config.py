from typing import Literal

from pydantic import BaseModel, Field
from pydantic.json_schema import SkipJsonSchema


class QuarantineConfig(BaseModel):
    """Configuration for table quarantine and validation.

    Attributes:
        quarantine_catalog: Catalog name for quarantined data (defaults to project catalog).
        quarantine_schema: Schema name for quarantined data (defaults to project schema).
        quarantine_prefix: Prefix for quarantine table names.
        quarantine_suffix: Suffix for quarantine table names.
        validation_prefix: Prefix for validation table names.
        validation_suffix: Suffix for validation table names.
    """

    quarantine_catalog: str | None = Field(
        default=None,
        description="Catalog for quarantined data",
    )
    quarantine_schema: str | None = Field(
        default=None,
        description="Schema for quarantined data",
    )
    quarantine_prefix: str = Field(
        default="",
        description="Prefix for quarantine table names",
    )
    quarantine_suffix: str = Field(
        default="_quarantine",
        description="Suffix for quarantine table names",
    )
    validation_prefix: str = Field(
        default="",
        description="Prefix for validation table names",
    )
    validation_suffix: str = Field(
        default="_validation",
        description="Suffix for validation table names",
    )


class RemoteCatalogConfig(BaseModel):
    """Configuration for remote Databricks catalog synchronization.

    Attributes:
        table_tag_mode: How to apply table tags ("append", "replace", "managed").
        managed_table_tags: List of tag keys to manage on tables.
        column_tag_mode: How to apply column tags ("append", "replace", "managed").
        managed_column_tags: List of tag keys to manage on columns.
        table_property_mode: How to apply table properties ("append" or "managed").
        managed_table_properties: List of property keys to manage on tables.
    """

    table_tag_mode: Literal["append", "replace", "managed"] = Field(
        default="replace",
        description="How to apply table tags to remote catalog",
    )
    managed_table_tags: list[str] = Field(
        default_factory=list,
        description="List of tag keys to manage on tables",
    )
    column_tag_mode: Literal["append", "replace", "managed"] = Field(
        default="replace",
        description="How to apply column tags to remote catalog",
    )
    managed_column_tags: list[str] = Field(
        default_factory=list,
        description="List of tag keys to manage on columns",
    )
    table_property_mode: Literal["append", "managed"] = Field(
        default="append",
        description="How to apply table properties to remote catalog",
    )
    managed_table_properties: list[str] = Field(
        default_factory=list,
        description="List of property keys to manage on tables",
    )


class VarsOverwriteConfig(BaseModel):
    """Configuration for variable overrides.

    Attributes:
        vars: Dictionary of variables to override.
    """

    vars: dict = Field(
        default_factory=dict,
        description="Variables to override in the project",
    )


class ProjectConfig(BaseModel):
    """Project-level configuration for a kelp project.

    Attributes:
        models_path: Path to table/model definitions.
        models: Configuration hierarchy for models.
        metrics_path: Path to metric view definitions.
        metric_views: Configuration hierarchy for metric views.
        functions_path: Path to function definitions.
        functions: Configuration hierarchy for functions.
        abacs_path: Path to ABAC policy definitions.
        abacs: Configuration hierarchy for ABAC policies.
        quarantine_config: Configuration for table quarantine and validation.
        remote_catalog_config: Configuration for remote catalog synchronization.
        runtime_vars: Runtime variables (internal use).
        project_file_path: Path to the project configuration file (internal use).
    """

    models_path: str | None = Field(
        default=None,
        description="Path to table/model definitions",
    )
    models: dict = Field(
        default_factory=dict,
        description="Configuration hierarchy for models",
    )
    metrics_path: str | None = Field(
        default=None,
        description="Path to metric view definitions",
    )
    metric_views: dict = Field(
        default_factory=dict,
        description="Configuration hierarchy for metric views",
    )
    functions_path: str | None = Field(
        default=None,
        description="Path to function definitions",
    )
    functions: dict = Field(
        default_factory=dict,
        description="Configuration hierarchy for functions",
    )
    abacs_path: str | None = Field(
        default=None,
        description="Path to ABAC policy definitions",
    )
    abacs: dict = Field(
        default_factory=dict,
        description="Configuration hierarchy for ABAC policies",
    )
    quarantine_config: QuarantineConfig = Field(
        default_factory=QuarantineConfig,
        description="Configuration for table quarantine and validation",
    )
    remote_catalog_config: RemoteCatalogConfig = Field(
        default_factory=RemoteCatalogConfig,
        description="Configuration for remote catalog synchronization",
    )
    runtime_vars: SkipJsonSchema[dict] = Field(
        default_factory=dict,
        description="Runtime variables",
    )
    project_file_path: SkipJsonSchema[str] = Field(
        default="",
        description="Path to the project configuration file",
    )
