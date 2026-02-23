from typing import Literal

from pydantic import BaseModel, Field
from pydantic.json_schema import SkipJsonSchema


class QuarantineConfig(BaseModel):
    quarantine_catalog: str | None = None
    quarantine_schema: str | None = None
    quarantine_prefix: str = ""
    quarantine_suffix: str = "_quarantine"
    validation_prefix: str = ""
    validation_suffix: str = "_validation"


class RemoteCatalogConfig(BaseModel):
    table_tag_mode: Literal["append", "replace"] = "append"
    managed_table_tags: list[str] = Field(default_factory=list)
    column_tag_mode: Literal["append", "replace"] = "append"
    managed_column_tags: list[str] = Field(default_factory=list)
    table_property_mode: Literal["append", "replace"] = "append"
    managed_table_properties: list[str] = Field(default_factory=list)


class VarsOverwriteConfig(BaseModel):
    vars: dict = Field(default_factory=dict)


class ProjectConfig(BaseModel):
    # vars_overwrite: str | None = Field(
    #     default=None,
    #     description="Path to a YAML file containing vars to overwrite the project vars. May be added to .gitignore to set individual developer vars like schema names",
    # )
    # vars: dict = Field(default_factory=dict)
    # metadata_paths: list[str] = Field(default_factory=lambda: ["kelp_models"])
    models_path: str | None = Field(default=None)
    models: dict = Field(default_factory=dict)
    quarantine_config: QuarantineConfig = Field(default_factory=QuarantineConfig)
    remote_catalog_config: RemoteCatalogConfig = Field(default_factory=RemoteCatalogConfig)
    runtime_vars: SkipJsonSchema[dict] = Field(default_factory=dict)
    project_file_path: SkipJsonSchema[str] = Field(default="")
