from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from kelp.models.project_config import ProjectConfig
from kelp.models.table import Table


class JsonSchemaModel(BaseModel):
    """Base model for json schema generation."""

    model_config = ConfigDict(title="Kelp", extra="forbid")

    kelp_project: ProjectConfig | None = Field(
        default=None,
        description="Project-level configuration for the Kelp project.",
    )
    kelp_models: list[Table] | None = Field(
        default=None,
        description="List of table models defined in the configuration.",
    )
    vars: dict[str, Any] | None = Field(
        default=None,
        description="Dictionary of variables that can be used in the configuration.",
    )
    vars_overwrite: str | None = Field(
        default=None,
        description="Path to a YAML file containing variables that overwrite the ones defined in the project file. "
        "May be added to .gitignore to set individual developer vars like schema names.",
    )
    targets: dict[str, Any] | None = Field(
        default=None,
        description="List of target configurations for deployment or other purposes.",
    )
    targets_path: str | None = Field(
        default=None,
        description="Path to an external YAML file or folder containing target configurations. This allows separating target definitions from the main project file.",
    )


def generate_json_schema() -> dict:
    """Generate JSON schema for the Kelp project configuration."""
    return JsonSchemaModel.model_json_schema()
