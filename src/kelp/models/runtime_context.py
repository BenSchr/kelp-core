from pydantic import BaseModel, Field

from kelp.models.catalog import Catalog
from kelp.models.project_config import ProjectConfig


class RuntimeContext(BaseModel):
    """Runtime context containing all configuration and state for a kelp execution.

    Represents the complete configuration state loaded for a single kelp project
    execution, including the project configuration, catalog of tables and metric views,
    and any runtime variables.

    Attributes:
        project_root: Root directory path of the kelp project.
        catalog: Catalog of tables and metric views.
        project_config: Project-level configuration.
        target: Target environment name (e.g., 'dev', 'prod'), if any.
        runtime_vars: Runtime variables available during execution.
    """

    project_root: str = Field(
        description="Root directory path of the kelp project",
    )
    catalog: Catalog = Field(
        description="Catalog of tables and metric views",
    )
    project_config: ProjectConfig = Field(
        description="Project-level configuration",
    )
    target: str | None = Field(
        default=None,
        description="Target environment name (e.g., 'dev', 'prod')",
    )
    runtime_vars: dict = Field(
        default_factory=dict,
        description="Runtime variables available during execution",
    )
