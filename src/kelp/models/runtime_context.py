from pydantic import BaseModel, Field

from kelp.models.catalog import Catalog
from kelp.models.project_config import ProjectConfig


class RuntimeContext(BaseModel):
    project_root: str
    catalog: Catalog
    project_config: ProjectConfig
    target: str | None = None
    runtime_vars: dict = Field(default_factory=dict)
