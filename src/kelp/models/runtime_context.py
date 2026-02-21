from kelp.models.catalog import Catalog
from kelp.models.project_config import ProjectConfig
from pydantic import BaseModel


class RuntimeContext(BaseModel):
    project_root: str
    catalog: Catalog
    project_config: ProjectConfig
    target: str | None = None
    runtime_vars: dict | None = None
