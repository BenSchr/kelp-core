from typing import Any
from kelp.constants import KELP_PROJECT_HEADER
from kelp.models.project_config import ProjectConfig
from kelp.utils.jinja_parser import load_yaml_with_jinja


def load_project_yaml(
    project_file_path: str,
    runtime_vars: dict[str, Any],
) -> dict:
    """Load the project YAML file as a dict."""

    project_data = load_yaml_with_jinja(project_file_path, jinja_context=runtime_vars)[
        KELP_PROJECT_HEADER
    ]

    return project_data


def load_project(project_file_path: str, runtime_vars: dict[str, Any] = {}) -> ProjectConfig:
    project_data = load_project_yaml(project_file_path, runtime_vars=runtime_vars)
    # project_data["project_root"] = project_root
    return ProjectConfig(**project_data)
