"""Project configuration discovery, loading, and parsing.

This module handles:
- Project file discovery (auto-detection from current directory or explicit specification)
- Project YAML loading with Jinja template rendering
- Target-specific configuration resolution
- Project configuration model instantiation

Project discovery priority: Spark config > OS environment > Directory search
Target configuration can be inline (in project file) or in separate target files.
"""

import logging
from pathlib import Path
from typing import Any

from kelp.config.settings import create_settings_resolver
from kelp.config.vars import resolve_vars_with_target
from kelp.constants import KELP_PROJECT_FILENAME, KELP_PROJECT_HEADER
from kelp.models.project_config import ProjectConfig
from kelp.utils.common import find_path_by_name
from kelp.utils.jinja_parser import _deep_merge_dicts, load_yaml_with_jinja
from kelp.utils.yaml_parser import load_yaml

logger = logging.getLogger(__name__)


def resolve_project_root_dir() -> str:
    """Search for project root directory containing the project file.

    Searches for KELP_PROJECT_FILENAME in parent directories (up to 3 levels)
    and child directories (up to 3 levels) from the current working directory.

    Returns:
        Absolute path to the directory containing the project file.

    Raises:
        FileNotFoundError: If project file is not found in the search path.
    """
    project_filename = KELP_PROJECT_FILENAME
    project_path = find_path_by_name(
        Path.cwd(),
        project_filename,
        search_strategy="both",
        max_depth_up=3,
        max_depth_down=3,
    )

    if project_path is None:
        raise FileNotFoundError(
            f"Project root with '{project_filename}' not found in current, child and parent directories."
        )

    return str(project_path.parent if project_path.is_file() else project_path)


def load_project_yaml(
    project_file_path: str,
    runtime_vars: dict[str, Any],
) -> dict:
    """Load the project YAML file as a dict."""
    project_data = load_yaml_with_jinja(project_file_path, jinja_context=runtime_vars)
    return project_data


def resolve_project_file_path() -> str:
    """
    Resolve project file path using priority: spark > os > folder search.

    Returns:
        Resolved absolute path to kelp_project.yml file.
    """

    # Create resolver for project file discovery
    resolver = create_settings_resolver()
    # Try to resolve from spark/os
    resolved_file = resolver.resolve("project_file", default=None)

    if resolved_file:
        file_path = Path(resolved_file)
        if file_path.exists() and file_path.is_file():
            return str(file_path.absolute())
        raise FileNotFoundError(f"Specified project file not found: {resolved_file}")

    # Fallback to folder search
    project_root = resolve_project_root_dir()
    return str(Path(project_root).joinpath(KELP_PROJECT_FILENAME))


def load_target_yaml(
    target_file_path: str | Path,
    project_header: str,
    target: str | None = None,
    runtime_vars: dict[str, Any] | None = None,
) -> dict:
    """Load the target YAML file as a dict.

    Args:
        target_file_path: Path to the target file.
        project_header: Header key to extract from target config.
        target: Target name to load.
        runtime_vars: Variables for Jinja rendering. If None, empty dict is used.

    Returns:
        Loaded and filtered target configuration.
    """
    runtime_vars = runtime_vars or {}
    if target_file_path is None or target is None:
        return {}

    target_data = (
        load_yaml_with_jinja(target_file_path, jinja_context=runtime_vars)
        .get("targets", {})
        .get(target, {})
        .get(project_header, {})
    )
    return target_data


def resolve_target_file_path(project_file_path: str, target: str) -> str:
    project_root = Path(project_file_path).parent
    raw_project_data = load_yaml(project_file_path)
    if raw_project_data.get("targets", {}).get(target):
        return project_file_path
    elif raw_project_data.get("targets_path"):
        targets_path = Path(project_root).joinpath(raw_project_data["targets_path"])
        try:
            if targets_path.is_file():
                return str(targets_path)
            elif targets_path.is_dir():
                for yaml_file in targets_path.glob("*.yml"):
                    file_data = load_yaml(yaml_file)
                    if file_data.get("targets", {}).get(target):
                        return str(yaml_file)
                for yaml_file in targets_path.glob("*.yaml"):
                    file_data = load_yaml(yaml_file)
                    if file_data.get("targets", {}).get(target):
                        return str(yaml_file)
        except Exception as e:
            logger.debug(f"Failed to resolve target file path from {targets_path}: {e}")
    raise ValueError(f"Target '{target}' not found in project file or targets_path.")


def load_project_config_data(
    project_file_path: str,
    target: str | None = None,
    init_vars: dict | None = None,
    project_header: str = KELP_PROJECT_HEADER,
) -> dict:
    if not project_file_path:
        project_file_path = resolve_project_file_path()
    if not target:
        # Try to resolve target from settings (spark/os env)
        target = create_settings_resolver().resolve("target", default=None)

    target_file_path = None
    if target:
        target_file_path = resolve_target_file_path(project_file_path, target)
    logger.debug("Resolved target file path: %s", target_file_path)

    # Resolve variables using simple priority: init_vars > target_vars > default_vars
    runtime_vars = resolve_vars_with_target(
        project_file_path,
        target_file_path,
        target=target,
        init_vars=init_vars,
    )
    project_data = load_project_yaml(project_file_path, runtime_vars=runtime_vars)
    project_config = project_data.get(project_header, {})
    target_config = {}
    if project_file_path == target_file_path:
        target_config = project_data.get("targets", {}).get(target, {}).get(project_header, {})
    else:
        target_config = load_target_yaml(target_file_path, project_header, target, runtime_vars)

    project_data = _deep_merge_dicts(project_config, target_config)
    project_data["project_file_path"] = project_file_path
    project_data["runtime_vars"] = runtime_vars

    return project_data


def load_project_config(
    project_file_path: str,
    target: str | None = None,
    init_vars: dict[str, Any] = {},
) -> ProjectConfig:

    # Deep merge target data into project data with target taking precedence
    project_data = load_project_config_data(
        project_file_path,
        target=target,
        init_vars=init_vars,
    )

    return ProjectConfig(**project_data)
