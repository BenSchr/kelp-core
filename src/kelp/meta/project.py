"""Generic project discovery and framework settings loading for meta backends."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from kelp.meta.spec import MetaProjectSpec
from kelp.meta.utils import (
    _deep_merge_dicts,
    find_path_by_name,
    load_yaml,
    load_yaml_with_jinja,
)
from kelp.meta.variables import resolve_vars_with_target

logger = logging.getLogger(__name__)


def resolve_project_root_dir(
    project_filename: str,
    *,
    max_depth_up: int = 3,
    max_depth_down: int = 3,
) -> str:
    """Resolve project root by searching for project file around cwd.

    Args:
        project_filename: Project file name to discover.
        max_depth_up: Parent directory search depth.
        max_depth_down: Child directory search depth.

    Returns:
        Absolute path to project root directory.

    Raises:
        FileNotFoundError: If project file cannot be found.

    """
    project_path = find_path_by_name(
        Path.cwd(),
        project_filename,
        search_strategy="both",
        max_depth_up=max_depth_up,
        max_depth_down=max_depth_down,
    )
    if project_path is None:
        raise FileNotFoundError(
            f"Project root with '{project_filename}' not found in current, child and parent directories.",
        )

    return str(project_path.parent if project_path.is_file() else project_path)


def resolve_project_file_path(
    project_filename: str,
    *,
    explicit_project_file_path: str | None = None,
) -> str:
    """Resolve project file path from explicit input or directory discovery.

    Args:
        project_filename: Default file name for discovery.
        explicit_project_file_path: Optional explicit path.

    Returns:
        Absolute project file path.

    Raises:
        FileNotFoundError: If explicit file does not exist.

    """
    if explicit_project_file_path:
        explicit_path = Path(explicit_project_file_path)
        if explicit_path.exists() and explicit_path.is_file():
            return str(explicit_path.absolute())
        raise FileNotFoundError(f"Specified project file not found: {explicit_project_file_path}")

    project_root = resolve_project_root_dir(project_filename)
    return str(Path(project_root).joinpath(project_filename))


def resolve_target_file_path(
    project_file_path: str | Path,
    target: str,
    *,
    targets_key: str = "targets",
    targets_path_key: str = "targets_path",
) -> str:
    """Resolve file containing selected target configuration.

    Args:
        project_file_path: Main project file path.
        target: Target name.
        targets_key: Target section key.
        targets_path_key: Optional key pointing to separate target files.

    Returns:
        Path to YAML file that contains the selected target.

    Raises:
        ValueError: If target cannot be found.

    """
    project_file = Path(project_file_path)
    project_root = project_file.parent
    raw_project_data = load_yaml(project_file)

    if raw_project_data.get(targets_key, {}).get(target):
        return str(project_file)

    targets_path = raw_project_data.get(targets_path_key)
    if targets_path:
        candidate = project_root / targets_path
        if candidate.is_file():
            return str(candidate)
        if candidate.is_dir():
            for pattern in ("*.yml", "*.yaml"):
                for yaml_file in candidate.glob(pattern):
                    file_data = load_yaml(yaml_file)
                    if file_data.get(targets_key, {}).get(target):
                        return str(yaml_file)

    raise ValueError(f"Target '{target}' not found in project file or targets path.")


def load_framework_settings(
    spec: MetaProjectSpec,
    *,
    project_file_path: str,
    target: str | None = None,
    init_vars: dict[str, Any] | None = None,
) -> tuple[BaseModel, dict[str, Any], str | None]:
    """Load framework-specific settings and shared runtime vars.

    Args:
        spec: Framework project specification.
        project_file_path: Resolved project file path.
        target: Selected target.
        init_vars: Runtime var overrides.

    Returns:
        Tuple of (framework settings model, runtime vars, target file path).

    """
    target_file_path: str | None = None
    if target:
        target_file_path = resolve_target_file_path(
            project_file_path,
            target,
            targets_key=spec.targets_key,
        )

    runtime_vars = resolve_vars_with_target(
        project_file_path,
        target_file_path=target_file_path,
        target=target,
        init_vars=init_vars,
        vars_key=spec.vars_key,
        targets_key=spec.targets_key,
        vars_overwrite_key=spec.vars_overwrite_key,
        target_var_name=spec.target_var_name,
    )

    project_data = load_yaml_with_jinja(project_file_path, jinja_context=runtime_vars)
    framework_project_cfg = project_data.get(spec.project_header, {})

    framework_target_cfg: dict[str, Any] = {}
    if target and target_file_path == project_file_path:
        framework_target_cfg = (
            project_data.get(spec.targets_key, {}).get(target, {}).get(spec.project_header, {})
        )
    elif target and target_file_path is not None:
        framework_target_cfg = (
            load_yaml_with_jinja(target_file_path, jinja_context=runtime_vars)
            .get(spec.targets_key, {})
            .get(target, {})
            .get(spec.project_header, {})
        )

    merged_framework_cfg = _deep_merge_dicts(framework_project_cfg, framework_target_cfg)
    framework_settings = spec.project_settings_model(**merged_framework_cfg)

    logger.debug("Loaded framework settings for %s", spec.framework_id)
    return framework_settings, runtime_vars, target_file_path
