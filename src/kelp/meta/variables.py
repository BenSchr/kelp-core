"""Shared variable resolution for meta frameworks.

Variable and target resolution is framework-agnostic and intentionally shared
across frameworks. Framework-specific settings are loaded separately via each
framework's ``project_header``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from kelp.meta.utils import (
    load_yaml,
    load_yaml_with_jinja,
    render_obj_with_jinja,
)

logger = logging.getLogger(__name__)


def load_and_render_target_vars(
    target_file_path: str | Path,
    target: str,
    *,
    render_vars: dict[str, Any] | None = None,
    targets_key: str = "targets",
    vars_key: str = "vars",
) -> dict[str, Any]:
    """Load and render target-specific variables from a target YAML file.

    Args:
        target_file_path: Path to the target YAML file.
        target: Target name to resolve.
        render_vars: Runtime vars for Jinja rendering.
        targets_key: Top-level targets section key.
        vars_key: Nested vars section key within the target.

    Returns:
        Target-scoped variable dictionary.

    """
    jinja_context = render_vars or {}
    return (
        load_yaml_with_jinja(Path(target_file_path), jinja_context=jinja_context)
        .get(targets_key, {})
        .get(target, {})
        .get(vars_key, {})
    )


def resolve_vars_with_target(
    project_file_path: str | Path,
    *,
    target_file_path: str | Path | None = None,
    target: str | None = None,
    init_vars: dict[str, Any] | None = None,
    vars_key: str = "vars",
    targets_key: str = "targets",
    vars_overwrite_key: str = "vars_overwrite",
    target_var_name: str = "target",
) -> dict[str, Any]:
    """Resolve runtime variables with built-in target injection.

    Priority order (high to low):
        init_vars > overwrite_file_vars > target_vars > default_vars > builtins

    The selected target is injected as a built-in variable before rendering
    default vars, allowing root-level vars to use ``${ target }``.

    Args:
        project_file_path: Path to the main project YAML file.
        target_file_path: Optional path to target definitions YAML.
        target: Selected target (for example ``dev`` or ``prod``).
        init_vars: Highest-priority override variables.
        vars_key: Root vars key.
        targets_key: Root targets key.
        vars_overwrite_key: Key pointing to optional overwrite vars file.
        target_var_name: Built-in variable name for selected target.

    Returns:
        Resolved runtime variable mapping.

    """
    project_file = Path(project_file_path)
    project_root = project_file.parent
    raw_project_data = load_yaml(project_file)
    user_init_vars = init_vars or {}

    builtins: dict[str, Any] = {}
    if target is not None:
        builtins[target_var_name] = target

    default_vars = raw_project_data.get(vars_key, {})
    resolved_default_vars = render_obj_with_jinja(
        default_vars,
        jinja_context={**default_vars, **builtins},
    )

    target_vars: dict[str, Any] = {}
    if target and target_file_path is not None:
        target_vars = load_and_render_target_vars(
            target_file_path,
            target,
            render_vars={**resolved_default_vars, **builtins},
            targets_key=targets_key,
            vars_key=vars_key,
        )
        logger.debug("Loaded target vars for target '%s': %s", target, target_vars)
    elif target and target_file_path is None:
        logger.debug(
            "Target '%s' provided without target_file_path; skipping target vars.",
            target,
        )

    overwrite_file_vars: dict[str, Any] = {}
    overwrite_path = raw_project_data.get(vars_overwrite_key)
    if overwrite_path:
        vars_overwrite_file = project_root / overwrite_path
        try:
            overwrite_data = load_yaml(vars_overwrite_file)
            raw_overwrite_vars = overwrite_data.get(vars_key, {})
            overwrite_file_vars = render_obj_with_jinja(
                raw_overwrite_vars,
                jinja_context={**resolved_default_vars, **target_vars, **builtins},
            )
        except FileNotFoundError:
            logger.debug("Overwrite vars file not found: %s", vars_overwrite_file)

    runtime_vars = {
        **builtins,
        **resolved_default_vars,
        **target_vars,
        **overwrite_file_vars,
        **user_init_vars,
    }

    if target is not None:
        runtime_vars[target_var_name] = target

    logger.debug("Resolved runtime vars: %s", runtime_vars)
    return runtime_vars
