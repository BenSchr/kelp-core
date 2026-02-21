from pathlib import Path
from kelp.utils.jinja_parser import load_yaml_with_jinja, render_obj_with_jinja
from kelp.utils.yaml_parser import load_yaml
import logging

logger = logging.getLogger(__name__)


def load_and_render_target_vars(
    target_file_path: str | Path, target: str, render_vars: dict = {}
) -> dict:
    target_file_path = Path(target_file_path)
    return (
        load_yaml_with_jinja(
            target_file_path,
            jinja_context=render_vars,
        )
        .get("targets", {})
        .get(target, {})
        .get("vars", {})
    )


def resolve_vars_with_target(
    project_file_path: str | Path,
    target_file_path: str | Path | None = None,
    target: str | None = None,
    init_vars: dict | None = None,
) -> dict:
    """
    Resolve variables using simple priority merging with target support.

    Priority order (highest to lowest): init_vars > target vars > overwrite vars > default vars

    Targets can be defined in the project file or loaded from targets_path.
    Jinja variables are rendered progressively so each layer can reference previous layers.

    Args:
        project_file_path: Path to the project file.
        target: Target name to use from the targets section.
        init_vars: Variables to override (highest priority).

    Returns:
        Resolved variables dictionary suitable for rendering.
    """
    project_file_path = Path(project_file_path)
    root_dir = project_file_path.parent
    raw_project_data = load_yaml(project_file_path)

    # Get default variables from 'vars' section
    default_vars = raw_project_data.get("vars", {})

    # Render default vars with itself as context
    resolved_vars = render_obj_with_jinja(default_vars, jinja_context=default_vars)

    # Get target-specific variables if target is specified
    target_vars = {}
    if target and target_file_path is not None:
        target_vars = load_and_render_target_vars(
            target_file_path, target, render_vars=resolved_vars
        )
        logger.debug("Loaded target vars for target '%s': %s", target, target_vars)
    elif target and target_file_path is None:
        logger.debug(
            "Target '%s' specified but no target_file_path provided; skipping target vars.",
            target,
        )
    # Load overwrite file if specified
    overwrite_file_vars = {}
    if raw_project_data.get("vars_overwrite"):
        overwrite_vars_path = Path(root_dir).joinpath(raw_project_data["vars_overwrite"])
        try:
            overwrite_data = load_yaml(overwrite_vars_path)
            file_vars = overwrite_data.get("vars", {})
            overwrite_file_vars = render_obj_with_jinja(file_vars, jinja_context={**resolved_vars})
        except FileNotFoundError:
            logger.debug(f"Overwrite vars file not found: {overwrite_vars_path}")

    runtime_vars = {**resolved_vars, **target_vars, **overwrite_file_vars, **(init_vars or {})}

    logger.debug("Resolved vars: %s", resolved_vars)

    return runtime_vars
