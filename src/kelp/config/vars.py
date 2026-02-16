from pathlib import Path
from kelp.utils.jinja_parser import render_obj_with_jinja
from kelp.utils.yaml_parser import load_yaml
import logging

logger = logging.getLogger(__name__)


def resolve_variables(
    project_file_path: str | Path, env: str | None = None, overwrite_vars: dict = {}
) -> tuple[dict, dict]:
    """Resolve variables for the project by loading project-level vars and applying any overwrites."""
    project_file_path = Path(project_file_path)
    root_dir = project_file_path.parent
    raw_project_data = load_yaml(project_file_path)
    project_vars = raw_project_data.get("vars") or {}
    overwrite_file_vars = {}
    if raw_project_data.get("vars_overwrite"):
        overwrite_vars_path = Path(root_dir).joinpath(raw_project_data["vars_overwrite"])
        try:
            overwrite_file_vars = load_yaml(overwrite_vars_path).get("vars") or {}
        # Ignore if the overwrite file doesn't exist, just treat it as empty vars since this may not checked into version control
        except FileNotFoundError:
            pass

    overwrite_vars = overwrite_vars or {}
    runtime_vars = {}
    if project_vars:
        project_vars = render_obj_with_jinja(project_vars, jinja_context=project_vars)

        runtime_vars.update(project_vars.get("default") or {})

        if env and env in project_vars:
            runtime_vars.update(project_vars.get(env) or {})

    runtime_vars.update(overwrite_file_vars)
    runtime_vars.update(overwrite_vars)
    project_vars.update(runtime_vars)

    logger.debug(f"Resolved project vars: {project_vars}")
    logger.debug(f"Resolved runtime vars: {runtime_vars}")

    # Also passing the full project_vars to render the project file again without unresolved errors
    # Maybe change this later
    return runtime_vars, project_vars
