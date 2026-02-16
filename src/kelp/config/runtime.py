from pathlib import Path
from kelp.config.catalog import parse_catalog
from kelp.config.project import load_project
from kelp.config.vars import resolve_variables
from kelp.constants import KELP_PROJECT_FILENAME
from kelp.models.runtime_context import RuntimeContext
from kelp.utils.jinja_parser import load_yaml_with_jinja, _deep_merge_dicts


def resolve_project_root() -> str:
    """Resolve the project root path."""

    project_filename = KELP_PROJECT_FILENAME
    current_path = Path.cwd()
    # Check in current and two parent directories
    for _ in range(3):
        candidate = current_path / project_filename
        if candidate.exists() and candidate.is_file():
            return str(current_path)
        current_path = current_path.parent
    # Check in child directories of current path, two levels deep
    for child in Path.cwd().iterdir():
        if child.is_dir():
            candidate = child / project_filename
            if candidate.exists() and candidate.is_file():
                return str(child)
            # Check one more level deep
            for grandchild in child.iterdir():
                if grandchild.is_dir():
                    candidate = grandchild / project_filename
                    if candidate.exists() and candidate.is_file():
                        return str(grandchild)

    raise FileNotFoundError(
        f"Project root with '{project_filename}' not found in current, child and parent directories."
    )


def load_config_files(project_root: str, file_paths: list[str], vars: dict) -> dict:
    # Load and merge multiple YAML config files with jinja into a single dict.
    merged_config = {}
    for file_path in file_paths:
        full_path = Path(project_root).joinpath(file_path)
        if not full_path.exists():
            raise FileNotFoundError(f"Config file not found: {full_path}")
        config_data = load_yaml_with_jinja(full_path, jinja_context=vars)
        merged_config = _deep_merge_dicts(merged_config, config_data)
    return merged_config


def load_runtime_config(
    project_file_path: str | None = None, env: str | None = None, overwrite_vars: dict = {}
) -> RuntimeContext:
    project_root = None
    if not project_file_path:
        project_root = resolve_project_root()
        project_file_path = Path(project_root).joinpath(KELP_PROJECT_FILENAME)
    if not project_root:
        project_root = Path(project_file_path).parent
    runtime_vars, full_vars = resolve_variables(project_file_path, env, overwrite_vars)

    project_config = load_project(project_file_path, full_vars)

    raw_config = load_config_files(project_root, project_config.metadata_paths, runtime_vars)

    catalog = parse_catalog(
        raw_config.get("kelp_models", []),
        project_config.models,
    )

    return RuntimeContext(
        project_root=str(project_root),
        catalog=catalog,
        project_config=project_config,
        env=env,
        runtime_vars=runtime_vars,
    )
