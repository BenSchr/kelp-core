from pathlib import Path
from kelp.config.catalog import parse_catalog
from kelp.config.project import load_project_config
from kelp.config.vars import resolve_vars_with_target
from kelp.models.runtime_context import RuntimeContext
from kelp.utils.jinja_parser import load_yaml_with_jinja, _deep_merge_dicts
from kelp.config.settings import create_settings_resolver
import logging


logger = logging.getLogger(__name__)


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
    project_file_path: str | None = None,
    target: str | None = None,
    init_vars: dict | None = None,
) -> RuntimeContext:
    """
    Load runtime configuration using the new modular approach with target support.

    Features:
    - Uses simple variable priority: init_vars > target_vars > default_vars
    - Supports target-specific configurations
    - Settings resolver for project settings (separate concern)
    - Auto-detects Spark if available for settings resolution

    Priority for project file resolution: spark.conf > os env > folder search

    Args:
        project_file_path: Explicit project file path. If not provided, resolved via spark/os env/folder search.
        target: Target name to use (e.g., 'dev', 'prod'). Optional.
        init_vars: Variables to override (highest priority).

    Returns:
        RuntimeContext with resolved configuration.
    """

    # Load project configuration with resolved variables
    project_config = load_project_config(project_file_path, target, init_vars)

    project_root = Path(project_config.project_file_path).parent
    runtime_vars = project_config.runtime_vars

    # Load metadata files with resolved variables
    raw_config = load_config_files(project_root, project_config.metadata_paths, runtime_vars)

    # Parse catalog
    catalog = parse_catalog(
        raw_config.get("kelp_models", []),
        project_config.models,
    )

    return RuntimeContext(
        project_root=str(project_root),
        catalog=catalog,
        project_config=project_config,
        target=target,  # Store target as target for backward compatibility
        runtime_vars=runtime_vars,
    )
