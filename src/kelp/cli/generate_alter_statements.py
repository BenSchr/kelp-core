from pathlib import Path
from typing import Annotated

from typer import Option

from kelp.cli.common_params import (
    dbx_profile_option,
    debug_option,
    dry_run_option,
    kelp_project_path_option,
    manifest_file_path_option,
    target_option,
)


def _resolve_target(target: str | None) -> str | None:
    """Resolve a target from settings when not provided.

    Args:
        target: Explicit target value, if provided.

    Returns:
        Resolved target or None if not set anywhere.

    """
    from kelp.config.settings import resolve_setting

    return target or resolve_setting("target", default=None)


def generate_alter_statements(
    project_file_path: kelp_project_path_option = None,
    target: target_option = None,
    manifest_file_path: manifest_file_path_option = None,
    profile: dbx_profile_option = None,
    dry_run: dry_run_option = False,
    debug: debug_option = False,
    output_file: Annotated[
        str | None, Option("--output", "-o", help="Path to output file for ALTER TABLE")
    ] = None,
    silent: Annotated[
        bool, Option("--silent", help="Only output ALTER TABLE statements, suppressing other logs")
    ] = False,
):
    """Generate ALTER TABLE statements for tables and metric views in the catalog that are different from the Databricks catalog."""
    from kelp.catalog.api import sync_catalog
    from kelp.cli.output import print_message, print_warning
    from kelp.config import init

    log_level = "DEBUG" if debug else None
    resolved_target = _resolve_target(target)
    init(
        project_file_path=project_file_path,
        target=resolved_target,
        manifest_file_path=manifest_file_path,
        log_level=log_level,
    )
    queries = sync_catalog(sync_functions=True, profile=profile)
    if not silent:
        for q in queries:
            print_message(q + ";")
    if output_file:
        if dry_run:
            print_warning(f"• dry-run: skipped writing {output_file}")
            return
        with Path(output_file).open("w") as f:
            for q in queries:
                f.write(q + ";\n")
        print_message(f"\nALTER TABLE statements written to {output_file}")
