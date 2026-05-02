from typing import Annotated

import typer


def _resolve_target(target: str | None) -> str | None:
    """Resolve a target from settings when not provided.

    Args:
        target: Explicit target value, if provided.

    Returns:
        Resolved target or None if not set anywhere.
    """
    from kelp.config.settings import resolve_setting

    return target or resolve_setting("target", default=None)


def manifest(
    project_file_path: Annotated[
        str | None,
        typer.Option(
            ...,
            "-c",
            "--config",
            help="Path to kelp_project.yml (optional, will auto-detect if not provided)",
        ),
    ] = None,
    target: Annotated[str | None, typer.Option(help="Target environment")] = None,
    output: Annotated[
        str,
        typer.Option(..., "-o", "--output", help="Output path for the manifest JSON file"),
    ] = "manifest.json",
    debug: Annotated[bool, typer.Option(help="Debug mode")] = False,
) -> None:
    """Export a kelp manifest JSON file from the current project.

    The manifest is a pre-rendered snapshot of the fully resolved runtime
    context. It can be passed back to kelp init via the KELP_MANIFEST_FILE
    environment variable or the manifest_file_path parameter to skip re-rendering
    all metadata files and variables.
    """
    from kelp.cli.output import print_message, print_success
    from kelp.config import export_manifest
    from kelp.config.config import KelpFramework
    from kelp.utils.logging import configure_logging

    log_level = "DEBUG" if debug else None
    if log_level:
        configure_logging(log_level)

    resolved_target = _resolve_target(target)

    # Always build from source when exporting (ignore KELP_MANIFEST_FILE env var)
    ctx = KelpFramework.init(
        project_file_path=project_file_path,
        target=resolved_target,
        refresh=True,
    )

    output_path = export_manifest(output, ctx)

    print_success(f"✓ Manifest exported to: {output_path}")
    print_message(f"  Target: {ctx.target or '(none)'}")
    print_message(f"  Models: {len(ctx.catalog_index.get_all('models'))}")
    print_message("")
    print_message("Use it with:")
    print_message(f"  export KELP_MANIFEST_FILE={output_path}")
