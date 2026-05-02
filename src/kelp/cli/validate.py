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


def validate(
    config_path: Annotated[
        str | None,
        typer.Option("-c", help="Path to the kelp_project.yml"),
    ] = None,
    target: Annotated[str | None, typer.Option(help="Environment to validate against")] = None,
    manifest_file_path: Annotated[
        str | None,
        typer.Option(
            ...,
            "-m",
            "--manifest",
            help="Path to manifest JSON file (skips source file loading)",
        ),
    ] = None,
    debug: Annotated[bool, typer.Option(help="Debug mode")] = False,
) -> None:
    """Validate the Kelp project configuration and catalog."""

    from kelp.cli.output import print_error, print_message, print_success
    from kelp.config import init

    log_level = "DEBUG" if debug else None

    resolved_target = _resolve_target(target)

    run_ctx = init(
        config_path, resolved_target, manifest_file_path=manifest_file_path, log_level=log_level
    )
    validated = bool(run_ctx)
    if validated:
        print_success("✓ Configuration is valid!")
    else:
        print_error("✗ Configuration is invalid!")
        raise typer.Exit(code=1)

    project_config = run_ctx.project_settings

    print_message(f"Project config loaded from: {run_ctx.project_file_path}")
    print_message(f"Loaded from manifest: {run_ctx.generated_from_manifest}")
    if run_ctx.generated_from_manifest:
        print_message(f"Manifest path: {run_ctx.manifest_file_path}")
    print_message(f"Target environment: {run_ctx.target}")
    print_message(f"Runtime variables: {run_ctx.runtime_vars}")
    print_message(f"Relative models path: {project_config.models_path}")
    print_message(f"Models found: {len(run_ctx.catalog_index.get_all('models'))}")

    if project_config.functions_path:
        print_message(f"Relative functions path: {project_config.functions_path}")
        print_message(f"Functions found: {len(run_ctx.catalog_index.get_all('functions'))}")

    if project_config.abacs_path:
        print_message(f"Relative ABACs path: {project_config.abacs_path}")
        print_message(f"ABAC policies found: {len(run_ctx.catalog_index.get_all('abacs'))}")

    # Show metrics info if metrics are configured
    if project_config.metrics_path:
        print_message(f"Relative metrics path: {project_config.metrics_path}")
        print_message(f"Metric views found: {len(run_ctx.catalog_index.get_all('metric_views'))}")
    # Show sources info if sources are configured
    if project_config.sources_path:
        print_message(f"Relative sources path: {project_config.sources_path}")
        print_message(f"Sources found: {len(run_ctx.catalog_index.get_all('sources'))}")
