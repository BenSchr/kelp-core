from typing import Annotated

import typer

from kelp.config.settings import resolve_setting


def _resolve_target(target: str | None) -> str | None:
    """Resolve a target from settings when not provided.

    Args:
        target: Explicit target value, if provided.

    Returns:
        Resolved target or None if not set anywhere.

    """
    return target or resolve_setting("target", default=None)


def validate(
    config_path: Annotated[
        str | None,
        typer.Option("-c", help="Path to the kelp_project.yml"),
    ] = None,
    target: Annotated[str | None, typer.Option(help="Environment to validate against")] = None,
    debug: Annotated[bool, typer.Option(help="Debug mode")] = False,
) -> None:
    """Validate the Kelp project configuration and catalog."""

    from kelp.config.lifecycle import init

    log_level = "DEBUG" if debug else None

    resolved_target = _resolve_target(target)

    run_ctx = init(config_path, resolved_target, log_level=log_level)
    validated = bool(run_ctx)
    if validated:
        typer.secho("✓ Configuration is valid!", fg=typer.colors.GREEN)
    else:
        typer.secho("✗ Configuration is invalid!", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    typer.echo(f"Project config loaded from: {run_ctx.project_config.project_file_path}")
    typer.echo(f"Target environment: {run_ctx.target}")
    typer.echo(f"Runtime variables: {run_ctx.runtime_vars}")
    typer.echo(f"Relative models path: {run_ctx.project_config.models_path}")
    typer.echo(f"Models found: {len(run_ctx.catalog.table_index)}")

    if run_ctx.project_config.functions_path:
        typer.echo(f"Relative functions path: {run_ctx.project_config.functions_path}")
        typer.echo(f"Functions found: {len(run_ctx.catalog.function_index)}")

    if run_ctx.project_config.abacs_path:
        typer.echo(f"Relative ABACs path: {run_ctx.project_config.abacs_path}")
        typer.echo(f"ABAC policies found: {len(run_ctx.catalog.abac_index)}")

    # Show metrics info if metrics are configured
    if run_ctx.project_config.metrics_path:
        typer.echo(f"Relative metrics path: {run_ctx.project_config.metrics_path}")
        typer.echo(f"Metric views found: {len(run_ctx.catalog.metrics_index)}")
    # Show sources info if sources are configured
    if run_ctx.project_config.sources_path:
        typer.echo(f"Relative sources path: {run_ctx.project_config.sources_path}")
        typer.echo(f"Sources found: {len(run_ctx.catalog.source_index)}")
