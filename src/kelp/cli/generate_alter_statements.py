from pathlib import Path

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


def generate_alter_statements(
    project_file_path: str | None = typer.Option(
        None,
        "-c",
        "--config",
        help="Path to kelp_project.yml (optional, will auto-detect if not provided)",
    ),
    target: str | None = typer.Option(
        None,
        "--target",
        help="Environment to use for variable resolution",
    ),
    profile: str | None = typer.Option(
        None,
        "-p",
        "--profile",
        help="Databricks CLI profile to use",
    ),
    output_file: str | None = typer.Option(
        None,
        "-o",
        "--output",
        help="Path to output file for ALTER TABLE statements (optional, defaults to stdout)",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview output without writing"),
    silent: bool = typer.Option(
        False,
        "--silent",
        help="Only output ALTER TABLE statements, suppressing other logs",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable debug logging (overrides --log-level)",
    ),
):
    """Generate ALTER TABLE statements for tables and metric views in the catalog that are different from the Databricks catalog."""
    log_level = "DEBUG" if debug else None
    from kelp.catalog.api import sync_catalog
    from kelp.config.lifecycle import init

    resolved_target = _resolve_target(target)
    init(project_root=project_file_path, target=resolved_target, log_level=log_level)
    queries = sync_catalog(sync_functions=True)
    if not silent:
        for q in queries:
            typer.echo(q + ";")
    if output_file:
        if dry_run:
            typer.secho(f"• dry-run: skipped writing {output_file}", fg=typer.colors.YELLOW)
            return
        with Path(output_file).open("w") as f:
            for q in queries:
                f.write(q + ";\n")
        typer.echo(f"\nALTER TABLE statements written to {output_file}")
