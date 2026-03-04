from pathlib import Path

import typer

from kelp.config.lifecycle import get_context
from kelp.config.settings import resolve_setting
from kelp.service.table_manager import TableManager


def _resolve_target(target: str | None) -> str | None:
    """Resolve a target from settings when not provided.

    Args:
        target: Explicit target value, if provided.

    Returns:
        Resolved target or None if not set anywhere.

    """
    return target or resolve_setting("target", default=None)


def generate_ddl(
    name: str = typer.Argument(
        ..., help="Name of the table, metric view, function, or ABAC policy"
    ),
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
        help="Path to output file for DDL statement (optional, defaults to stdout)",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview output without writing"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug logging"),
) -> None:
    """Generate CREATE DDL statement for a table, metric view, function, or ABAC policy.

    For tables, this currently shows metadata (CREATE TABLE DDL generation is not yet implemented).
    """
    log_level = "DEBUG" if debug else None
    from kelp.catalog.metric_view_ddl import generate_create_metric_view_ddl
    from kelp.config.lifecycle import init

    resolved_target = _resolve_target(target)
    init(project_root=project_file_path, target=resolved_target, log_level=log_level)

    ctx = get_context()

    # Try to find in metric views first
    try:
        metric_view = ctx.catalog.get_metric_view(name, soft_handle=False)
        ddl = generate_create_metric_view_ddl(metric_view)

        if output_file:
            if dry_run:
                typer.echo(ddl + ";")
                typer.secho(f"• dry-run: skipped writing {output_file}", fg=typer.colors.YELLOW)
                return
            with Path(output_file).open("w") as f:
                f.write(ddl + ";\n")
            typer.secho(f"✓ DDL written to {output_file}", fg=typer.colors.GREEN)
        else:
            typer.echo(ddl + ";")
        return
    except KeyError:
        pass  # Not a metric view, try table

    # Try to find in functions
    try:
        function = ctx.catalog.get_function(name)
        from kelp.catalog.function_ddl import generate_create_function_ddl

        ddl = generate_create_function_ddl(function)

        if output_file:
            if dry_run:
                typer.echo(ddl + ";")
                typer.secho(f"• dry-run: skipped writing {output_file}", fg=typer.colors.YELLOW)
                return
            with Path(output_file).open("w") as f:
                f.write(ddl + ";\n")
            typer.secho(f"✓ DDL written to {output_file}", fg=typer.colors.GREEN)
        else:
            typer.echo(ddl + ";")
        return
    except KeyError:
        pass

    # Try to find in ABAC policies
    try:
        policy = ctx.catalog.get_abac(name)
        from kelp.catalog.abac_ddl import generate_create_abac_policy_ddl

        ddl = generate_create_abac_policy_ddl(policy)

        if output_file:
            if dry_run:
                typer.echo(ddl)
                typer.secho(f"• dry-run: skipped writing {output_file}", fg=typer.colors.YELLOW)
                return
            with Path(output_file).open("w") as f:
                f.write(ddl + "\n")
            typer.secho(f"✓ DDL written to {output_file}", fg=typer.colors.GREEN)
        else:
            typer.echo(ddl)
        return
    except KeyError:
        pass

    # Try to find in tables
    try:
        table = ctx.catalog.get_table(name, soft_handle=False)
        ddl = TableManager.get_spark_schema_ddl(table)
        if ddl is None:
            typer.secho(
                f"✗ Failed to generate DDL for table '{name}'",
                fg=typer.colors.RED,
            )
            raise typer.Exit(code=1)
        typer.secho(f"Found table: {table.get_qualified_name()}", fg=typer.colors.YELLOW)
        typer.echo(f"  Type: {table.table_type}")
        typer.echo(f"  Catalog: {table.catalog}")
        typer.echo(f"  Schema: {table.schema_}")
        typer.echo(f"  Columns: {len(table.columns)}")
        typer.echo("\n-- DDL --")
        typer.echo(ddl + ";")
        if output_file:
            if dry_run:
                typer.secho(f"• dry-run: skipped writing {output_file}", fg=typer.colors.YELLOW)
                return
            with Path(output_file).open("w") as f:
                f.write(ddl + ";\n")
            typer.secho(f"✓ DDL written to {output_file}", fg=typer.colors.GREEN)
        return
    except KeyError:
        pass  # Not found in either

    # Not found
    typer.secho(
        f"✗ '{name}' not found in catalog (tables, metric views, functions, or ABAC policies)",
        fg=typer.colors.RED,
    )
    raise typer.Exit(code=1)
