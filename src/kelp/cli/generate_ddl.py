from pathlib import Path

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
    manifest_file_path: str | None = typer.Option(
        None,
        "-m",
        "--manifest",
        help="Path to manifest JSON file (skips source file loading)",
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
    from kelp.catalog.abac_ddl import generate_create_abac_policy_ddl
    from kelp.catalog.function_ddl import generate_create_function_ddl
    from kelp.catalog.metric_view_ddl import generate_create_metric_view_ddl
    from kelp.cli.output import print_error, print_info, print_message, print_success, print_warning
    from kelp.config import get_context, init
    from kelp.service.model_manager import ModelManager

    log_level = "DEBUG" if debug else None
    resolved_target = _resolve_target(target)
    init(
        project_file_path=project_file_path,
        target=resolved_target,
        manifest_file_path=manifest_file_path,
        log_level=log_level,
    )

    ctx = get_context()

    # Try to find in metric views first
    try:
        metric_view = ctx.catalog_index.get("metric_views", name)
        ddl = generate_create_metric_view_ddl(metric_view)

        if output_file:
            if dry_run:
                print_message(ddl + ";")
                print_warning(f"• dry-run: skipped writing {output_file}")
                return
            with Path(output_file).open("w") as f:
                f.write(ddl + ";\n")
            print_success(f"✓ DDL written to {output_file}")
        else:
            print_message(ddl + ";")
        return
    except KeyError:
        pass  # Not a metric view, try table

    # Try to find in functions
    try:
        function = ctx.catalog_index.get("functions", name)
        ddl = generate_create_function_ddl(function)

        if output_file:
            if dry_run:
                print_message(ddl + ";")
                print_warning(f"• dry-run: skipped writing {output_file}")
                return
            with Path(output_file).open("w") as f:
                f.write(ddl + ";\n")
            print_success(f"✓ DDL written to {output_file}")
        else:
            print_message(ddl + ";")
        return
    except KeyError:
        pass

    # Try to find in ABAC policies
    try:
        policy = ctx.catalog_index.get("abacs", name)
        ddl = generate_create_abac_policy_ddl(policy)

        if output_file:
            if dry_run:
                print_message(ddl)
                print_warning(f"• dry-run: skipped writing {output_file}")
                return
            with Path(output_file).open("w") as f:
                f.write(ddl + "\n")
            print_success(f"✓ DDL written to {output_file}")
        else:
            print_message(ddl)
        return
    except KeyError:
        pass

    # Try to find in tables
    try:
        table = ctx.catalog_index.get("models", name)
        ddl = ModelManager.get_spark_schema_ddl(table)
        if ddl is None:
            print_error(f"✗ Failed to generate DDL for table '{name}'")
            raise typer.Exit(code=1)
        print_info(f"Found table: {table.get_qualified_name()}")
        print_message(f"  Type: {table.table_type}")
        print_message(f"  Catalog: {table.catalog}")
        print_message(f"  Schema: {table.schema_}")
        print_message(f"  Columns: {len(table.columns)}")
        print_message("\n-- DDL --")
        print_message(ddl + ";")
        if output_file:
            if dry_run:
                print_warning(f"• dry-run: skipped writing {output_file}")
                return
            with Path(output_file).open("w") as f:
                f.write(ddl + ";\n")
            print_success(f"✓ DDL written to {output_file}")
        return
    except KeyError:
        pass  # Not found in either

    # Not found
    print_error(
        f"✗ '{name}' not found in catalog (tables, metric views, functions, or ABAC policies)"
    )
    raise typer.Exit(code=1)
