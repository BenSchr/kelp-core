import logging
from pathlib import Path

import typer
import yaml
from dotenv import load_dotenv

from kelp.config.lifecycle import get_context
from kelp.config.settings import resolve_setting
from kelp.service.pipeline_manager import PipelineManager
from kelp.service.table_manager import TableManager
from kelp.service.yaml_manager import ServicePathConfig, YamlManager
from kelp.utils.databricks import get_table_from_dbx_sdk

logger = logging.getLogger(__name__)


app = typer.Typer()


def _resolve_target(target: str | None) -> str | None:
    """Resolve a target from settings when not provided.

    Args:
        target: Explicit target value, if provided.

    Returns:
        Resolved target or None if not set anywhere.

    """
    return target or resolve_setting("target", default=None)


@app.command()
def sync_from_catalog(
    table_path: str = typer.Argument(
        ...,
        help="Fully qualified table name, e.g. database.schema.table",
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
        help="Path to output file for YAML (optional)",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview output without writing"),
) -> None:
    """Sync specified table metadata from the Databricks catalog to a YAML model definition.

    Fetches table metadata from Databricks and outputs a sample YAML model
    definition suitable for including in a project's kelp_models.
    """

    table = get_table_from_dbx_sdk(table_path, profile=profile)

    # Use unified model serialization (no hierarchy defaults for standalone output)
    model_dict = YamlManager.table_to_model_dict(table, include_hierarchy_defaults=False)
    content = {"kelp_models": [model_dict]}

    yaml_content = yaml.safe_dump(content, sort_keys=False)
    if output_file:
        if dry_run:
            typer.echo(yaml_content)
            typer.secho(f"• dry-run: skipped writing {output_file}", fg=typer.colors.YELLOW)
            return
        with Path(output_file).open("w") as f:
            f.write(yaml_content)
        typer.secho(f"✓ YAML written to {output_file}", fg=typer.colors.GREEN)
        return

    styled_output = typer.style(
        yaml_content,
        fg=typer.colors.GREEN,
        bold=True,
    )
    typer.echo(styled_output)


@app.command()
def sync_from_pipeline(
    pipeline_id: str = typer.Option(None, "--id", help="Databricks pipeline ID"),
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
        help="Path to output file for sync log",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview changes without writing"),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable debug logging (overrides --log-level)",
    ),
) -> None:
    """Sync table metadata from a Databricks pipeline to local YAML files.

    Fetches all tables from the specified pipeline, compares them with the catalog,
    and creates or updates YAML files for new or changed tables.
    """

    from kelp.config.lifecycle import init

    load_dotenv()
    log_level = "DEBUG" if debug else None
    resolved_target = _resolve_target(target)

    # Initial setup - init with current target (may be None)
    init(project_root=project_file_path, target=resolved_target, log_level=log_level)

    log_lines: list[str] = []

    def _log(message: str, *, err: bool = False) -> None:
        log_lines.append(message)
        typer.echo(message, err=err)

    # Initialize managers
    pipeline_manager = PipelineManager(profile=profile)

    if not pipeline_id:
        pipelines = pipeline_manager.detect_pipelines(target=resolved_target)
        if not pipelines:
            logger.error(
                "No pipeline ID provided and auto-detection failed for target '%s'",
                resolved_target,
            )
            raise typer.Exit(1)

        # If no target was originally resolved, use the detected target and reinit
        if not resolved_target:
            detected_target = pipelines[0].target
            _log(f"Using target: {detected_target}")
            init(project_root=project_file_path, target=detected_target, log_level=log_level)
            resolved_target = detected_target

        pipeline_ids = [p.id for p in pipelines]
        pipeline_info_str = ", ".join(f"{p.name} ({p.target})" for p in pipelines)
        _log(f"Auto-detected pipelines: {pipeline_info_str}")
    else:
        pipeline_ids = [pipeline_id]

    _log(
        f"Fetching tables from pipeline {pipeline_ids[0] if len(pipeline_ids) == 1 else 'multiple'}..."
    )

    # Get context after pipelines detected and target resolved
    try:
        ctx = get_context()
    except Exception as e:
        logger.error("Failed to load project context: %s", e)
        raise Exception(f"Failed to load project context: {e}") from e
    # Fetch tables from pipeline
    tables = []
    for pipeline_id in pipeline_ids:
        p_tables = pipeline_manager.fetch_pipeline_tables(
            pipeline_id,
            quarantine_config=ctx.project_config.quarantine_config,
        )
        tables.extend(p_tables)
        _log(f"Fetched {len(p_tables)} tables from pipeline {pipeline_id}")

    if not tables:
        logger.warning("No tables found in pipeline.")
        raise typer.Exit(1)

    # Compare with catalog and sync
    catalog_index = ctx.catalog.table_index
    new_tables = []
    existing_tables = []

    for table in tables:
        if table.name in catalog_index:
            existing_tables.append(table)
            table.origin_file_path = catalog_index[table.name].origin_file_path
        else:
            new_tables.append(table)

    _log(f"  - {len(existing_tables)} tables exist in catalog")
    _log(f"  - {len(new_tables)} new tables not in catalog")

    # Create central path configuration for all sync operations
    path_config = ServicePathConfig.from_context()

    # Sync all tables
    synced_count = 0
    created_count = 0
    updated_count = 0
    no_change_count = 0
    checked_count = 0
    errors = []
    dry_run_updates: list[str] = []
    dry_run_skipped: list[str] = []
    result_messages: list[str] = []

    with typer.progressbar(
        tables,
        label="Syncing tables",
        length=len(tables),
    ) as progress:
        for table in progress:
            checked_count += 1
            try:
                report = YamlManager.patch_table_yaml(
                    table,
                    path_config=path_config,
                    dry_run=dry_run,
                )
                synced_count += 1

                # Determine status based on report
                if report.changes_made:
                    is_new = table.name not in catalog_index
                    if is_new:
                        created_count += 1
                        status = "created"
                    else:
                        updated_count += 1
                        status = "updated"

                    # Show details of changes
                    change_details = []
                    if report.added_fields:
                        change_details.append(f"+{len(report.added_fields)} fields")
                    if report.updated_fields:
                        change_details.append(f"~{len(report.updated_fields)} fields")
                    if report.removed_fields:
                        change_details.append(f"-{len(report.removed_fields)} fields")

                    details = f" ({', '.join(change_details)})" if change_details else ""
                    if dry_run:
                        dry_run_updates.append(f"{table.name} -> {report.file_path}{details}")
                    else:
                        msg = f"✓ {status}: {table.name}{details} at {report.file_path}"
                        result_messages.append(msg)
                        logger.debug(msg)
                else:
                    no_change_count += 1
                    logger.debug("• no change: %s", table.name)

            except Exception as e:  # noqa: BLE001
                error_msg = f"Failed to sync {table.name}: {e}"
                errors.append(error_msg)
                logger.error(error_msg)
                if dry_run:
                    dry_run_skipped.append(f"{table.name} (error: {e})")
                continue

    # Print result summary
    typer.echo()
    if result_messages:
        for msg in result_messages:
            _log(msg)
        _log("")
    _log(f"✓ Sync complete: {synced_count}/{len(tables)} tables synced")
    _log(f"  - {created_count} created")
    _log(f"  - {updated_count} updated")
    _log(f"  - {no_change_count} unchanged")
    if dry_run:
        _log("\nDry-run report:")
        if dry_run_updates:
            _log("  Would update:")
            for line in dry_run_updates:
                _log(f"    - {line}")
        if dry_run_skipped:
            _log("  Skipped:")
            for line in dry_run_skipped:
                _log(f"    - {line}")
        _log(f"  Checked: {checked_count}")

    if errors:
        _log(f"\n{len(errors)} errors occurred:", err=True)
        for error in errors:
            _log(f"  - {error}", err=True)
        raise typer.Exit(1)

    if output_file:
        with Path(output_file).open("w") as f:
            for line in log_lines:
                f.write(line + "\n")
        typer.secho(f"✓ Sync log written to {output_file}", fg=typer.colors.GREEN)


@app.command()
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

    load_dotenv()
    resolved_target = _resolve_target(target)
    init(project_root=project_file_path, target=resolved_target, log_level=log_level)
    queries = sync_catalog()
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


@app.command()
def generate_ddl(
    name: str = typer.Argument(..., help="Name of the table or metric view"),
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
    output_file: str | None = typer.Option(
        None,
        "-o",
        "--output",
        help="Path to output file for DDL statement (optional, defaults to stdout)",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview output without writing"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug logging"),
) -> None:
    """Generate CREATE DDL statement for a table or metric view.

    This command generates the DDL for creating a metric view from the catalog.
    For tables, this currently shows metadata (CREATE TABLE DDL generation is not yet implemented).
    """
    log_level = "DEBUG" if debug else None
    from kelp.catalog.metric_view_ddl import generate_create_metric_view_ddl
    from kelp.config.lifecycle import init

    load_dotenv()
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
    typer.secho(f"✗ '{name}' not found in catalog (tables or metric views)", fg=typer.colors.RED)
    raise typer.Exit(code=1)
