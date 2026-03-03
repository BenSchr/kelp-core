import json
from pathlib import Path
from typing import Annotated

import typer

from kelp.cli.catalog import app as catalog_app
from kelp.cli.init import app as init_app
from kelp.config.settings import resolve_setting
from kelp.models.jsonschema import generate_json_schema

kelp_banner = """
⌜                                 ⌝
  ██╗  ██╗███████╗██╗     ██████╗  
  ██║ ██╔╝██╔════╝██║     ██╔══██╗ 
  █████╔╝ █████╗  ██║     ██████╔╝ 
  ██╔═██╗ ██╔══╝  ██║     ██╔═══╝  
  ██║  ██╗███████╗███████╗██║      
  ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝      
⌞                                 ⌟
Metadata Toolkit for Databricks Spark and Declarative Pipelines
"""

app = typer.Typer(
    name="kelp",
    help="🌿 Kelp - Metadata Toolkit for Databricks Spark and Declarative Pipelines",
    no_args_is_help=True,
)

app.add_typer(catalog_app)
app.add_typer(init_app)


def _resolve_target(target: str | None) -> str | None:
    """Resolve a target from settings when not provided.

    Args:
        target: Explicit target value, if provided.

    Returns:
        Resolved target or None if not set anywhere.

    """
    return target or resolve_setting("target", default=None)


@app.command()
def version() -> None:
    """Display the current version of Kelp."""
    typer.echo(kelp_banner)
    typer.echo("Kelp version: 0.0.0")


@app.command()
def json_schema(
    output: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="Output path for the JSON schema file",
        ),
    ] = Path("kelp_json_schema.json"),
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Preview output without writing"),
    ] = False,
) -> None:
    """Generate JSON schema for kelp_project.yml configuration.

    Args:
        output: Path where the JSON schema will be saved.

    """
    json_schema = generate_json_schema()
    if dry_run:
        typer.echo(json.dumps(json_schema, indent=2))
        typer.secho(f"• dry-run: skipped writing {output}", fg=typer.colors.YELLOW)
        return

    with output.open("w") as f:
        json.dump(json_schema, f, indent=2)

    typer.secho(f"✓ JSON schema created: {output}", fg=typer.colors.GREEN)


@app.command()
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


@app.command()
def sync_local_catalog(
    name: Annotated[
        str | None,
        typer.Argument(help="Table or metric view name/FQN to sync"),
    ] = None,
    config_path: Annotated[
        str | None,
        typer.Option("-c", help="Path to the kelp_project.yml"),
    ] = None,
    target: Annotated[str | None, typer.Option(help="Environment to sync against")] = None,
    profile: Annotated[str | None, typer.Option("-p", help="Databricks CLI profile to use")] = None,
    output_file: Annotated[
        str | None,
        typer.Option("-o", "--output", help="Path to output file for sync log"),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Preview changes without writing"),
    ] = False,
    debug: Annotated[bool, typer.Option(help="Debug mode")] = False,
) -> None:
    """Sync local YAML files with remote Unity Catalog tables and metric views.

    If a name or FQN is provided, only that object is synced. Otherwise, all
    cataloged objects are synced.
    """

    from kelp.config.lifecycle import get_context, init
    from kelp.service.yaml_manager import ServicePathConfig, YamlManager
    from kelp.utils.databricks import get_metric_view_from_dbx_sdk, get_table_from_dbx_sdk

    log_level = "DEBUG" if debug else None
    resolved_target = _resolve_target(target)
    init(config_path, resolved_target, log_level=log_level)
    ctx = get_context()

    tables: list = []
    metric_views: list = []

    if name:
        table_match = ctx.catalog.table_index.get(name)
        metric_match = ctx.catalog.metrics_index.get(name)

        if not table_match and not metric_match:
            for table in ctx.catalog.get_tables():
                if table.get_qualified_name() == name:
                    table_match = table
                    break
            if not metric_match:
                for metric_view in ctx.catalog.get_metric_views():
                    if metric_view.get_qualified_name() == name:
                        metric_match = metric_view
                        break

        if table_match:
            tables = [table_match]
        if metric_match:
            metric_views = [metric_match]

        if not tables and not metric_views:
            typer.secho(
                f"✗ '{name}' not found in local catalog (tables or metric views)",
                fg=typer.colors.RED,
            )
            raise typer.Exit(code=1)
    else:
        tables = ctx.catalog.get_tables()
        metric_views = ctx.catalog.get_metric_views()

    table_path_config = ServicePathConfig.from_context()
    metric_path_config = None
    if ctx.project_config.metrics_path:
        metric_path_config = ServicePathConfig.from_context(
            service_root_key="metrics_path",
            hierarchy_config_key="metric_views",
        )

    log_lines: list[str] = []

    def _log(message: str, *, err: bool = False) -> None:
        log_lines.append(message)
        typer.echo(message, err=err)

    dry_run_updates: list[str] = []
    dry_run_skipped: list[str] = []
    unchanged_count = 0
    tables_checked = 0
    metric_views_checked = 0

    with typer.progressbar(
        tables,
        label="Syncing tables",
        length=len(tables),
    ) as progress:
        for table in progress:
            tables_checked += 1
            fqn = table.get_qualified_name()
            try:
                remote = get_table_from_dbx_sdk(fqn, profile=profile)
            except Exception as exc:  # noqa: BLE001
                _log(f"• skipped (not in remote): {fqn} ({exc})")
                if dry_run:
                    dry_run_skipped.append(f"{fqn} (not in remote)")
                continue
            remote.origin_file_path = table.origin_file_path

            report = YamlManager.patch_table_yaml(
                remote,
                path_config=table_path_config,
                dry_run=dry_run,
            )
            if report.changes_made:
                if dry_run:
                    dry_run_updates.append(f"{table.name} -> {report.file_path}")
                else:
                    _log(f"✓ updated: {table.name} at {report.file_path}")
            else:
                unchanged_count += 1

    if metric_views and not metric_path_config:
        _log("✗ metrics_path is not configured in kelp_project.yml", err=True)
        raise typer.Exit(code=1)

    with typer.progressbar(
        metric_views,
        label="Syncing metric views",
        length=len(metric_views),
    ) as progress:
        for metric_view in progress:
            metric_views_checked += 1
            fqn = metric_view.get_qualified_name()
            try:
                remote = get_metric_view_from_dbx_sdk(fqn, profile=profile)
            except Exception as exc:  # noqa: BLE001
                _log(f"• skipped (not in remote): {fqn} ({exc})")
                if dry_run:
                    dry_run_skipped.append(f"{fqn} (not in remote)")
                continue
            remote.origin_file_path = metric_view.origin_file_path

            report = YamlManager.patch_metric_view_yaml(
                remote,
                path_config=metric_path_config,
                dry_run=dry_run,
            )
            if report.changes_made:
                if dry_run:
                    dry_run_updates.append(f"{metric_view.name} -> {report.file_path}")
                else:
                    _log(f"✓ updated: {metric_view.name} at {report.file_path}")
            else:
                unchanged_count += 1

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
        _log(f"  Unchanged: {unchanged_count}")
        _log(f"  Tables checked: {tables_checked}")
        if metric_views:
            _log(f"  Metric views checked: {metric_views_checked}")

    if output_file:
        with Path(output_file).open("w") as f:
            for line in log_lines:
                f.write(line + "\n")
        typer.secho(f"✓ Sync log written to {output_file}", fg=typer.colors.GREEN)


def main() -> None:
    """Entry point for the CLI."""
    app()
