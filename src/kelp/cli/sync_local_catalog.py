from pathlib import Path
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
    """Update local YAML files from remote Unity Catalog tables and metric views.

    If a name or FQN is provided, only that object is synced. Otherwise, all
    cataloged objects are synced.
    """

    from kelp.cli.output import print_error, print_message, print_success
    from kelp.config import get_context, init
    from kelp.models.project_config import ProjectConfig
    from kelp.service.yaml_manager import ServicePathConfig, YamlManager
    from kelp.utils.databricks import (
        get_metric_view_from_dbx_sdk,
        get_table_from_dbx_sdk,
    )

    log_level = "DEBUG" if debug else None
    resolved_target = _resolve_target(target)
    init(config_path, resolved_target, log_level=log_level)
    ctx = get_context()

    project_config: ProjectConfig = ctx.project_settings

    tables: list = []
    metric_views: list = []

    if name:
        table_match = ctx.catalog_index.get_index("models").get(name)
        metric_match = ctx.catalog_index.get_index("metric_views").get(name)

        if not table_match and not metric_match:
            for table in ctx.catalog_index.get_all("models"):
                if table.get_qualified_name() == name:
                    table_match = table
                    break
            if not metric_match:
                for metric_view in ctx.catalog_index.get_all("metric_views"):
                    if metric_view.get_qualified_name() == name:
                        metric_match = metric_view
                        break

        if table_match:
            tables = [table_match]
        if metric_match:
            metric_views = [metric_match]

        if not tables and not metric_views:
            print_error(f"✗ '{name}' not found in local catalog (tables or metric views)")
            raise typer.Exit(code=1)
    else:
        tables = ctx.catalog_index.get_all("models")
        metric_views = ctx.catalog_index.get_all("metric_views")

    table_path_config = ServicePathConfig.from_context()
    metric_path_config = None
    if project_config.metrics_path:
        metric_path_config = ServicePathConfig.from_context(
            service_root_key="metrics_path",
            hierarchy_config_key="metric_views",
        )
    remote_catalog_config = project_config.remote_catalog_config

    log_lines: list[str] = []

    def _log(message: str, *, err: bool = False) -> None:
        log_lines.append(message)
        if err:
            print_error(message)
        else:
            print_message(message)

    dry_run_updates: list[str] = []
    dry_run_skipped: list[str] = []
    unchanged_count = 0
    tables_checked = 0
    metric_views_checked = 0

    # Sync tables
    for table in tables:
        tables_checked += 1
        fqn = table.get_qualified_name()
        try:
            remote = get_table_from_dbx_sdk(fqn, profile=profile)
        except Exception as exc:  # noqa: BLE001
            _log(f"• skipped (not in remote): {fqn} ({exc})")
            if dry_run:
                dry_run_skipped.append(f"{fqn} (not in remote)")
            continue
        if not remote:
            _log(f"• skipped (not in remote): {fqn}")
            if dry_run:
                dry_run_skipped.append(f"{fqn} (not in remote)")
            continue
        remote.origin_file_path = table.origin_file_path

        report = YamlManager.patch_model_yaml(
            remote,
            path_config=table_path_config,
            dry_run=dry_run,
            remote_catalog_config=remote_catalog_config,
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

    # Sync metric views
    for metric_view in metric_views:
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
        print_success(f"✓ Sync log written to {output_file}")
