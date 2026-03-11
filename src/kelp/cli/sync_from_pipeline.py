import logging
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
        help="Databricks CLI profile to use, overrides DATABRICKS_CLI_PROFILE environment variable (defaults to 'DEFAULT' profile if not set)",
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
    from kelp.cli.output import print_error, print_message, print_success
    from kelp.config import get_context, init
    from kelp.service.pipeline_manager import PipelineManager
    from kelp.service.yaml_manager import ServicePathConfig, YamlManager

    logger = logging.getLogger(__name__)

    log_level = "DEBUG" if debug else None
    resolved_target = _resolve_target(target)

    # Initial setup - init with current target (may be None)
    init(project_file_path=project_file_path, target=resolved_target, log_level=log_level)

    log_lines: list[str] = []

    def _log(message: str, *, err: bool = False) -> None:
        log_lines.append(message)
        if err:
            print_error(message)
        else:
            print_message(message)

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
            init(project_file_path=project_file_path, target=detected_target, log_level=log_level)
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
        p_tables = pipeline_manager.fetch_pipeline_models(
            pipeline_id,
            quarantine_config=ctx.project_settings.quarantine_config,
        )
        for table in p_tables:
            _log(f"  • {table.name}")
        tables.extend(p_tables)
        _log(f"Fetched {len(p_tables)} tables from pipeline {pipeline_id}")

    if not tables:
        logger.warning("No tables found in pipeline.")
        raise typer.Exit(1)

    # Compare with catalog and sync
    catalog_index = ctx.catalog_index.get_index("models")
    print(catalog_index.keys())
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

    for table in tables:
        checked_count += 1
        try:
            report = YamlManager.patch_model_yaml(
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
                    msg = f"✓ {status}: {table.name}{details}"
                    result_messages.append(msg)
                    logger.debug(msg)
            else:
                no_change_count += 1
                if dry_run:
                    dry_run_skipped.append(f"{table.name} (no changes)")
                logger.debug("• no change: %s", table.name)

        except Exception as e:  # noqa: BLE001
            error_msg = f"Failed to sync {table.name}: {e}"
            errors.append(error_msg)
            logger.error(error_msg)
            if dry_run:
                dry_run_skipped.append(f"{table.name} (error: {e})")
            continue

    # Print result summary
    print_message("")
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
        print_success(f"✓ Sync log written to {output_file}")
