import typer
from kelp.utils.databricks import get_table_from_dbx_sdk
from kelp.service.pipeline_manager import PipelineManager
from kelp.service.yaml_manager import YamlManager
from kelp.config.lifecycle import get_context
import yaml
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


app = typer.Typer()


@app.command()
def generate_model_yaml(
    table_path=typer.Argument(..., help="Fully qualified table name, e.g. database.schema.table"),
    profile: str | None = typer.Option(
        None, "-p", "--profile", help="Databricks CLI profile to use"
    ),
    exclude: list[str] = typer.Option(
        default=["table_properties", "schema", "catalog"],
        help="List of table attributes to exclude from the generated YAML",
    ),
) -> None:
    """Generate a sample kelp_project.yml file."""

    exclude = list(exclude)

    if "schema" in exclude:
        exclude.remove("schema")
        exclude.append("schema_")

    table = get_table_from_dbx_sdk(table_path, profile=profile)
    model_content = table.model_dump(exclude=exclude, exclude_none=True, exclude_defaults=True)
    # filter all nulls
    model_content = {k: v for k, v in model_content.items() if v}

    new_columns = []
    for column in model_content["columns"]:
        column = {k: v for k, v in column.items() if v}
        # if column["data_type"].startswith("array"):
        #     column["data_type"] = "array"
        new_columns.append(column)
    model_content["columns"] = new_columns

    content = {"kelp_models": [model_content]}
    yaml_content = typer.style(
        yaml.dump(content, sort_keys=False), fg=typer.colors.GREEN, bold=True
    )
    typer.echo(yaml_content)


@app.command()
def sync_from_pipeline(
    pipeline_id: str = typer.Argument(..., help="Databricks pipeline ID"),
    project_file_path: str | None = typer.Option(
        None,
        "-c",
        "--config",
        help="Path to kelp_project.yml (optional, will auto-detect if not provided)",
    ),
    target: str = typer.Option(
        "dev", "--target", help="Environment to use for variable resolution (default: dev)"
    ),
    profile: str | None = typer.Option(
        None, "-p", "--profile", help="Databricks CLI profile to use"
    ),
    debug: bool = typer.Option(
        False, "--debug", help="Enable debug logging (overrides --log-level)"
    ),
) -> None:
    """Sync table metadata from a Databricks pipeline to local YAML files.

    Fetches all tables from the specified pipeline, compares them with the catalog,
    and creates or updates YAML files for new or changed tables.
    """

    from kelp.config.lifecycle import init

    log_level = "DEBUG" if debug else None
    init(project_root=project_file_path, target=target, log_level=log_level)

    try:
        ctx = get_context()
    except Exception as e:
        logger.error(f"{e}", exc_info=True)
        raise typer.Exit(1)

    typer.echo(f"Fetching tables from pipeline {pipeline_id}...")

    # Initialize managers
    pipeline_manager = PipelineManager(profile=profile)

    # Fetch tables from pipeline
    tables = pipeline_manager.fetch_pipeline_tables(
        pipeline_id, quarantine_config=ctx.project_config.quarantine_config
    )

    if not tables:
        logger.warning("No tables found in pipeline.")
        raise typer.Exit(1)

    typer.echo(f"Fetched {len(tables)} tables from pipeline {pipeline_id}")

    # Compare with catalog and sync
    catalog_index = ctx.catalog.index
    new_tables = []
    existing_tables = []

    for table in tables:
        if table.name in catalog_index:
            existing_tables.append(table)
        else:
            new_tables.append(table)

    typer.echo(f"  - {len(existing_tables)} tables exist in catalog")
    typer.echo(f"  - {len(new_tables)} new tables not in catalog")

    # Sync all tables
    synced_count = 0
    created_count = 0
    updated_count = 0
    no_change_count = 0
    errors = []

    for table in tables:
        try:
            report = YamlManager.patch_table_yaml(table)
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
                typer.echo(f"  ✓ {status}: {table.name}{details} at {report.file_path}")
            else:
                no_change_count += 1
                typer.echo(f"  • no change: {table.name}")

        except Exception as e:
            error_msg = f"Failed to sync {table.name}: {e}"
            errors.append(error_msg)
            logger.error(f"  ✗ {error_msg}")
            continue

    # Summary
    typer.echo(f"\nSync complete: {synced_count}/{len(tables)} tables synced")
    typer.echo(f"  - {created_count} created")
    typer.echo(f"  - {updated_count} updated")
    typer.echo(f"  - {no_change_count} no changes")

    if errors:
        typer.echo(f"\n{len(errors)} errors occurred:", err=True)
        for error in errors:
            typer.echo(f"  - {error}", err=True)
        raise typer.Exit(1)


@app.command()
def generate_alter_statements(
    project_file_path: str | None = typer.Option(
        None,
        "-c",
        "--config",
        help="Path to kelp_project.yml (optional, will auto-detect if not provided)",
    ),
    target: str = typer.Option(
        "dev", "--target", help="Environment to use for variable resolution (default: dev)"
    ),
    profile: str | None = typer.Option(
        None, "-p", "--profile", help="Databricks CLI profile to use"
    ),
    output_file: str | None = typer.Option(
        None,
        "-o",
        "--output",
        help="Path to output file for ALTER TABLE statements (optional, defaults to stdout)",
    ),
    silent: bool = typer.Option(
        False, "--silent", help="Only output ALTER TABLE statements, suppressing other logs"
    ),
    debug: bool = typer.Option(
        False, "--debug", help="Enable debug logging (overrides --log-level)"
    ),
):
    """Generate ALTER TABLE statements for tables in the catalog that are missing from the pipeline."""
    log_level = "DEBUG" if debug else None
    from kelp.config.lifecycle import init
    from kelp.catalog.api import sync_catalog

    load_dotenv()
    init(project_root=project_file_path, target=target, log_level=log_level)
    queries = sync_catalog()
    if not silent:
        for q in queries:
            typer.echo(q + ";")
    if output_file:
        with open(output_file, "w") as f:
            for q in queries:
                f.write(q + ";\n")
        typer.echo(f"\nALTER TABLE statements written to {output_file}")
