from pathlib import Path

import typer
import yaml


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
    from kelp.cli.output import print_message, print_success, print_warning
    from kelp.service.yaml_manager import YamlManager
    from kelp.utils.databricks import get_table_from_dbx_sdk

    table = get_table_from_dbx_sdk(table_path, profile=profile)
    if not table:
        print_warning(f"⚠ Table not found in Databricks catalog: {table_path}")
        return
    # Use unified model serialization (no hierarchy defaults for standalone output)
    model_dict = YamlManager.model_to_dict(table, include_hierarchy_defaults=False)
    content = {"kelp_models": [model_dict]}

    yaml_content = yaml.safe_dump(content, sort_keys=False, allow_unicode=True)
    if output_file:
        if dry_run:
            print_message(yaml_content)
            print_warning(f"• dry-run: skipped writing {output_file}")
            return
        with Path(output_file).open("w") as f:
            f.write(yaml_content)
        print_success(f"✓ YAML written to {output_file}")
        return

    print_message(yaml_content)
