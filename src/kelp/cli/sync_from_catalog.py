from pathlib import Path

import typer
import yaml

from kelp.service.yaml_manager import YamlManager
from kelp.utils.databricks import get_table_from_dbx_sdk


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

    yaml_content = yaml.safe_dump(content, sort_keys=False, allow_unicode=True)
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
