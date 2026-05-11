from typing import Annotated

from typer import Argument, Option

from kelp.cli.common_params import (
    dbx_profile_option,
    dry_run_option,
)


def sync_from_catalog(
    table_path: Annotated[
        str, Argument(help="Fully qualified table name, e.g. database.schema.table")
    ],
    profile: dbx_profile_option = None,
    include_properties: Annotated[
        bool,
        Option(
            "--include-properties",
            help="Include all table properties in the output YAML (use with caution, may include many properties)",
        ),
    ] = False,
    output_file: Annotated[
        str | None,
        Option(
            "--output",
            "-o",
            help="Path to output file for YAML (optional)",
        ),
    ] = None,
    dry_run: dry_run_option = False,
) -> None:
    """Sync specified table metadata from the Databricks catalog to a YAML model definition.

    Fetches table metadata from Databricks and outputs a sample YAML model
    definition suitable for including in a project's kelp_models.
    """

    from pathlib import Path

    import yaml

    from kelp.cli.output import print_message, print_success, print_warning
    from kelp.service.yaml_manager import YamlManager
    from kelp.utils.databricks import get_table_from_dbx_sdk

    table = get_table_from_dbx_sdk(table_path, profile=profile)
    if not table:
        print_warning(f"⚠ Table not found in Databricks catalog: {table_path}")
        return
    # Use unified model serialization (no hierarchy defaults for standalone output)

    model_dict = YamlManager.model_to_dict(table, include_hierarchy_defaults=False)

    if not include_properties:
        # Remove table_properties to avoid overwhelming output with irrelevant properties
        model_dict.pop("table_properties", None)

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
