import typer
from kelp.utils.databricks import get_table_from_dbx_sdk
import yaml

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

    if "schema" in exclude:
        exclude.remove("schema")
        exclude.append("schema_")

    table = get_table_from_dbx_sdk(table_path, profile=profile)
    model_content = table.model_dump(exclude=exclude, exclude_none=True, exclude_defaults=True)
    ## filter all nulls
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
