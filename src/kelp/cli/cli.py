import json
from pathlib import Path
from typing import Annotated
import typer
from kelp.models.jsonschema import generate_json_schema
from kelp.cli.catalog import app as catalog_app

app = typer.Typer(
    name="kelp",
    help="🌿 Kelp - A tool for managing your data projects with ease.",
    no_args_is_help=True,
)

app.add_typer(catalog_app)


@app.command()
def version() -> None:
    """Display the current version of Kelp."""
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
) -> None:
    """Generate JSON schema for kelp_project.yml configuration.

    Args:
        output: Path where the JSON schema will be saved.
    """
    json_schema = generate_json_schema()
    with open(output, "w") as f:
        json.dump(json_schema, f, indent=2)

    typer.secho(f"✓ JSON schema created: {output}", fg=typer.colors.GREEN)


@app.command()
def validate(
    config_path: Annotated[str, typer.Option("-c", help="Path to the kelp_project.yml")] = None,
    target: Annotated[str, typer.Option(help="Environment to validate against")] = "dev",
    debug: Annotated[bool, typer.Option(help="Debug mode")] = False,
) -> None:
    from kelp.config.lifecycle import init
    from kelp.config.runtime import resolve_project_root

    if not config_path:
        try:
            config_path = resolve_project_root()
            typer.secho(f"✓ Found project root at: {config_path}", fg=typer.colors.GREEN)
        except FileNotFoundError as e:
            typer.secho(f"✗ {str(e)}", fg=typer.colors.RED)
            raise typer.Exit(code=1)

    log_level = "DEBUG" if debug else None

    run_ctx = init(config_path, target, log_level=log_level)
    validated = True if run_ctx else False
    if validated:
        typer.secho("✓ Configuration is valid!", fg=typer.colors.GREEN)
    else:
        typer.secho("✗ Configuration is invalid!", fg=typer.colors.RED)
        raise typer.Exit(code=1)


def main() -> None:
    """Entry point for the CLI."""
    app()
