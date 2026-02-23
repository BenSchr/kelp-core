import json
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv

from kelp.cli.catalog import app as catalog_app
from kelp.models.jsonschema import generate_json_schema

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
    load_dotenv()
    from kelp.config.lifecycle import init

    log_level = "DEBUG" if debug else None

    run_ctx = init(config_path, target, log_level=log_level)
    validated = True if run_ctx else False
    if validated:
        typer.secho("✓ Configuration is valid!", fg=typer.colors.GREEN)
    else:
        typer.secho("✗ Configuration is invalid!", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    typer.echo(f"Project config loaded from: {run_ctx.project_config.project_file_path}")
    typer.echo(f"Target environment: {run_ctx.target}")
    typer.echo(f"Runtime variables: {run_ctx.runtime_vars}")
    typer.echo(f"Relative models path: {run_ctx.project_config.models_path}")
    typer.echo(f"Models found: {len(run_ctx.catalog.index)}")


def main() -> None:
    """Entry point for the CLI."""
    app()
