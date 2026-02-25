from pathlib import Path
from typing import Annotated

import typer

from kelp.constants import KELP_PROJECT_FILENAME

app = typer.Typer()


def _write_file(path: Path, content: str) -> None:
    """Write content to a file, creating parent directories as needed.

    Args:
            path: File path to write.
            content: File content.

    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _init_project_file(project_root: Path) -> Path:
    """Create the kelp_project.yml file with base configuration.

    Args:
            project_root: Root directory for the project.

    Returns:
            Path to the created kelp_project.yml.

    """
    project_file = project_root / KELP_PROJECT_FILENAME
    if project_file.exists():
        typer.secho(
            f"✗ {project_file} already exists. Remove it or choose another path.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)

    project_content = """\
kelp_project:
  models_path: "./kelp_metadata/models"
  models:
    +catalog: ${ catalog }
    +tags:
      kelp_managed: ""

  metrics_path: "./kelp_metadata/metrics"
  metric_views:
    +catalog: ${ catalog }
    +tags:
      kelp_managed: ""

  targets_path: "./kelp_metadata/targets"

vars:
  catalog: "my_catalog"

targets:
  dev:
    vars:
      catalog: ${ catalog }_dev

  prod:
    vars:
      catalog: ${ catalog }_prod
"""

    _write_file(project_file, project_content)
    return project_file


def _init_metadata_dirs(project_root: Path) -> list[Path]:
    """Create metadata directories and .gitkeep files.

    Args:
            project_root: Root directory for the project.

    Returns:
            List of created .gitkeep file paths.

    """
    metadata_root = project_root / "kelp_metadata"
    entries = {
        "models": ("Place model YAML files here. Each file should define a `kelp_models` list."),
        "metrics": (
            "Place metric view YAML files here. Each file should define a `kelp_metric_views` list."
        ),
        "targets": ("Optional target YAML files can live here when using `targets_path`."),
    }

    created_files: list[Path] = []
    for folder, description in entries.items():
        folder_path = metadata_root / folder
        folder_path.mkdir(parents=True, exist_ok=True)
        gitkeep_path = folder_path / ".gitkeep"
        if not gitkeep_path.exists():
            _write_file(gitkeep_path, description + "\n")
            created_files.append(gitkeep_path)
    return created_files


@app.command()
def init(
    project_root: Annotated[
        str,
        typer.Argument(
            help="Project directory to initialize (defaults to current working directory).",
        ),
    ] = ".",
) -> None:
    """Initialize a Kelp project scaffold.

    Creates a base `kelp_project.yml` and the `kelp_metadata` folder structure.

    Args:
            project_root: Directory to initialize.

    """

    project_root_path = Path(project_root).expanduser().resolve()
    project_root_path.mkdir(parents=True, exist_ok=True)

    project_file = _init_project_file(project_root_path)
    created_gitkeeps = _init_metadata_dirs(project_root_path)

    typer.secho(f"✓ Created {project_file}", fg=typer.colors.GREEN)
    for gitkeep in created_gitkeeps:
        typer.secho(f"✓ Created {gitkeep}", fg=typer.colors.GREEN)
