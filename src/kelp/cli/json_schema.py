import json
from pathlib import Path
from typing import Annotated

import typer


def _find_project_root() -> Path:
    """Find the project root directory (directory containing kelp_project.yml).

    Returns:
        Path to project root, or current working directory if not found.
    """
    cwd = Path.cwd()
    for parent in [cwd, *list(cwd.parents)]:
        if (parent / "kelp_project.yml").exists() or (parent / "kelp_project.yaml").exists():
            return parent
    return cwd


def strip_trailing_commas(s: str) -> str:
    """Remove trailing commas in JSON before closing brackets.

    Removes commas that appear just before a closing } or ].
    Handles nested cases by applying repeatedly until no more matches.

    Args:
        s: JSON string with potential trailing commas.

    Returns:
        Cleaned JSON string without trailing commas.
    """
    # Remove commas that appear *just before* a closing } or ]
    # Example: {"a":1,} -> {"a":1}
    #          [1,2,]   -> [1,2]
    # Apply repeatedly until no more matches to handle nested cases
    s = s.replace("\n", "")  # Remove newlines to simplify regex
    # Remove trailing commas at the end of the string
    if s.endswith((",}", ",]")):
        s = s[:-2] + s[-1]  # Remove the comma before the closing bracket

    return s


def _update_settings_json(vscode_dir: Path, schema_filename: str) -> None:
    """Create or update settings.json with YAML schema configuration.

    Preserves all existing settings and only updates the yaml.schemas section.

    Args:
        vscode_dir: Path to .vscode directory.
        schema_filename: Name of the schema file (e.g., "kelp_json_schema.json").
    """
    from kelp.cli.output import print_error, print_info, print_message, print_success

    settings_path = vscode_dir / "settings.json"
    print_message(f"Updating VS Code settings at: {settings_path}")
    # Load existing settings or create empty dict

    try:
        with settings_path.open("r") as f:
            clean = strip_trailing_commas(f.read())
            settings = json.loads(clean)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print_error(
            "If there are trailing commas e.g. ',}' or ',]' in settings.json, please remove them to ensure proper JSON formatting."
        )
        raise ValueError(f"Error reading {settings_path}: {e}") from e

    # Ensure yaml.schemas exists
    if "yaml.schemas" not in settings:
        settings["yaml.schemas"] = {}

    # Determine the schema path relative to .vscode
    # Only use .vscode prefix if the directory is actually named .vscode
    schema_ref = ".vscode/" + schema_filename if vscode_dir.name == ".vscode" else schema_filename

    # Schema configuration
    schema_config = [
        "kelp_project.yml",
        "kelp_project.yaml",
        "kelp_metadata/**/*.yml",
        "kelp_metadata/**/*.yaml",
    ]

    # Check if schema needs to be added
    needs_update = schema_ref not in settings["yaml.schemas"]

    if needs_update:
        # Add the kelp schema (preserves all other schemas)
        settings["yaml.schemas"][schema_ref] = schema_config

        # Write updated settings with proper formatting
        with settings_path.open("w") as f:
            json.dump(settings, f, indent=4)
            f.write("\n")  # Add trailing newline for proper file formatting

        print_success(f"✓ Updated {settings_path} with YAML schema configuration")
    else:
        print_info(f"• YAML schema already configured in {settings_path}")


def json_schema(
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Output path for JSON schema (default: current directory)",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(..., "--dry-run", help="Preview output without writing"),
    ] = False,
    vscode: Annotated[
        bool,
        typer.Option(
            ...,
            "--vscode",
            help="Create/update VS Code .vscode/settings.json with YAML schema config",
        ),
    ] = False,
) -> None:
    """Generate JSON schema for kelp_project.yml configuration.

    By default, this command will save the schema to the current directory as
    kelp_json_schema.json.

    Use --vscode flag to enable VS Code integration:
    1. Save the schema to .vscode/kelp_json_schema.json in the project root
    2. Create .vscode/settings.json if it doesn't exist
    3. Configure YAML schema references in settings.json

    Args:
        output: Path where the JSON schema will be saved. If not provided, defaults to
                current directory as kelp_json_schema.json.
        dry_run: Preview output without writing files.
        vscode: Enable VS Code integration (create .vscode dir, update settings.json).
    """
    from kelp.cli.output import print_message, print_success, print_warning
    from kelp.models.jsonschema import generate_json_schema

    # Determine output path
    if output is None:
        if vscode:
            # With --vscode flag, save to .vscode directory in project root
            project_root = _find_project_root()
            vscode_dir = project_root / ".vscode"
            output = vscode_dir / "kelp_json_schema.json"
        else:
            # Default: save to current directory
            output = Path.cwd() / "kelp_json_schema.json"
            vscode_dir = None
    else:
        output = Path(output)
        vscode_dir = output.parent if output.parent.name == ".vscode" else None

    json_schema_data = generate_json_schema()

    if dry_run:
        print_message(json.dumps(json_schema_data, indent=2))
        print_warning(f"• dry-run: skipped writing {output}")
        if vscode and vscode_dir:
            print_warning(f"• dry-run: would update {vscode_dir / 'settings.json'}")
        return

    # Create .vscode directory if needed
    if vscode:
        if vscode_dir is None:
            vscode_dir = output.parent
        vscode_dir.mkdir(parents=True, exist_ok=True)

    # Write schema file
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as f:
        json.dump(json_schema_data, f, indent=2)

    print_success(f"✓ JSON schema created: {output}")

    # Update settings.json if vscode flag is enabled
    if vscode and vscode_dir:
        _update_settings_json(vscode_dir, output.name)
