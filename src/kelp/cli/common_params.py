from typing import Annotated

from typer import Option

dbx_profile_option = Annotated[
    str | None, Option("--profile", "-p", help="Databricks CLI profile to use")
]
dry_run_option = Annotated[bool, Option("--dry-run", help="Preview output without writing")]

kelp_project_path_option = Annotated[
    str | None,
    Option(
        "--config",
        "-c",
        help="Path to kelp_project.yml (optional, will auto-detect if not provided)",
    ),
]

manifest_file_path_option = Annotated[
    str | None,
    Option("--manifest", "-m", help="Path to manifest JSON file (skips source file loading)"),
]
target_option = Annotated[
    str | None,
    Option("--target", "-t", help="Target to use for variable resolution"),
]
debug_option = Annotated[
    bool,
    Option(
        "--debug",
        help="Enable debug logging",
    ),
]
