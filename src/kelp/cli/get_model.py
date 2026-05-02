from typing import Annotated

import typer


def _resolve_target(target: str | None) -> str | None:
    """Resolve a target from settings when not provided.

    Args:
        target: Explicit target value, if provided.

    Returns:
        Resolved target or None if not set anywhere.
    """
    from kelp.config.settings import resolve_setting

    return target or resolve_setting("target", default=None)


def get_model(
    name: Annotated[
        str,
        typer.Argument(help="Model name to look up in the catalog"),
    ],
    project_file_path: Annotated[
        str | None,
        typer.Option(
            ...,
            "-c",
            "--config",
            help="Path to kelp_project.yml (optional, will auto-detect if not provided)",
        ),
    ] = None,
    target: Annotated[str | None, typer.Option(help="Target environment")] = None,
    manifest_file_path: Annotated[
        str | None,
        typer.Option(
            ...,
            "-m",
            "--manifest",
            help="Path to manifest JSON file (skips source file loading)",
        ),
    ] = None,
    debug: Annotated[bool, typer.Option(help="Debug mode")] = False,
) -> None:
    """Print a model definition from the catalog as JSON."""
    from kelp.cli.output import print_error, print_message
    from kelp.config import init
    from kelp.utils.logging import configure_logging

    log_level = "DEBUG" if debug else None
    if log_level:
        configure_logging(log_level)

    resolved_target = _resolve_target(target)

    ctx = init(
        project_file_path=project_file_path,
        target=resolved_target,
        manifest_file_path=manifest_file_path,
        refresh=True,
    )

    try:
        model = ctx.catalog_index.get("models", name)
    except KeyError:
        print_error(f"✗ Model '{name}' not found in catalog.")
        available = [m.name for m in ctx.catalog_index.get_all("models")]
        if available:
            print_message(f"  Available models: {', '.join(sorted(available))}")
        raise typer.Exit(code=1) from None

    print_message(model.model_dump_json(indent=2, by_alias=True, exclude_none=True))
