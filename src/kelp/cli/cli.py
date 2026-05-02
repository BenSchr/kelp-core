def main() -> None:
    """Entry point for the Kelp CLI.

    Lazily imports all commands to optimize startup time. Commands are loaded
    only when the user invokes them or requests help.
    """
    import typer

    app = typer.Typer(
        name="kelp",
        help="🌿 Kelp - Metadata Toolkit for Databricks Spark and Declarative Pipelines",
        no_args_is_help=True,
    )

    # Lazy import and register commands
    from kelp.cli.check_policies import check_policies
    from kelp.cli.generate_alter_statements import generate_alter_statements
    from kelp.cli.generate_ddl import generate_ddl
    from kelp.cli.get_model import get_model
    from kelp.cli.init import init
    from kelp.cli.json_schema import json_schema
    from kelp.cli.manifest import manifest
    from kelp.cli.sync_from_catalog import sync_from_catalog
    from kelp.cli.sync_from_pipeline import sync_from_pipeline
    from kelp.cli.sync_local_catalog import sync_local_catalog
    from kelp.cli.validate import validate
    from kelp.cli.version import version

    app.command()(version)
    app.command()(json_schema)
    app.command()(validate)
    app.command()(check_policies)
    app.command()(manifest)
    app.command()(get_model)
    app.command()(sync_local_catalog)
    app.command()(sync_from_catalog)
    app.command()(sync_from_pipeline)
    app.command()(generate_alter_statements)
    app.command()(generate_ddl)
    app.command()(init)

    app()
