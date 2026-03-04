import typer

kelp_banner = """
⌜                                 ⌝
  ██╗  ██╗███████╗██╗     ██████╗  
  ██║ ██╔╝██╔════╝██║     ██╔══██╗ 
  █████╔╝ █████╗  ██║     ██████╔╝ 
  ██╔═██╗ ██╔══╝  ██║     ██╔═══╝  
  ██║  ██╗███████╗███████╗██║      
  ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝      
⌞                                 ⌟
Metadata Toolkit for Databricks Spark and Declarative Pipelines
"""


def version() -> None:
    """Display the current version of Kelp."""
    typer.echo(kelp_banner)
    typer.echo("Kelp version: 0.0.0")
