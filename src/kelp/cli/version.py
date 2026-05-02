from importlib.metadata import version as pkg_version

from kelp.cli.output import print_message

kelp_banner = """
⌜                                 ⌝
  ██╗  ██╗███████╗██╗     ██████╗  
  ██║ ██╔╝██╔════╝██║     ██╔══██╗ 
  █████╔╝ █████╗  ██║     ██████╔╝ 
  ██╔═██╗ ██╔══╝  ██║     ██╔═══╝  
  ██║  ██╗███████╗███████╗██║      
  ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝      
⌞                                 ⌟
Metadata Toolkit for Spark and Spark Declarative Pipelines
"""


def version() -> None:
    """Display the current version of Kelp."""
    print_message(kelp_banner)
    print_message(f"Kelp version: {pkg_version('kelp-core')}")
