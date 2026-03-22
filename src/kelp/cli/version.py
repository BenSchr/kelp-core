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
Metadata Toolkit for Databricks Spark and Declarative Pipelines
"""


def version() -> None:
    """Display the current version of Kelp."""
    print_message(kelp_banner)
    print_message("Kelp version: 0.0.5")
