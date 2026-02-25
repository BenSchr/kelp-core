
```
██╗  ██╗███████╗██╗     ██████╗
██║ ██╔╝██╔════╝██║     ██╔══██╗
█████╔╝ █████╗  ██║     ██████╔╝
██╔═██╗ ██╔══╝  ██║     ██╔═══╝
██║  ██╗███████╗███████╗██║
╚═╝  ╚═╝╚══════╝╚══════╝╚═╝
Metadata Toolkit for Databricks Spark and Declarative Pipelines
```
Welcome to Kelp's documentation! This guide will help you get started with installing and initializing Kelp for your data engineering projects. Kelp is a powerful framework designed to simplify the management of data pipelines, quality checks, and table configurations. Follow the instructions below to set up Kelp in your environment and start building robust data solutions.

# Installation

To install Kelp, you can use either `uv` or `pip` or the package manager of your choice. Below are the commands for both methods:

```
uv add kelp-py==0.1.0
```

```
pip install kelp-py==0.1.0
```


# Initialization

```
kelp init .
```


This will create a `kelp_project.yml` file in the current directory, which is the main configuration file for your Kelp project. You can customize this file to specify your project's settings, variables and file paths.


```python
kelp_project.yml # (1)!
kelp_metadata/# (2)!
    models/**/*.yml
    metrics/**/*.yml
```

1. Here lives your main project configuration file where you can set global settings, variables, and other configurations for your Kelp project.
2. This directory is where you store your model and metric definitions in YAML format. You can organize them in subdirectories as needed (e.g., by environment, team, or domain).

Example structure
```markdown
kelp_project.yml 
kelp_metadata/
    models/
        bronze/ 
            bronze_customers.yml
        silver/
            silver_customers.yml
        gold/
            gold_customers.yml
    metrics/
        customer_metrics.yml
```

If you use Databricks SDP you can run following command to sync tables from pipelines to metadata:

```bash
kelp sync-from-pipeline <pipeline_id>
```

If you omit `<pipeline_id>`, Kelp tries to detect the pipeline IDs from your databricks asset bundle (if it exists) and syncs tables from those pipelines.

```
kelp sync-from-pipeline
```
