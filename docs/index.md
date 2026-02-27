
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

## Installation

To install Kelp, you can use either `uv` or `pip` or the package manager of your choice. Below are the commands for both methods:

```
uv add kelp-py==0.1.0
```

```
pip install kelp-py==0.1.0
```


## Initialization

After activating your environment and installing `kelp`, you can initialize a new Kelp project in your desired directory by running the following command:

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

## Setup targets and base configurations

Targets in Kelp represent different environments or configurations for your pipelines (e.g., development, staging, production). You can define targets in your `kelp_project.yml` file under the `targets` section. Each target can have its own settings, such as catalog and schema variables and other environment-specific configurations.

```yaml
kelp_project:

  models_path: "./kelp_metadata/models"
  models:
    +catalog: ${ catalog } # (1)!
    bronze:
      +schema: kelp_bronze
    silver:
      +schema: kelp_silver
    gold:
      +schema: kelp_gold
    +tags:
      kelp_managed: "" # (2)!

  metrics_path: "./kelp_metadata/metrics"
  metric_views:
    +catalog: ${ catalog }
    +schema: kelp_gold
    +tags:
      kelp_managed: ""

vars:
  default_catalog: my_catalog
  default_schema: my_schema

targets:
  dev:
    vars:
      catalog: ${default_catalog}_dev # (3)!
      schema: ${default_schema}_dev
  prod:
    vars:
      catalog: ${default_catalog}_prod
      schema: ${default_schema}_prod
```

1. Setup directory level configurations `+` which can be inherited by all models and metric views in that directory.
2. This sets a tag on all models in this project.
3. You can overwrite variables for each target.

## Sync your pipeline tables

If you use Databricks SDP you can run following command to sync tables from pipelines to metadata:

```bash
kelp sync-from-pipeline <pipeline_id>
```
If no tables are found validate or run the pipeline and try again.

If you omit `<pipeline_id>`, Kelp tries to detect the pipeline IDs from your local databricks asset bundle state (if it exists).

```
kelp sync-from-pipeline
```

## Set Environment Variables

If you often reuse a specific target and project path you can set them as environment variables to avoid passing them as arguments in every command.

```bash
export KELP_TARGET=dev
export KELP_PROJECT_PATH=/path/to/your/kelp_project.yml
```
