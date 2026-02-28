
```
██╗  ██╗███████╗██╗     ██████╗
██║ ██╔╝██╔════╝██║     ██╔══██╗
█████╔╝ █████╗  ██║     ██████╔╝
██╔═██╗ ██╔══╝  ██║     ██╔═══╝
██║  ██╗███████╗███████╗██║
╚═╝  ╚═╝╚══════╝╚══════╝╚═╝
Metadata Toolkit for Databricks Spark and Declarative Pipelines
```
Welcome to Kelp's documentation! This guide will help you install and initialize Kelp for your data engineering projects. Kelp is a powerful framework designed to simplify the management of data pipelines, quality checks, and table configurations. Follow the instructions below to set up Kelp in your environment and start building robust data solutions.

## Installation

To install Kelp, you can use `uv`, `pip`, or the package manager of your choice. Below are the commands for both methods:

```
uv add kelp-py==0.1.0
```

```
pip install kelp-py==0.1.0
```


## Initialization

After installing `kelp`, initialize a new Kelp project in your desired directory by running the following command:

```
kelp init .
```

This will create a `kelp_project.yml` file in the current directory, which is the main configuration file for your Kelp project. You can customize this file to specify your project's settings, variables and file paths.


```python
kelp_project.yml # (1)!
kelp_metadata/# (2)!
    models/**/*.yml
    metrics/**/*.yml
  functions/**/*.yml
  abacs/**/*.yml
```

1. This is where your main project configuration file lives. Here you can set global settings, variables, and other configurations for your Kelp project.
2. This directory stores your model and metric definitions in YAML format. You can organize them in subdirectories as needed (e.g., by environment, team, or domain).

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
    functions/
      functions.yml
      sql/
        mask_ssn.sql
    abacs/
      policies.yml
```

## Set Up Targets and Base Configurations

Targets in Kelp represent different environments or configurations for your pipelines (e.g., development, staging, production). Define targets in your `kelp_project.yml` file under the `targets` section. Each target can have its own settings, such as catalog and schema variables, as well as other environment-specific configurations.

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

  functions_path: "./kelp_metadata/functions"
  functions:
    +catalog: ${ security_catalog } # (4)!
    +schema: ${ security_schema }

  abacs_path: "./kelp_metadata/abacs"
  abacs: {}

vars:
  default_catalog: my_catalog
  default_schema: my_schema
  default_security_catalog: security_catalog
  default_security_schema: security_schema

targets:
  dev:
    vars:
      catalog: ${default_catalog}_dev # (3)!
      schema: ${default_schema}_dev
      security_catalog: ${default_security_catalog}_dev
      security_schema: ${default_security_schema}_dev
  prod:
    vars:
      catalog: ${default_catalog}_prod
      schema: ${default_schema}_prod
      security_catalog: ${default_security_catalog}_prod
      security_schema: ${default_security_schema}_prod
```

1. Set up directory-level configurations with `+` that can be inherited by all models and metric views in that directory.
2. This sets a tag on all models in this project.
3. You can override variables for each target.
4. Functions often live in a separate security schema/catalog and can be configured independently.

## Define Functions and ABAC Policies

Kelp now supports two additional top-level metadata objects:

- `kelp_functions` for SQL/Python Unity Catalog functions
- `kelp_abacs` for Unity Catalog ABAC policies (row filters and column masks)

You can define function bodies inline or reference external SQL/Python files with `body_path`.

Learn more in the dedicated guide: [Functions and ABAC Policies](guides/03_functions_abacs.md).

## Sync Your Pipeline Tables

If you use Databricks SDP, you can run the following command to sync tables from pipelines to metadata:

```bash
kelp sync-from-pipeline <pipeline_id>
```
If no tables are found, validate or run the pipeline and try again.

If you omit `<pipeline_id>`, Kelp attempts to detect the pipeline IDs from your local Databricks Asset Bundle state (if it exists).

```
kelp sync-from-pipeline
```

## Set Environment Variables

If you frequently reuse a specific target and project path, you can set them as environment variables to avoid passing them as arguments with every command.

```bash
export KELP_TARGET=dev
export KELP_PROJECT_PATH=/path/to/your/kelp_project.yml
```
