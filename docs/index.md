
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

## Next Steps

Explore Kelp's comprehensive guides to get the most out of the framework:

| Guide | Overview |
|-------|----------|
| [Spark Declarative Pipelines (SDP)](guides/sdp.md) | Integrate Kelp with Databricks SDP using decorators and the low-level API |
| [Normal Spark (Non-SDP)](guides/normal_spark.md) | Use Kelp in standard Spark jobs with `kelp.tables`, DDL, and DQX |
| [Sync Metadata with Your Catalog](guides/catalog.md) | Keep local metadata in sync with Unity Catalog |
| [DataFrame Transformations](guides/transformations.md) | Use composable transformations like `apply_schema()` and `apply_func()` |
| [Project Configuration](guides/project_config.md) | Master `kelp_project.yml` configuration, hierarchies, and targets |
| [CLI Reference](guides/cli.md) | Command-line tools for project management and metadata sync |
| [Functions](guides/functions.md) | Define reusable SQL and Python functions in Unity Catalog |
| [ABAC Policies](guides/abacs.md) | Implement row and column access control |
| [Metric Views](guides/metric_views.md) | Define business metrics and dimensions |

## Build Transformations

Kelp provides utilities to transform data using DataFrame transformations that can be chained together:

- **Schema enforcement** - Apply and enforce schemas from metadata via `apply_schema()`
- **Function application** - Apply Unity Catalog functions via `apply_func()`

Use Kelp's composable transformations in your pipelines:

```python
from kelp.transformations import apply_schema, apply_func
import kelp.pipelines as kp

@kp.table()
def silver_customers():
    df = spark.readStream.table(kp.ref("bronze_customers"))
    
    return (
        df
        .transform(apply_schema("silver_customers"))
        .transform(apply_func(
            func_name="normalize_email",
            new_column="email_clean",
            parameters="email"
        ))
    )
```

Learn more in the [DataFrame Transformations](guides/transformations.md) guide.

## Define Functions, Metrics, and Policies

Kelp supports multiple metadata objects beyond tables:

- **`kelp_functions`** - SQL/Python Unity Catalog functions (define once, use in code and ABAC)
- **`kelp_metric_views`** - Business metrics for analytics and dashboards
- **`kelp_abacs`** - Row filters and column masking (attribute-based access control)

Example function:

```yaml
kelp_functions:
  - name: normalize_email
    language: SQL
    parameters:
      - name: email
        data_type: STRING
    returns_data_type: STRING
    body: lower(trim(email))
```

Example metric view:

```yaml
kelp_metric_views:
  - name: customer_monthly_revenue
    catalog: ${ catalog }
    schema: ${ metric_schema }
    definition:
      measures:
        - name: total_revenue
          expr: SUM(amount)
        - name: order_count
          expr: COUNT(*)
      dimensions:
        - name: order_month
          expr: DATE_TRUNC('MONTH', order_date)
      source_table: ${ catalog }.gold.orders
```

Learn more in the [Functions](guides/functions.md), [Metric Views](guides/metric_views.md), and [ABAC Policies](guides/abacs.md) guides.

## Use the Kelp CLI

The Kelp CLI provides commands for project management and metadata synchronization:

```bash
# Initialize a new project
uv run kelp init project  ./my_project

# Generate JSON schema for IDE support
uv run kelp json-schema --output kelp_json_schema.json

# Sync metadata from Databricks tables to YAML
uv run kelp catalog sync-from-catalog "catalog.schema.table" --output models/table.yml

# Validate project configuration
uv run kelp validate --target prod

```

Learn more in the [CLI Reference](guides/cli.md).

## Sync Metadata to Unity Catalog

After your pipeline creates tables, sync metadata (descriptions, tags, constraints) to the catalog:

```python
import kelp.catalog as kc

kc.init("kelp_project.yml", target="prod")

# Sync functions first (before pipeline runs)
for query in kc.sync_functions():
    spark.sql(query)

# Sync tables, metric views and ABAC policies (after pipeline runs)
for query in kc.sync_catalog():
    spark.sql(query)

```

Learn more in the [Sync Metadata with Your Catalog](guides/catalog.md) guide.

## Environment Variables

If you frequently reuse a specific target and project path, you can set them as environment variables:

```bash
export KELP_TARGET=prod
export KELP_PROJECT_FILE=/path/to/kelp_project.yml

# Now commands use these defaults
uv run kelp validate
uv run kelp catalog sync-from-catalog "catalog.schema.table"
```
