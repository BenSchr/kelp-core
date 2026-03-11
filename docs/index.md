
```
██╗  ██╗███████╗██╗     ██████╗
██║ ██╔╝██╔════╝██║     ██╔══██╗
█████╔╝ █████╗  ██║     ██████╔╝
██╔═██╗ ██╔══╝  ██║     ██╔═══╝
██║  ██╗███████╗███████╗██║
╚═╝  ╚═╝╚══════╝╚══════╝╚═╝
Metadata Toolkit for Databricks Spark and Declarative Pipelines
```
Kelp is a powerful framework designed to simplify the management of data pipelines, quality checks, and table configurations. Follow the instructions below to set up Kelp in your environment and start building robust data solutions.

Documentation: [https://benschr.github.io/kelp-core/](https://benschr.github.io/kelp-core/)

## Why Kelp?
Kelp provides a metadata and transformation layer for Databricks Spark and Spark Declarative Pipelines (SDP). It lets you define data models, quality checks, and transformations in structured YAML while offering Python utilities for advanced logic. With Kelp you can:

### Metadata management
- Define models, metric views, functions, ABAC policies, and data sources in readable, maintainable YAML
- Enforce metadata governance with declarative policies (required descriptions, tags, and allowed/forbidden columns)
- Keep local metadata synchronized with Unity Catalog for improved governance and discoverability
- Centralize data location configuration (volumes, tables, raw paths) and reference them from any pipeline
- Use variables and targets for environment-specific configuration
- Inherit directory-level settings and tags across models

### Spark Declarative Pipelines (SDP)
- Inject metadata into SDP decorators with minimal boilerplate
- Optionally use DQX quality checks instead of SDP expectations
- Apply a quarantine pattern for validation failures
- Sync metadata to Unity Catalog after pipeline runs
- Easily inject catalog and schema names for tables and functions
- Sync descriptions and tags from metadata to tables and columns without requiring the Spark schema to match exactly
- Use a low-level API (no decorators) to stay robust against SDP syntax or feature changes

### Extra utilities
- Composable DataFrame transformations for schema enforcement and function application
- CLI tools for project management and metadata synchronization
- Metric views for defining business metrics and dimensions in metadata
- ABAC policies for row- and column-level access control defined in metadata and applied in code and the catalog
- Reusable function definitions in metadata that can be referenced from code and ABAC policies for consistent logic and easier maintenance

## Installation

To install Kelp, you can use `uv`, `pip`, or the package manager of your choice. Below are the commands for both methods:

```
uv add kelp-core==0.0.4
```

```
pip install kelp-core==0.0.4
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
  policies/**/*.yml
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
    policies/
      governance.yml
    sources/
      sources.yml
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

  sources_path: "./kelp_metadata/sources"
  sources: {}

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

> ⚠ Some links in the table below may not work in repository preview contexts.
> Please use the docs website for reliable navigation: https://benschr.github.io/kelp-core/

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
| [Governance Policies](guides/policies.md) | Enforce metadata quality rules for models and columns |
| [Metric Views](guides/metric_views.md) | Define business metrics and dimensions |
| [Sources](guides/sources.md) | Centralize data source configuration and reference in pipelines |

## JsonSchema for IDE Support

Kelp can generate a JsonSchema file from your `kelp_project.yml` configuration. This schema can be used to enable autocompletion and validation in compatible IDEs when editing your YAML files.
To generate the JsonSchema and configure VSCode settings, run the following command:

```
kelp json-schema --vscode
```
This command will create a `kelp_json_schema.json` file in your project directory and update your VSCode settings to associate this schema with your Kelp YAML files.

You can also generate the JsonSchema without updating VSCode settings:

```
kelp json-schema --output kelp_json_schema.json
```

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
- **`kelp_policies`** - Metadata governance rules validated locally during init and via CLI

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

Learn more in the [Functions](guides/functions.md), [Metric Views](guides/metric_views.md), [ABAC Policies](guides/abacs.md), and [Governance Policies](guides/policies.md) guides.

## Metadata Governance Policies

Use metadata policies to keep model definitions consistent and audit-friendly across teams.

```yaml
kelp_project:
  policy_config:
    enabled: false  # (1)!
    fast_exit: false

kelp_policies:
  - name: required_metadata
    applies_to: "models/**"
    model:
      require_description: true
      require_tags: [owner, domain]
      severity: error
```
1. Enabling this flag will run policy checks on each metadata load, for most use cases it's recommended to run policies via the CLI instead of on every load for better performance.

Run policy checks directly from the CLI:

```bash
uv run kelp check-policies
uv run kelp check-policies --fast-exit
```

See the full policy options in the [Governance Policies guide](guides/policies.md).

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
