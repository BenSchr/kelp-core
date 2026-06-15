# Kelp Project Overview

## Goal

The Kelp project aims to provide a unified framework for managing Spark projects, with a focus on metadata management. It is designed to simplify the development and maintenance of Spark applications by providing tools and best practices for working with Spark tables, views, and other assets.

## Key Features
- **Metadata Management**: Kelp provides a structured way to manage metadata for Spark projects, including tables, views, and metric views.
- **Integration with Spark**: Kelp is designed to work seamlessly with Spark, allowing developers to easily create and modify Spark tables.
- **Support for Spark Declarative Pipelines**: Kelp supports the use of Spark Declarative Pipelines, enabling developers to inject parameters from metadata into the declarative definitions.
- **Sync with Unity Catalog**: Kelp can sync metadata with Unity Catalog, ensuring that all assets are properly documented with comments and tags.

## Project Structure

```
project-root/**
- kelp_project.yml
- kelp_metadata/
    - models/
    - sources/
    - metrics/
    - ...
```

## Kelp-Project.yml

The `kelp_project.yml` file is the central configuration file for a Kelp project. It defines the structure of the project, including the location of metadata files and any project-specific settings.

```yaml
kelp_project:
  models_path: "./kelp_metadata/models"

  # Models configurations get inherited by folder structure. `+` marks properties without marks folders.
  models:
    +catalog: ${ catalog }
    bronze:
      +schema: kelp_bronze
    silver:
      +schema: kelp_silver
    gold:
      +schema: kelp_gold
    +tags:
      kelp_managed: ""
      domain: "sales"
      owner: "analytics-team"

  # Metric Views configuration
  metrics_path: "./kelp_metadata/metrics"
  metric_views:
    +catalog: ${ catalog }
    +schema: kelp_gold
    +tags:
      kelp_managed: ""
      domain: "sales"
      owner: "analytics-team"

  # Functions configuration
  functions_path: "./kelp_metadata/functions"
  functions:
    +catalog: ${ catalog }
    +schema: ${ function_schema }
    abac:
      +schema: ${ abac_schema }

  # ABAC configuration
  abacs_path: "./kelp_metadata/abacs"
  abacs: {}

  # Sources configuration
  sources_path: "./kelp_metadata/sources"
  sources:
    +catalog: ${catalog}
    +schema: ${landing_schema}

  # Governance policies configuration
  policies_path: "./kelp_metadata/policies"
  policy_config:
    enabled: false

vars:
  catalog: kelp_catalog
  landing_schema: kelp_bronze
  function_schema: kelp_funcs
  abac_schema: kelp_abac

## Target specific variables can be defined under the `targets` section. This allows for different configurations based on the deployment environment (e.g., development, production).
targets:
  dev:
    vars:
      catalog: ${catalog}

  prod:
    vars:
      catalog: ${catalog}
```

## Models

Models represent tables, views, streaming tables or materialized views in Spark. They are defined in the `kelp_metadata/models` directory and can be organized into subdirectories for better structure. Each model is defined in a YAML file that specifies its properties, such as the schema definition, tags, and comments.

Following are the mainly used properties that can be defined for a model:
```yaml
kelp_models:
- name: bronze_customers
  description: This is the bronze customers model
  constraints:
      - name: pk_bronze_customers
        type: primary_key
        columns:
          - customer_id
  cluster_by:
  - customer_id
  columns:
  - name: customer_id
    data_type: int
    description: The unique identifier for a customer
    tags:
      data_classification: internal
  - name: ...
  tags:
    example-tag: ""
```

## Sources

Sources represent the input data for the Spark project. They are defined in the `kelp_metadata/sources` directory and can include properties such as the format of the source data.

```yaml
kelp_sources:
  - name: landing_volume_customers
    source_type: volume
    volume_name: landing_volume/customers
    description: "Raw customer data in Parquet format from the landing volume"
    options:
      cloudFiles.format: parquet
  - name: test_table_source
    table_name: test_table_source
```

## Metrics

Metrics represent Metric Views that can be defined in the `kelp_metadata/metrics` directory.

```yaml
kelp_metric_views:
  - name: revenue_metrics
    description: Key revenue and order metrics for business analytics and dashboards2
    definition:
      comment: Key revenue and order metrics for business analytics and dashboards2
      version: 1.1
      source: ${ catalog }.kelp_gold.gold_orders_customers
      dimensions:
        - name: order_state
          expr: order_state
          comment: order_state indicates the lifecycle state of an order (e.g., 'completed')
          tags:
            hello: abcd
        - name: ...
      measures:
        - name: total_revenue
          expr: SUM(total_price)
          comment: Total revenue (sum of item prices) for the grouping
        - name: ...
```
