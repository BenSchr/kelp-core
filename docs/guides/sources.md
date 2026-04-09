# Data Sources

This guide explains how to define and use Kelp data sources. Sources provide a centralized way to manage and reference data locations (volumes, tables, raw paths) across your pipelines.

## Overview

Kelp sources simplify data access by:

- **Centralizing path configuration** - Define source locations once in YAML
- **Supporting multiple source types** - volumes, tables, and raw paths
- **Enabling path templating** - Use variables in source paths
- **Managing options** - Store format, headers, and other source-specific settings

## Source Types

Kelp supports three types of data sources:

### 1. Volume Sources

Reference data stored in Databricks Unity Catalog volumes.

You can define volume sources in two ways:

**Option A: Using catalog, schema, and volume_name (recommended)**

This approach automatically constructs the volume path as `/Volumes/catalog/schema/volume_name`:

```yaml
kelp_sources:
  - name: landing_customers
    source_type: volume
    catalog: my_catalog
    schema: landing
    volume_name: customers/
    description: "Raw customer data from external system"
```

**Option B: Using explicit path**

This approach gives you full control over the path:

```yaml
kelp_sources:
  - name: landing_customers
    source_type: volume
    path: /Volumes/my_catalog/landing/customers/
    description: "Raw customer data from external system"
```

Both approaches work identically in your code:

### 2. Table Sources

Reference existing tables in Unity Catalog.

```yaml
kelp_sources:
  - name: reference_countries
    source_type: table
    catalog: my_catalog
    schema: reference_data
    table_name: countries
    description: "Reference table of valid countries"
```

```python
# Get the fully qualified table name
table_fqn = kp.source("reference_countries")
# Returns: my_catalog.reference_data.countries

df = spark.read.table(table_fqn)
```

### 3. Raw Path Sources

Reference arbitrary file paths (S3, ADLS, etc.).

```yaml
kelp_sources:
  - name: external_api_cache
    source_type: raw_path
    path: s3://my-bucket/cache/api-responses/
    description: "Cached API responses from external service"
```


## Configuration

### Project Setup

Add sources configuration to your `kelp_project.yml`:

```yaml
kelp_project:
  sources_path: "./kelp_metadata/sources"
  sources: {}
  
  # Other configurations...
```

### Directory Structure

Organize your sources in the `kelp_metadata/sources` directory:

```
kelp_metadata/
├── sources/
│   ├── sources.yml
│   ├── landing_sources.yml
│   └── reference_sources.yml
├── models/
├── metrics/
└── functions/
```

### Defining Sources

Create YAML files in your `sources_path` directory:

```yaml
kelp_sources:
  - name: raw_events
    source_type: volume
    catalog: ${ catalog }
    schema: ${ landing_schema }
    volume_name: raw_data/events
    description: "Event stream from analytics provider"
    options:
      cloudFiles.format: parquet

  - name: reference_categories
    source_type: table
    catalog: ${ catalog }
    schema: reference
    table_name: product_categories
    description: "Product categories reference table"

vars:
  catalog: my_catalog
  landing_schema: landing_zone
```


## Source Options

All source types support an `options` dictionary for source-specific settings:

```yaml
kelp_sources:
  - name: landing_json
    source_type: volume
    path: /Volumes/my_catalog/landing/api_data/
    description: "API responses in JSON format"
    options:
      cloudFiles.format: json
      cloudFiles.schemaLocation: /Volumes/checkpoints/api_data/
      multiLine: "true"
```

Access options in your pipeline:

```python
import kelp.pipelines as kp

path = kp.source("landing_json")
options = kp.source_options("landing_json")

df = spark.readStream.format("cloudFiles").options(**options).load(path)
```

## Usage in Pipelines

### PySpark DataFrames API

Use sources in any PySpark job:

```python
import kelp.tables as kt

path = kt.source("raw_events")
options = kt.source_options("raw_events")

df = spark.readStream.format("cloudFiles").options(**options).load(path)
```

### SDP Decorators

Use sources in Spark Declarative Pipeline transformations:

```python
import kelp.pipelines as kp
from pyspark import pipelines as dp

@dp.streaming_table
def bronze_events():
    path = kp.source("raw_events")
    options = kp.source_options("raw_events")
    
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**options)
        .load(path)
    )
```

### With CloudFiles

CloudFiles format commonly used with Volumes:

```python
import kelp.pipelines as kp

@kp.streaming_table()
def bronze_customer_data():
    path = kp.source("landing_customers")
    options = kp.source_options("landing_customers")
    
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**options)
        .load(path)
    )
```

### Mixed Sources

Combine multiple sources in a single transformation:

```python
import kelp.pipelines as kp

@kp.streaming_table()
def enriched_customers():
    customers_df = spark.readStream.table(
        kp.source("raw_customers")
    )
    
    categories_df = spark.read.table(
        kp.source("reference_categories")
    )
    
    return customers_df.join(categories_df, on="category_id")
```

## Validation

Validate sources configuration with the CLI:

```bash
uv run kelp validate

# Output example:
# ✓ Configuration is valid!
# ...
# Relative sources path: ./kelp_metadata/sources
# Sources found: 5
```

## Reference
- [Source Models](../reference/sources.md)

## See Also

- [Project Configuration](project_config.md)
- [Tables Guide](../reference/tables.md)
- [Pipelines API](../reference/pipelines.md)
