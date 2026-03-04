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

## Variable Interpolation

Sources support Jinja2 variable interpolation using `${ variable_name }`:

```yaml
kelp_sources:
  - name: landing
    source_type: volume
    catalog: ${ catalog }
    schema: ${ landing_schema }
    volume_name: raw
    description: "Landing zone for raw data"

  - name: legacy_landing
    source_type: volume
    path: /Volumes/${ catalog }/${ landing_schema }/raw/
    description: "Alternative path-based definition"

vars:
  catalog: my_catalog
  landing_schema: landing_zone
```
  landing_schema: landing_zone
```

Variables can be overridden per target:

```yaml
targets:
  dev:
    vars:
      catalog: my_catalog_dev
      landing_schema: dev_landing

  prod:
    vars:
      catalog: my_catalog_prod
      landing_schema: prod_landing
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

## Best Practices

### 1. Centralize Data Paths

Instead of hardcoding paths in transformations:

```python
# ❌ Avoid
path = "/Volumes/my_catalog/landing/customers/"
```

Use sources:

```python
# ✅ Preferred
path = kp.source("landing_customers")
```

### 2. Use Descriptive Names

Name sources to clearly indicate their purpose and format:

```yaml
# ✅ Good
- name: landing_json_api_responses
  source_type: volume
  path: /Volumes/my_catalog/api_data/responses/

# ❌ Vague
- name: data_location
  source_type: volume
  path: /Volumes/my_catalog/api_data/responses/
```

### 3. Include Options in Source Definition

Keep format and reader options with the source configuration:

```yaml
# ✅ Centralized
- name: landing_multiline_json
  source_type: volume
  path: /Volumes/my_catalog/raw/
  options:
    cloudFiles.format: json
    multiLine: "true"

# ❌ Spread across code
path = kp.source("landing_multiline_json")
# Options defined separately in transformation code
```

### 4. Document Source Purpose

Add clear descriptions to help team members understand each source:

```yaml
- name: landing_events
  source_type: volume
  path: /Volumes/${ catalog }/events/
  description: "Event stream from product analytics"
```

## Examples

### Complete Volume-based ETL

Configuration:

```yaml
# kelp_metadata/sources/sources.yml
kelp_sources:
  - name: raw_events
    source_type: volume
    catalog: ${ catalog }
    schema: ${ landing_schema }
    volume_name: events
    description: "Raw event data from analytics platform"
    options:
      cloudFiles.format: parquet

  - name: reference_users
    source_type: table
    catalog: ${ catalog }
    schema: reference
    table_name: dim_users
    description: "User reference dimension"

vars:
  catalog: my_catalog
  landing_schema: landing_zone
```

Pipeline:

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

@dp.streaming_table
def silver_enriched_events():
    events = spark.readStream.table(kp.ref("bronze_events"))
    users = spark.read.table(kp.source("reference_users"))
    
    return events.join(
        users,
        on="user_id"
    )
```

## API Reference

### `kp.source(name: str) -> str`

Get the path for a data source.

**Arguments:**
- `name` - Source name as defined in YAML

**Returns:**
- For table sources: fully qualified name (catalog.schema.table_name)
- For volume/raw_path sources: the path string

**Raises:**
- `KeyError` if source not found in catalog
- `ValueError` if source configuration is incomplete

### `kp.source_options(name: str) -> dict`

Get the options dictionary for a data source.

**Arguments:**
- `name` - Source name as defined in YAML

**Returns:**
- Dictionary of source-specific options (empty dict if no options defined)

**Raises:**
- `KeyError` if source not found in catalog

## See Also

- [Project Configuration](project_config.md)
- [Tables Guide](../reference/tables.md)
- [Pipelines API](../reference/pipelines.md)
