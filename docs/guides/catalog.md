# Sync Metadata with Your Catalog

Kelp's catalog sync feature allows you to keep your local metadata in sync with your remote catalog. This ensures that any changes you make to your local metadata are reflected in your remote catalog, and vice versa.

Catalog sync supports:

- `kelp_models` (tables) - Schema, descriptions, tags, constraints
- `kelp_metric_views` - Metrics, dimensions, tags
- `kelp_functions` - SQL/Python function registration (must be synced separately before pipelines run)
- `kelp_abacs` - Row and column access control policies

!!! note "Functions Sync Timing"
    Functions must be synced **before** your SDP pipeline runs, since ABAC policies and user code reference them. The `sync_catalog()` command does **NOT** include functions by default. Functions are typically synced in a separate initialization step, while metadata (descriptions, tags, etc.) is synced after pipelines complete. Use `sync_functions()` to sync Unity Catalog functions.

## Sync Metadata Code

When you use Kelp's catalog sync feature, you can leverage metadata defined in your project to apply descriptions, tags, and constraints to your tables and metric views.

`sync_catalog()` generates SQL queries based on the differences between your local metadata and the remote catalog. You can review these queries before executing them to ensure they align with your expectations.

**Typical workflow:**

1. **Before pipeline runs** - Sync functions separately: `sync_functions()`
2. **Pipeline runs** - Tables and metric views are created by SDP
3. **After pipeline runs** - Sync table/metric view metadata + abac policies: `sync_catalog()`

For flexibility, Kelp does not automatically execute these queries. Instead, you can execute them manually or automate execution in a Databricks job through Spark or another orchestration tool using a SQL endpoint.

```python
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
import kelp.catalog as kc

w = WorkspaceClient()
dbutils = WorkspaceClient().dbutils()
spark = SparkSession.active()

kelp_project_file = dbutils.widgets.get("kelp_project_file")
kelp_target = dbutils.widgets.get("kelp_target")

kc.init(kelp_project_file, target=kelp_target)

# Sync tables and metric views after pipeline creates them
for query in kc.sync_catalog():
    try:
        print(f"Executing query: {query}")
        spark.sql(query)
    except Exception as e:
        print(f"Error executing query: {query}\nException: {e}")
```

### Syncing Specific Metadata Types

Kelp provides specialized sync functions for different metadata types:

```python
# Sync functions BEFORE pipeline runs (required for ABAC and code references)
for query in kc.sync_functions():
    spark.sql(query)

# Sync only tables (typically after pipeline creates them)
for query in kc.sync_tables():
    spark.sql(query)

# Sync only metric views
for query in kc.sync_metric_views():
    spark.sql(query)

# Sync only ABAC policies (after functions are synced)
for query in kc.sync_abac_policies():
    spark.sql(query)

# Sync only a subset of metadata
for query in kc.sync_tables(table_names=["my_table1", "my_table2"]):
    spark.sql(query)

for query in kc.sync_metric_views(view_names=["my_metric_view1"]):
    spark.sql(query)

for query in kc.sync_functions(function_names=["my_function1"]):
    spark.sql(query)

for query in kc.sync_abac_policies(policy_names=["my_policy1"]):
    spark.sql(query)
```

## Complete Sync Workflow

Here's a typical end-to-end workflow for syncing metadata:

```python
import kelp.catalog as kc
from pyspark.sql import SparkSession

spark = SparkSession.active()
kc.init("kelp_project.yml", target="prod")

# Step 1: Sync functions (run BEFORE pipeline, required for ABAC and code)
print("Syncing functions...")
for query in kc.sync_functions():
    spark.sql(query)

# Step 2: Run your SDP pipeline here
# (tables and metric views are created by the pipeline)

# Step 3: Sync table and metric view metadata (run AFTER pipeline completes)
print("Syncing table and metric view metadata...")
for query in kc.sync_tables():
    spark.sql(query)

for query in kc.sync_metric_views():
    spark.sql(query)

# Step 4: Sync ABAC policies (after functions and tables exist)
print("Syncing ABAC policies...")
for query in kc.sync_abac_policies():
    spark.sql(query)

print("✓ All metadata synced successfully!")
```

### Execution Order

When using `sync_catalog()`, Kelp applies objects in this order:

1. Tables
2. Metric views
3. ABAC policies (included in `sync_catalog()`)

**Functions are NOT included** in `sync_catalog()` by default. Sync functions separately before your pipeline runs:

```python
# Before pipeline runs - sync functions first
for query in kc.sync_functions():
    spark.sql(query)

# After pipeline runs - sync metadata
for query in kc.sync_catalog():
    spark.sql(query)
```

When functions are synced, they are applied immediately, making them available for ABAC policies and Python code that references them.


## Configuration for Metadata Sync

Kelp manages how metadata is applied to your catalog through configurable modes and managed keys. You can specify these settings in your `kelp_project.yml` file under the `remote_catalog_config` section.

The following are the available options:

**append**

Kelp adds or updates tags and properties but does not remove existing ones when they are missing locally but still defined in the remote catalog.

**replace** (only for tags)

Kelp replaces all existing tags with those defined in your local metadata.

**managed**

Kelp manages only the tags or properties specified in `managed_table_tags`, `managed_column_tags`, or `managed_table_properties`. It adds, updates, or removes these specific tags or properties to match your local metadata, while leaving all other tags and properties unchanged.

```yaml
# kelp_project.yml
kelp_project:
  # ...
  remote_catalog_config:
    table_tag_mode: "replace"
    managed_table_tags: []
    column_tag_mode: "replace"
    managed_column_tags: []
    table_property_mode: "append"
    managed_table_properties: []
```
