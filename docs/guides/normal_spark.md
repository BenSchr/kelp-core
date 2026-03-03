# Using Kelp with Normal Spark (Non-SDP)

This guide explains how to use Kelp in a standard Spark job (non-SDP), focusing on the `kelp.tables` API, DDL generation, schema enforcement, and applying DQX checks directly.

## Initialize Kelp

Kelp requires project initialization to load metadata:

```python
from kelp import init

# Auto-discover kelp_project.yml from cwd
ctx = init(target="dev")
```

Or pass an explicit path:

```python
ctx = init("./config/kelp_project.yml", target="prod")
```

## Table Metadata with `kelp.tables`

The `kelp.tables` API provides metadata accessors for any Spark job (no SDP dependencies).

```python
from kelp import tables as kt

# Fully qualified table name (catalog.schema.table)
print(kt.ref("customers"))

# Spark schema DDL
print(kt.schema("customers"))

# Raw schema (no constraints)
print(kt.schema_lite("customers"))

# Full CREATE TABLE DDL
print(kt.ddl("customers"))

# Column metadata
columns = kt.columns("customers")
for col in columns:
    print(col.name, col.data_type)
```

## Create Tables with DDL

Generate and execute DDL directly in Spark SQL:

```python
from kelp import tables as kt
from pyspark.sql import SparkSession

spark = SparkSession.active()

ddl = kt.ddl("customers")
if ddl:
    spark.sql(ddl)
```

## Apply Schemas with `apply_schema()`

Use `apply_schema()` for schema enforcement on DataFrames:

```python
from kelp.transformations import apply_schema

raw_df = spark.read.table("raw.customers")
clean_df = raw_df.transform(apply_schema("customers", safe_cast=True))
```

You can also pass explicit DDL:

```python
clean_df = raw_df.transform(
    apply_schema(schema="id INT, email STRING, created_at TIMESTAMP")
)
```

## Apply Unity Catalog Functions with `apply_func()`

You can apply Unity Catalog functions defined in `kelp_functions`:

```python
from kelp.transformations import apply_func

result = raw_df.transform(
    apply_func(
        func_name="normalize_email",
        new_column="email_clean",
        parameters="email",
    )
)
```

## Quarantine and Validation Names from Metadata

Kelp exposes the resolved validation and quarantine table names on the table object. This is useful when you want to write valid/invalid rows using the same naming rules as SDP:

```python
from kelp import tables as kt

kelp_table = kt.get_table("orders")

print(kelp_table.validation_table)
print(kelp_table.quarantine_table)
```

## Apply DQX Checks Directly (Non-SDP)

Use Databricks DQX directly in a normal Spark job. For details on available check functions and engine methods, see the official docs:

- https://databrickslabs.github.io/dqx/docs/guide/quality_checks_apply/
- https://databrickslabs.github.io/dqx/docs/reference/engine/
- https://databrickslabs.github.io/dqx/docs/reference/quality_checks/

```python
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from kelp import tables as kt

spark = SparkSession.active()

df = spark.read.table("analytics.orders")

# Load checks from Kelp metadata
kelp_table = kt.get_table("orders")
dqx_checks = kelp_table.dqx_checks or []

# Apply DQX checks
engine = DQEngine(WorkspaceClient())
result = engine.apply_checks_by_metadata(df, dqx_checks)

# DQX may return (df, observation)
if isinstance(result, tuple):
    checked_df = result[0]
else:
    checked_df = result

# Split valid/invalid rows
valid_df = engine.get_valid(checked_df)
invalid_df = engine.get_invalid(checked_df)
```

### Write Valid and Invalid Records

```python
valid_df.write.mode("append").saveAsTable("analytics.orders_valid")
invalid_df.write.mode("append").saveAsTable("analytics.orders_quarantine")
```

## Example: End-to-End Normal Spark Workflow

```python
from kelp import init, tables as kt
from kelp.transformations import apply_schema, apply_func
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from pyspark.sql import SparkSession

spark = SparkSession.active()
init(target="prod")

# Read raw data
raw_df = spark.read.table("raw.orders")

# Apply schema and functions
clean_df = (
    raw_df
    .transform(apply_schema("orders", safe_cast=True))
    .transform(
        apply_func(
            func_name="normalize_customer_id",
            new_column="customer_id_norm",
            parameters="customer_id",
        )
    )
)

# Apply DQX checks
engine = DQEngine(WorkspaceClient())
checks = kt.get_table("orders").dqx_checks or []
result = engine.apply_checks_by_metadata(clean_df, checks)
checked_df = result[0] if isinstance(result, tuple) else result

# Split valid/invalid
valid_df = engine.get_valid(checked_df)
invalid_df = engine.get_invalid(checked_df)

# Write outputs
valid_df.write.mode("append").saveAsTable("analytics.orders")
invalid_df.write.mode("append").saveAsTable("analytics.orders_quarantine")
```

## Best Practices

1. **Initialize once** - Call `init()` at job start to load metadata.
2. **Use `kelp.tables` for metadata** - Avoid hardcoding catalog/schema names.
3. **Apply schema early** - Enforce schema before downstream logic.
4. **Use safe_cast for untrusted data** - Avoid job failures on type issues.
5. **Apply DQX checks directly** - Use `DQEngine` in non-SDP jobs.
6. **Sync functions before jobs** - Unity Catalog functions must exist first.

## See Also

- [Spark Declarative Pipelines](sdp.md) - SDP integration and decorators
- [Sync Metadata with Your Catalog](catalog.md) - Syncing metadata to UC
- [DataFrame Transformations](transformations.md) - Transformation utilities
- [Project Configuration](project_config.md) - Configuration and targets
