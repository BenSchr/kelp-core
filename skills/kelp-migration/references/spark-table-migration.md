# Spark Tabl Migration

This reference provides guidance on how to migrate existing Spark tables to use Kelp metadata. It covers how to refactor hardcoded catalog/schema references, extract metadata from code to YAML, and convert manual expectations to Kelp quality checks.

## Source Example

```python

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")

# Create table if not exists with hardcoded catalog and schema
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.my_table (
    id INT,
    name STRING,
    value DOUBLE
)
TABLEPROPERTIES (
 'key' = 'value'
)""") # ... other table properties, configurations and metadata

# Read and write to the table with hardcoded catalog and schema
df = spark.read.table(f"{source_catalog}.{source_schema}.source_table")

# Quality checks with manual assertions etc.
# Tranformations
# ...

df.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.my_table")
# or 
from delta.tables import DeltaTable
delta_table = DeltaTable.forName(spark, f"{catalog}.{schema}.my_table")
delta_table.alias("target").merge(
    df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() ## ... other transformations and operations

spark.sql(f"Optimize {catalog}.{schema}.my_table") # maintenance operations
spark.sql(f"Vacuum {catalog}.{schema}.my_table") # maintenance operations
```
## Refactored with Kelp Metadata

Follow the base skill instructions on how to setup the Kelp project and initialize Kelp in your Spark notebook or job. Then you can refactor the above code to use Kelp metadata and patterns as follows:

```python
import kelp.tables as kt

kelp_project_file = dbutils.widgets.get("kelp_project_file")
kelp_target = dbutils.widgets.get("kelp_target")

# init kelp
_ = kt.init(kelp_project_file, target=kelp_target)

@kt.materialized
def my_table(ctx: kt.MaterializedContext) -> DataFrame:

    source_table = kp.source("source_table") # auto resolves to source_catalog.source_schema.source_table or volume for external data
    other_upstream_table = kp.ref("other_upstream_table") # auto resolves to catalog.schema.other_upstream_table which is also managed in kelp_models 

    if ctx.is_incremental():
        # Incremental transformation logic here
        df = df_incremental
    else:
        # Full transformation logic here
        df = df_full
    # Your transformation logic here
    return df

# Use runner or call the function directly to materialize the table
```

## Kelp Model Metadata Example
Extract metadata to YAML:
- One file per model.
- Extract the table-schema to columns detect types as lowercase and also comments or any other relevant metadata. - Extract expectations to the quality section 
- Extract any other relevant metadata like clustering, partitioning, table properties, comments, etc.
- Extract materialization parameters like write mode, unique keys for merge, etc.

**Important**:Only add provided metadata. Don't add any additinal fields not in source except the user intructed to do so.

```yaml
kelp_models:
  - name: my_table
    description: "Description of my_table"
    columns:
      - name: id
        type: string
        comment: "The unique identifier"
      # ...
    cluster_by: ["<clustering-column>", "<clustering-column>"]
    table_properties:
      key: value
    materialization:
      write_mode: merge # or append or overwrite
      unique_keys:
        - order_nk
      merge_with_schema_evolution: true
    quality:
        engine: dqx
        spark_violation_action: drop # or error, or ignore
        spark_quarantine: true
        checks:
        - check:
            function: is_not_equal_to
            arguments:
                column: user_id
                value: "'U999'"
```



