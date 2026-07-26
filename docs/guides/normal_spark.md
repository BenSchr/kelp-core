# Using Kelp with Normal Spark (Non-SDP)

This guide explains how to use Kelp in a standard Spark job (non-SDP), focusing on the `kelp.tables` API, DDL generation, schema enforcement, and applying DQX checks directly.

## Initialize Kelp

Kelp requires project initialization to load metadata:

```python
import kelp.tables as kt

# Auto-discover kelp_project.yml from cwd
ctx = kt.init(target="dev")
```

Or pass an explicit path:

```python
ctx = kt.init("./config/kelp_project.yml", target="prod")
```

## Table Metadata with `kelp.tables`

The `kelp.tables` API provides metadata accessors for any Spark job (no SDP dependencies).

```python
import kelp.tables as kt

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
import kelp.tables as kt
from pyspark.sql import SparkSession

spark = SparkSession.active()

ddl = kt.ddl("customers")
if ddl:
    spark.sql(ddl)
```

## Materialize DataFrames (Decorator + Function)

Kelp now provides two non-SDP materialization entry points in `kelp.tables`:

- `@kt.materialized(...)` - decorate a function that returns a DataFrame.
- `kt.materialize(...)` - materialize an already-built DataFrame directly.

Both paths use Delta Lake writes and can resolve model configuration from metadata.

### Option A: Decorator (`@materialized`)

```python
import kelp.tables as kt
from pyspark.sql import SparkSession

spark = SparkSession.active()
kt.init(target="prod")


@kt.materialized(name="orders")
def build_orders():
    return spark.read.table("raw.orders")


@kt.materialized  # Name will be inferred from function name
def cusomters():
    return spark.read.table("raw.customers")


# Runs the function and materializes the returned DataFrame
df = build_orders()
```

With explicit override config:

```python
import kelp.tables as kt


@kt.materialized(
    name="orders",
    config={
        "mode": "append",
        "options": {"mergeSchema": "true"},
    },
)
def build_orders_incremental():
    return spark.read.table("staging.orders")
```

### Option B: Direct Function (`materialize`)

```python
import kelp.tables as kt

df = spark.read.table("staging.orders")

kt.materialize(
    dataframe=df,
    name="orders",  # unqualified: a kelp model name
    config=kt.OverwriteConfig(options={"overwriteSchema": "true"}),
)
```

If `config` is omitted, Kelp will use metadata materialization settings when available;
otherwise it falls back to append behavior.

Materialization does not require a kelp project - but the target must then be named in
full. An **unqualified** `name` is resolved against kelp metadata and raises `LookupError`
when no model matches it, so a typo or a renamed model cannot silently create a new table
in the session's default catalog and schema. A **qualified** `name`
(`catalog.schema.table`) is written exactly as given, with all metadata-driven steps -
target FQN resolution, `CREATE TABLE` DDL, DQX checks and catalog sync - skipped.

### Write modes

Materialization config is a discriminated union on `mode`, so each mode only accepts the
settings that apply to it. See [Materialization](materialization.md) for the full reference.

| Mode | Purpose |
|------|---------|
| `append` | Add rows to the target |
| `overwrite` | Replace the target contents (optionally scoped with `replace_where`) |
| `merge` | Merge rows by `keys`, one current version per key (SCD type 1) |
| `scd2` | Track full row history per key with `valid_from`/`valid_to` intervals |

```yaml
kelp_models:
  - name: orders
    materialization:
      mode: merge
      keys: [order_id]
      sequence_by: [updated_at]
      columns:
        exclude: [_op]        # keep CDC bookkeeping out of the target
      when_deleted: "_op = 'D'"
```

### Use context for incremental logic

Use the materialization context to implement incremental logic based on the existing data in the target table. You can access the current model's metadata and check if the materialization is running in incremental mode:

```python
@kt.materialized(name="silver_customers")
def silver_customers_refined(ctx: kt.MaterializedContext) -> DataFrame:
    source_df = spark.read.table(kt.ref("bronze_customers"))

    if ctx.is_incremental():
        max_customer_ts = (
            spark.table(ctx.this)
            .agg(f.max("customer_updated_at").alias("max_customer_updated_at"))
            .collect()[0]["max_customer_updated_at"]
        )
        if max_customer_ts is not None:
            source_df = source_df.filter(f.col("customer_updated_at") > f.lit(max_customer_ts))
```

### Quality Checks and Validation with DQX

Calling `materialize()` or using the `@materialized` decorator will automatically apply any DQX checks defined in the model metadata. For example, if you have the following checks defined in your `kelp_project.yml`:

```yaml
kelp_models:
  - name: customers
    quality:
      engine: dqx
      spark_violation_action: drop
      spark_quarantine: true
      checks:
        - check:
            function: is_not_equal_to
            arguments:
              column: user_id
              value: "'U999'"
```

Kelp will run DQX checks on the DataFrame returned by the `customers` function. Invalid rows will be quarantined to the specified quarantine table, and valid rows will be materialized to the target table. You can customize the behavior using the `spark_violation_action` and `spark_quarantine` settings in your model metadata.

```python
@kt.materialized(name="customers")
def customers(ctx: kt.MaterializedContext) -> DataFrame:
    df = spark.read.table("raw.customers")
    # ...
```

### Monitoring Quality Checks

Monitoring the quality checks can get configured in the `kelp_project.yml`:

```yaml
kelp_project:
    quality_config:
      dqx_monitoring_fqn: ${catalog}.kelp_bronze.dqx_monitoring
      dqx_monitoring_enabled: true
```

Kelp will bootstrap the specified monitoring table if it doesn't exist and log failed DQX check results for each materialization. You can query this table to analyze check outcomes, failure rates, and trends over time.
You can pre-define the monitoring table with the following schema to configure your desired tableproperties.

```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.<table> (
    target_table STRING,
    quarantine_table STRING,
    severity STRING,
    name STRING,
    message STRING,
    columns ARRAY<STRING>,
    issue_count BIGINT,
    filter STRING,
    function STRING,
    run_time TIMESTAMP,
    run_id STRING,
    user_metadata MAP<STRING,STRING>,
    rule_fingerprint STRING,
    rule_set_fingerprint STRING,
    skipped BOOLEAN
)
```

### Orchestrate materialization
You can orchestrate materialization in a normal Spark job by calling the decorated functions in the desired order. For example:

```python
def main():
    customers_df = customers()  # This will materialize the 'customers' table
    orders_df = build_orders()  # This will materialize the 'orders' table
```

Alternatively, you can use the runner API to execute the materialization with more control.
To run specify the dependencies and order of materialization, you can use the `depends_on` parameter in the `@materialized` decorator. For example:

```python
@kt.materialized(name="orders", depends_on=["raw_orders"])
def build_orders():
    return spark.read.table("raw_orders")
```

```python
from kelp.tables import Runner

runner = Runner()
planned_models = runner.plan_all()
print("📋 Runner execution plan:")
for idx, model_name in enumerate(planned_models, start=1):
    print(f"  {idx}. {model_name}")

runner.run_all()

runlog_rows = [
    {
        "model": e.model,
        "status": e.status,
        "started_at": str(e.started_at),
        "finished_at": str(e.finished_at),
        "duration_seconds": round(e.duration_seconds, 3),
        "error": e.error,
    }
    for e in runner.runlog.entries
]

schema = "model STRING, status STRING, started_at STRING, finished_at STRING, duration_seconds DOUBLE, error STRING"

print("\n🧾 Runner runlog")
spark.createDataFrame(runlog_rows, schema=schema).orderBy("model").show(truncate=False)
```

Models that do not depend on each other can run concurrently. This is off by default:

```python
runner.run_all(parallel=True, max_workers=4)
```

Execution stays level by level - every model in a dependency level finishes before the
next level starts - so `depends_on` is always respected. `runner.registry.levels()` shows
the grouping, and `Runner(registry=...)` takes an isolated `ModelRegistry` when one
process needs more than one set of models.

PySpark's active session is tracked per thread, so the runner resolves it once when a run
starts and hands it to each model rather than letting worker threads look for it. Pass one
explicitly with `Runner(spark=spark)` when the session the models should use is not the
one active where `run_all()` is called.

## Materialization Options

The steps kelp runs around a write - quality checks, catalog sync, `OPTIMIZE`, `VACUUM` -
are set project-wide and overridden per call. Unset switches fall back to the project
value:

```yaml
kelp_project:
  materialization_options:
    apply_vacuum: false      # e.g. never VACUUM in dev
```

```python
kt.materialize(dataframe=df, name="orders", options={"apply_optimize": False})


@kt.materialized(name="orders", options=kt.MaterializationOptions(sync_metadata=False))
def build_orders():
    return spark.read.table("staging.orders")
```

| Option | Default | Meaning |
|--------|---------|---------|
| `apply_quality_checks` | `true` | Apply DQX checks declared in model metadata |
| `sync_metadata` | `true` | Sync catalog metadata afterwards (requires a model, Databricks only) |
| `apply_optimize` | `true` | Run `OPTIMIZE` after the write |
| `apply_vacuum` | `true` | Run `VACUUM` after the write |
| `vacuum_lite` | `true` | Use `VACUUM ... LITE` (Databricks only) |

## Apply Schemas with `apply_schema()`

Use `apply_schema()` for schema enforcement on DataFrames:

```python
from kelp.transformations import apply_schema

raw_df = spark.read.table("raw.customers")
clean_df = raw_df.transform(apply_schema("customers", safe_cast=True))
```

You can also pass explicit DDL:

```python
clean_df = raw_df.transform(apply_schema(schema="id INT, email STRING, created_at TIMESTAMP"))
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

## See Also

- [Spark Declarative Pipelines](sdp.md) - SDP integration and decorators
- [Sync Metadata with Your Catalog](catalog.md) - Syncing metadata to UC
- [DataFrame Transformations](transformations.md) - Transformation utilities
- [Project Configuration](project_config.md) - Configuration and targets
