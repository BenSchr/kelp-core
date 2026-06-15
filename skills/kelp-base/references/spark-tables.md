# Kelp Spark Tables

This reference provides an overview of how to use Kelp parameters and decorators in conjunction with Spark tables within your Spark projects. It covers how to reference Kelp parameters in your Spark table definitions and how to use Kelp decorators to inject parameters and expectations into your Spark transformations.

## Main Kelp Assets

- **Sources**: Define the input data for your pipelines, including tables, files, or other data sources.
- **Models**: Configure the parameters passed to the declarative pipeline definitions. They can be used to create tables in your pipeline.

## Initializing Kelp in Spark Projects

To use Kelp in your Spark projects, you need to initialize Kelp and load the metadata. This typically involves referencing the path to the Kelp project file and the target environment. You can do this in your Spark application initialization code.

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
dbutils = WorkspaceClient().dbutils()

kelp_project_file = dbutils.widgets.get("kelp_project_file")
kelp_target = dbutils.widgets.get("kelp_target")

import kelp.tables as kt
_ = kt.init(kelp_project_file, target=kelp_target)
```

## Materializing Spark Tables with Kelp Parameters

You can materialize Spark tables using Kelp Model configuration parameters. This allows you to dynamically configure your Spark tables based on the metadata defined in your Kelp project.

```yaml
kelp_models:
  - name: silver_orders_refined_mat
    # ...
    materialization:
      write_mode: merge # or append or overwrite
      unique_keys:
        - order_nk
      merge_with_schema_evolution: true
      # Other options...
```

Kelp can materialize tables through `materialized` decorator or the `materialize` function. Both will use the materialization configuration defined in the model. If you can't identify a pattern in the current implementation ask the user which one to use, otherwise prefer the `materialized` decorator as it is more explicit and easier to understand.

### Using the `materialized` Decorator

```python
@kt.materialized(
    name="silver_customers_refined_mat", # detect from function name if not provided
    depends_on=["bronze_customers_mat"], # optional for runner usage
)
def silver_customers_refined(ctx: kt.MaterializedContext) -> DataFrame:
    if ctx.is_incremental():
        # Incremental transformation logic here
        df = df_incremental
    else:
        # Full transformation logic here
        df = df_full
    # Your transformation logic here
    return df

# using runner to materialize the table 
# otherwise you can also call the function directly if there is just a single table to materialize for instance

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
spark.createDataFrame(runlog_rows,schema=schema).orderBy("model").show(truncate=False)
```

This will run automatically:
- Bootstrap the target table if not exists
- Quality checks if defined
- Materialize the table with the defined write mode and conifgurations
- Update table properties, comments, tags and other metadata if defined in the model configuration
- Run maintenance operations if defined in the model configuration (vacuum, optimize)

## DQX Expectations in Spark Tables

You can also define DQX expectations to test the table, quarantine rows and log failed checks into a central table.

### Base Configuration

```yaml
kelp_project:
  # ...
  quality_config:
    dqx_monitoring_fqn: ${catalog}.kelp_bronze.dqx_monitoring # (1)!
    dqx_monitoring_enabled: true # (2)!
```
1. The fully qualified name of the DQX monitoring table where failed checks will be logged.
2. A flag to enable or disable DQX monitoring. When enabled, failed checks will be logged to the specified DQX monitoring table.

### Model configuration

You can specify DQX expectations in the model configuration. Kelp will automatically apply these expectations when you use one of the materialization decorators or functions. If you have defined quarantine expectations, Kelp will also automatically write the data to a quarantine table. If a row violates an expectation and the `spark_violation_action` is set to `drop`, the row will be dropped from the output table. If it is set to `error`, the pipeline will fail with an error. If it is set to `ignore`, the violation will be ignored and the row will be included in the output table.

```yaml
kelp_models:
  - name: silver_customers_refined_mat
    # ...
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

Use the project pattern to define the expectations in the model configuration if no pattern can be identified ask the user which one to use.
