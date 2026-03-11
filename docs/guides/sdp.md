# Spark Declarative Pipelines (SDP)

This guide provides an overview of how to integrate Kelp with Databricks Spark Declarative Pipelines (SDP). It covers Kelp initialization in an SDP environment, using decorators to define pipeline components, implementing quality checks, and utilizing the low-level API for more control over your pipelines.


## Initialize Kelp in SDP

### Spark Configurations

Kelp autodetects project and target configurations from Spark configurations. You can set these in your pipeline configurations.


```yaml
# databricks.yml
# ...
variables:
    kelp_project_file:
        description: Path to kelp project file
        default: ${workspace.file_path}/src/kelp_project_etl/kelp_project.yml
# ...
```

```yaml
# pipeline.yml
resources:
  pipelines:
    kelp_sample_sdp:
      name: kelp_sample_sdp
      # ...
      configuration:
        kelp.project_file: ${var.kelp_project_file}
        kelp.target: ${bundle.target}
      environment:
        dependencies:
          - kelp-core==0.0.4
          - databricks-labs-dqx
      # ...
```

### Explicit initialization in code

You can also explicitly initialize Kelp in your code, for example in a Python file that is part of your pipeline. This can be useful if you want more control over the initialization process.

Note that you must call `init` in each file to guarantee that Kelp is properly initialized in all situations (e.g., when partially running a pipeline).

```python
import kelp.pipelines as kp

kp.init("<path to kelp_project.yml>", target="<target>")

@kp.table
def my_table():
    # ...
```


## Use Kelp Decorators in SDP

Kelp provides decorators that wrap around the built-in SDP decorators to auto-inject the parameters defined in your metadata files.

Function names and decorator arguments are used to find the corresponding model definitions in your `kelp_metadata/models` directory. This keeps your pipeline code clean and focused on the logic, while Kelp handles configuration and metadata management.

```yaml
kelp_models:
  - name: my_table
    #... 

```

```python
import kelp.pipelines as kp

@kp.table # (1)!
def my_table():
    # ...

@kp.table(name="my_table") # (2)!
def different_name():
    # ...

@kp.materialized_view(name="my_mv") # (3)!
def my_mv():
  # ...
```

1. This will use the function name to search for the corresponding model definition in your `kelp_metadata/models` directory.

2. This will use the provided name to search for the corresponding model definition in your `kelp_metadata/models` directory.

3. `@kp.materialized_view` uses the same parameter style as `@kp.table`, but acts as a pass-through wrapper around SDP `@dp.materialized_view` without expectation or quarantine handling.


You can exclude parameters from being auto-injected by using `exclude_params`. This gives you more control over the parameters passed to your pipeline components. For example, you can exclude the `schema` parameter to prevent SDP from setting the Spark Schema.

```python
import kelp.pipelines as kp

@kp.table(exclude_params=["schema"])
def my_table():
    # ...
```

## Pass Parameters Without Decorators

Since Spark Declarative Pipelines (SDP) is under rapid development and may change syntax or add extra parameters to decorated functions, Kelp provides a low-level API to pass parameters without using decorators. This gives you more control over the parameters passed to your pipeline components and makes your code more resilient to changes in SDP.

```python
from pyspark import pipelines as dp
import kelp.pipelines as kp

@dp.table(**kp.params("my_table"))
def my_table():
    # ...

@dp.table(**kp.params("my_table", exclude=["schema"])) # (1)!
def my_table_no_schema():
    # ...

```

1. You may also exclude parameters when using the low-level API, just like with the decorators.

## Quality Checks and Quarantine

Kelp's quality checks can be easily integrated into your SDP pipelines. Define your quality checks in your models and then use them in your pipeline code. Kelp will automatically attach the defined quality checks to your decorated function.
The quarantine implementation is based on the Databricks documentation: [Quarantine invalid records](https://docs.databricks.com/aws/en/ldp/expectation-patterns?language=Python#quarantine-invalid-records)

### SDP Expectations

```yaml
kelp_models:
  - name: my_table
    # ...
    quality:
      engine: sdp
      expect_all: 
       "key": "expectation"
      expect_all_or_fail: ...
      expect_all_or_drop: ...
      expect_all_or_quarantine: ...
```

When you use `expect_all_or_quarantine`, Kelp will automatically quarantine the data if any of the expectations fail. You can then investigate and fix the issues with the data before allowing it to be used in downstream pipeline components.

This generates the following SDP chart:

```mermaid
flowchart LR
  upstream("upstream_table") --> validation
  validation("my_table_validation(private)") --> table("my_table")
  validation --> quarantine("my_table_quarantine")
  table --> downstream("downstream_table")
```

### DQX Checks

A similar approach can be taken for DQX checks. Define your DQX checks in your model metadata and Kelp will automatically run them in your pipeline.

Since DQX checks are applied at the code level, you can also set the expectation level and quarantine pattern for each table in your model metadata. This gives you more control over how the quality checks are applied and how the data is handled when checks fail.

The `sdp_expect_level` can be set to `warn`, `fail`, or `drop`, which correspond to the different expectation decorators in SDP. You can deactivate the expectation by setting it to `deactivate`.

Setting `sdp_quarantine` to `true` enables the quarantine pattern for this table, which will automatically quarantine the data if any of the DQX checks fail. This generates the same SDP chart as shown above for the `expect_all_or_quarantine` example, but with the DQX checks applied in the validation step.

```yaml
kelp_models:
  - name: my_table
    # ...
    quality:
      engine: dqx
      sdp_expect_level: warn # (1)!
      sdp_quarantine: true # (2)!
      checks:
        - check:
            function: is_in_list
            arguments:
              column: order_state
              allowed:
                - ...
```

1. This will append an SDP expectation to the pipeline with the corresponding level.

2. This will enable the quarantine pattern for this table.




## Using ref() and target() Functions in SDP

You can also use the `ref()` and `target()` functions to develop your upstream and downstream pipeline components.
This reduces the need to pass catalog and schema configurations in your pipeline code, as Kelp will auto-resolve these based on the model metadata.
If you use a quarantine pattern, `target` will auto-resolve to the validation table.

```python
import kelp.pipelines as kp
from pyspark import pipelines as dp


@kp.create_streaming_table("my_table")

@dp.append_flow(target = kp.target("my_table"),)
def upstream_flow():
  #...

@kp.table
def downstream_table():
    df = spark.readStream.table(kp.ref("my_table"))
    # ...
```

## Create Streaming Table Function

You can create streaming tables by using the `create_streaming_table` wrapper function or by using the low-level API `params_cst()`. The same rules for auto-injecting and excluding parameters apply to streaming tables.

Both options also inject the SDP expectation quality checks if they are defined in the model metadata.

```python
import kelp.pipelines as kp

kp.create_streaming_table("my_streaming_table")

# or using low-level API
from pyspark import pipelines as dp

dp.create_streaming_table(**kp.params_cst("my_streaming_table"))
``` 

## Applying Catalog Metadata to SDP Tables

Currently, SDP does not have full built-in support for catalog metadata like tags. If you wish to omit the Spark schema, you also cannot apply descriptions to your columns. Kelp provides a workaround for this by applying the catalog metadata in a separate step in your Lakeflow Job.

``` yaml
kelp_models:
  - name: my_table
    # ...
    columns:
      column1:
        description: This is column 1
        tags:
          - tag1 : "" # (1)!
          - tag2 : "value"
```

1. This will apply the tag `tag1` as key-only tag to `column1` in the catalog.

Learn more about syncing your catalog metadata with your tables here: [Sync Metadata with Your Catalog](./catalog.md)
