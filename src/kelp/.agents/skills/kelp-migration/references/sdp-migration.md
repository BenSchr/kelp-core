# SDP Migration
This reference provides guidance on how to migrate existing Spark Declarative Pipelines to use Kelp metadata. It covers how to refactor hardcoded catalog/schema references, extract metadata from code to YAML, and convert manual expectations to Kelp quality checks.

## Source Example

```python
catalog = spark.conf.get("my_catalog")
schema = spark.conf.get("my_schema")

table_schema = "<spark schema with optiona comments>"

dp.create_streaming_table(
  name = f"{catalog}.{schema}.<table-name>",
  comment = "<comment>",
  spark_conf={"<key>" : "<value", "<key" : "<value>"},
  table_properties={"<key>" : "<value>", "<key>" : "<value>"},
  path="<storage-location-path>",
  partition_cols=["<partition-column>", "<partition-column>"],
  cluster_by_auto = <bool>,
  cluster_by = ["<clustering-column>", "<clustering-column>"],
  schema="schema-definition",
  expect_all = {"<key>" : "<value", "<key" : "<value>"},
  expect_all_or_drop = {"<key>" : "<value", "<key" : "<value>"},
  expect_all_or_fail = {"<key>" : "<value", "<key" : "<value>"},
  row_filter = "row-filter-clause"
)

@dp.append_flow
def my_table_flow(
    target=f"{catalog}.{schema}.my_table",
):
    df = spark.readStream # ... read from source
    # Other transformations...
    return df


# Other expectations...
@dp.table(
  name=f"{catalog}.{schema}.<name>",
  comment="<comment>",
  spark_conf={"<key>" : "<value>", "<key>" : "<value>"},
  table_properties={"<key>" : "<value>", "<key>" : "<value>"},
  path="<storage-location-path>",
  partition_cols=["<partition-column>", "<partition-column>"],
  cluster_by_auto = <bool>,
  cluster_by = ["<clustering-column>", "<clustering-column>"],
  schema="schema-definition",
  row_filter = "row-filter-clause")
@dp.expect_all({description: constraint, ...})
@dp.expect_all_or_drop({description: constraint, ...})
@dp.expect_all_or_fail({description: constraint, ...})
def downstream_table():
    df = spark.readStream.table(f"{catalog}.{schema}.my_table")
    # Other transformations...
    return df


# Same for materialized view
```

## Target Example

Omit spark conf for catalog and schema, use `ref()` and `target()` functions to auto-resolve references, extract metadata to YAML, use kelp quality checks instead of manual expectations.

```python
import kelp.pipelines as kp

# no spark conf needed for catalog and schema, Kelp will auto-resolve based on model metadata

@dp.create_streaming_table(**kp.params_cst("my_table")) # auto-resolve fqn and parameters from model metadata

@dp.append_flow(target = kp.target("my_table"),) # auto-resolve target reference
def upstream_flow():
  #...

# Be aware this doesn't apply quality checks
# Use kp.table decorator or switch to create_streaming_table + flow if you need to apply quality checks
@dp.table(**kp.params("downstream_table")) # auto-resolve fqn and parameters from model metadata
def downstream_table():
    df = spark.readStream.table(kp.ref("my_table")) # auto-resolve reference to upstream table
    # Other transformations...
    return df

@dp.materialized_view(**kp.params("my_metric_view")) # auto-resolve fqn and parameters from model metadata
def my_metric_view():
    df = spark.readStream.table(kp.ref("downstream_table"))
```

## Target metadata example

Extract metadata to YAML:
- One file per model.
- Extract the table-schema to columns detect types as lowercase and also comments or any other relevant metadata. - Extract expectations to the quality section 
- Extract any other relevant metadata like clustering, partitioning, table properties, comments, etc.

**Important**:Only add provided metadata. Don't add any additinal fields not in source except the user intructed to do so.

```yaml
kelp_models:
  - name: my_table
    columns:
      - name: id
        type: string
        comment: "The unique identifier"
      #- ...
    cluster_by: ["<clustering-column>", "<clustering-column>"]
    table_properties:
      "<key>": "<value>"
    quality:
        engine: sdp
        expect_all: 
         "key": "expectation"
        expect_all_or_fail: ...
        expect_all_or_drop: ...
        expect_all_or_quarantine: ... # only with kp.table decorator, not supported for materialized views
```

## DAB Configuration

Be aware that the user has to set the pipeline configuration to put project file path and target in the DAB if a DAB is used.

