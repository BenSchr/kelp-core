# Sync Metadata with Your Catalog

Kelp's catalog sync feature allows you to keep your local metadata in sync with your remote catalog. This ensures that any changes you make to your local metadata are reflected in your remote catalog, and vice versa.

## Sync Metadata Code

When you use Kelp's catalog sync feature, you can leverage metadata defined in your project to apply descriptions, tags, and constraints to your tables and metric views.

`sync_catalog()` generates SQL queries based on the differences between your local metadata and the remote catalog. You can review these queries before executing them to ensure they align with your expectations.

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

for query in kc.sync_catalog():
    try:
        print(f"Executing query: {query}")
        spark.sql(query)
    except Exception as e:
        print(f"Error executing query: {query}\nException: {e}")
```

### Other Functions

Kelp also provides `sync_tables()` and `sync_metric_views()` to sync only tables or only metric views, respectively. These functions are useful when you want to sync just a subset of your metadata.

```python
# To sync only tables
for query in kc.sync_tables():
    # execute query

# To sync only metric views
for query in kc.sync_metric_views():
    # execute query

# Sync only a subset of metadata
for query in kc.sync_tables(table_names=["my_table1", "my_table2"]):
    # execute query

for query in kc.sync_metric_views(view_names=["my_metric_view1"]):
    # execute query    
```


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
