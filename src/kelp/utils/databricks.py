from kelp.models.table import Table
from databricks.sdk import WorkspaceClient


def get_table_from_dbx_sdk(
    full_table: str, w: WorkspaceClient | None = None, profile: str | None = None
) -> Table:
    """Retrieve table metadata from Databricks SDK and convert to Kelp Table format."""
    w = w or WorkspaceClient(profile=profile)
    info = w.tables.get(full_table)

    table_tags = {}
    for tag in w.entity_tag_assignments.list("tables", full_table):
        table_tags[tag.tag_key] = tag.tag_value

    table_obj = {}
    table_obj["name"] = info.name
    table_obj["catalog"] = info.catalog_name
    table_obj["schema_"] = info.schema_name
    table_obj["table_type"] = info.table_type.value.lower()
    table_obj["description"] = info.comment
    table_obj["tags"] = table_tags
    table_obj["columns"] = []
    table_obj["partition_cols"] = [
        col.name
        for col in sorted(
            [col for col in info.columns if col.partition_index is not None],
            key=lambda col: col.partition_index,
        )
    ]
    table_obj["cluster_by_auto"] = info.properties.get("clusterByAuto", "false").lower() == "true"
    table_obj["cluster_by"] = [
        col[0]
        for col in info.properties.get("clusteringColumns", "[]")
        .replace("[", "")
        .replace("]", "")
        .replace('"', "")
        .split(",")
        if col
    ]
    for col in info.columns:
        col_tags = {}
        for tag in w.entity_tag_assignments.list("columns", f"{full_table}.{col.name}"):
            col_tags[tag.tag_key] = tag.tag_value
        col_obj = {
            "name": col.name,
            "description": col.comment,
            "data_type": col.type_text,
            "nullable": col.nullable,
            "tags": col_tags,
        }
        table_obj["columns"].append(col_obj)

    table_obj["table_properties"] = info.properties

    ## constraints
    table_obj["constraints"] = []
    pk_contraint = {}
    fk_constraint = {}
    for contraint in info.table_constraints:
        if contraint.primary_key_constraint:
            pk_contraint = {
                "name": contraint.primary_key_constraint.name,
                "type": "primary_key",
                "columns": contraint.primary_key_constraint.child_columns,
            }
            table_obj["constraints"].append(pk_contraint)
        if contraint.foreign_key_constraint:
            fk_constraint = {
                "name": contraint.foreign_key_constraint.name,
                "type": "foreign_key",
                "columns": contraint.foreign_key_constraint.child_columns,
                "reference_table": contraint.foreign_key_constraint.parent_table,
                "reference_columns": contraint.foreign_key_constraint.parent_columns,
            }
            table_obj["constraints"].append(fk_constraint)

    return Table(**table_obj)
