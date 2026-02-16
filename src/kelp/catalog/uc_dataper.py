import logging

from pyspark.sql import SparkSession

from kelp.config.lifecycle import get_context
from kelp.models.project_config import RemoteCatalogConfig
from kelp.models.table import Column, Table
from kelp.service.table_manager import TableManager
from kelp.utils.databricks import get_table_from_dbx_sdk

logger = logging.getLogger(f"{__name__}")

BASE_ALTER_QUERY = "ALTER {table_type} {full_table_name} {alter_action}"
ALTER_COLUMN_QUERY = "ALTER COLUMN {column_name} {alter_action}"
SET_COMMENT_QUERY = "COMMENT '{comment}'"
SET_TABLE_PROPERTIES_QUERY = "SET TBLPROPERTIES ({properties})"
UNSET_TABLE_PROPERTIES_QUERY = "UNSET TBLPROPERTIES ({properties})"
SET_TAGS_QUERY = "SET TAGS ({tags})"
UNSET_TAGS_QUERY = "UNSET TAGS ({tags})"

COMMENT_ON_TYPE_QUERY = "COMMENT ON {type} {path} IS '{comment}'"
SET_TAG_ON_TYPE_QUERY = "SET TAG ON {type} {path} `{tag_key}`=`{tag_value}`"
UNSET_TAG_ON_TYPE_QUERY = "UNSET TAG ON {type} {path} `{tag_key}`"

CREATE_PRIMARY_KEY_QUERY = (
    "ALTER TABLE {full_table_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({columns})"
)
CREATE_FOREIGN_KEY_QUERY = "ALTER TABLE {full_table_name} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({columns}) REFERENCES {reference_table} ({reference_columns})"
DROP_CONSTRAINT_QUERY = "ALTER TABLE {full_table_name} DROP CONSTRAINT {constraint_name}"


TABLE_TYPE_MAPPING = {
    "managed": "TABLE",
    "view": "VIEW",
    "materialized_view": "MATERIALIZED VIEW",
    "streaming_table": "STREAMING TABLE",
}


class UnityCatalogAdapter:
    """Adapter for Unity Catalog remote catalog operations."""

    catalog_config: RemoteCatalogConfig
    _spark: SparkSession

    def __init__(
        self, config: RemoteCatalogConfig | None = None, spark: SparkSession | None = None
    ):
        self.catalog_config = config or get_context().project_config.remote_catalog_config
        # self._spark = spark or SparkSession.active()

    def sync_table(self, table: Table) -> list[str]:
        """Sync the given Table object to the remote catalog according to
        the provided sync configuration.

        Returns:
            List of SQL queries to be executed.
        """
        queries = []
        fqn = TableManager.get_qualified_tablename_from_table(table)
        remote_table = self.get_table_info(fqn)
        diffs = self.compare_table_state(table, remote_table)
        logger.debug(
            "Differences for table %s: %s",
            fqn,
            diffs,
        )
        # Apply updates as needed

        # Description updates
        if diffs["table_description"]:
            new_description = diffs["table_description"]
            queries.extend(
                self.update_table_description(
                    fqn,
                    new_description,
                    table.table_type,
                )
            )

        for col_name, col_diff in diffs["column_descriptions"].items():
            queries.extend(
                self.update_column_description(
                    fqn,
                    col_name,
                    diffs["column_descriptions"][col_name],
                    table.table_type,
                )
            )

        # Tag updates
        if diffs["table_tags"]["updates"]:
            queries.extend(
                self.update_table_tags(
                    fqn,
                    diffs["table_tags"]["updates"],
                    table.table_type,
                )
            )
        if diffs["table_tags"]["deletes"]:
            queries.extend(
                self.delete_table_tags(
                    fqn,
                    diffs["table_tags"]["deletes"],
                    table.table_type,
                )
            )

        for col_name, col_diff in diffs["column_tags"].items():
            if col_diff:
                if col_diff["updates"]:
                    queries.extend(
                        self.update_column_tags(
                            fqn,
                            col_name,
                            col_diff["updates"],
                            table.table_type,
                        )
                    )
                if col_diff["deletes"]:
                    queries.extend(
                        self.delete_column_tags(
                            fqn,
                            col_name,
                            col_diff["deletes"],
                            table.table_type,
                        )
                    )

        # Property updates
        if diffs["table_properties"]["updates"]:
            queries.extend(
                self.update_table_properties(
                    fqn,
                    diffs["table_properties"]["updates"],
                    table.table_type,
                )
            )
        if diffs["table_properties"]["deletes"]:
            queries.extend(
                self.delete_table_properties(
                    fqn,
                    diffs["table_properties"]["deletes"],
                    table.table_type,
                )
            )

        # Cluster by updates
        if (
            table.cluster_by != remote_table.cluster_by
            or table.cluster_by_auto != remote_table.cluster_by_auto
        ):
            queries.extend(
                self.update_cluster_by(
                    fqn,
                    table.cluster_by,
                    table.cluster_by_auto,
                    table.table_type,
                )
            )

        if table.partition_cols != remote_table.partition_cols:
            logger.warning(
                "Partition column changes require manual intervention in Unity Catalog; skipping partition column sync for %s",
                fqn,
            )

        # Constraint updates
        pk_diff = diffs["constraints_pk"]
        if pk_diff["delete"]:
            queries.extend(self.drop_constraint(fqn, pk_diff["delete"].name))
        if pk_diff["update"]:
            queries.extend(self.drop_constraint(fqn, pk_diff["update"].name))
            queries.extend(
                self.create_primary_key(
                    fqn,
                    pk_diff["update"].name,
                    pk_diff["update"].columns,
                )
            )
        fk_diff = diffs["constraints_fk"]
        for fk in fk_diff["delete"]:
            queries.extend(self.drop_constraint(fqn, fk.name))
        for fk in fk_diff["update"]:
            queries.extend(self.drop_constraint(fqn, fk.name))
            queries.extend(
                self.create_foreign_key(
                    fqn,
                    fk.name,
                    fk.columns,
                    fk.reference_table,
                    fk.reference_columns,
                )
            )
        for fk in fk_diff["create"]:
            queries.extend(
                self.create_foreign_key(
                    fqn,
                    fk.name,
                    fk.columns,
                    fk.reference_table,
                    fk.reference_columns,
                )
            )

        return queries

    def sync_tables(self, tables: list[Table]) -> list[str]:
        """Sync multiple Table objects to the remote catalog according to
        the provided sync configuration.

        Returns:
            List of SQL queries to be executed.
        """
        queries = []
        for table in tables:
            queries.extend(self.sync_table(table))
        return queries

    def sync_all_tables(self, tables: list[Table] | None = None) -> list[str]:
        """Sync all tables in the local project catalog to the remote catalog.

        Returns:
            List of SQL queries to be executed.
        """
        from kelp.config.lifecycle import get_context

        _tables = tables or get_context().catalog.get_tables()
        return self.sync_tables(_tables)

    def compare_table_state(self, local: Table, remote: Table) -> dict:
        """Compare a local table definition (from project catalog) with the
        remote Table object and return a dict describing differences.
        The returned dict has the following structure:
        {
            "table_description": str | None,
            "table_tags": {"updates": dict[str, str], "deletes": list[str]},
            "table_properties": {"updates": dict[str, str], "deletes": list[str]},
            "column_descriptions": dict[str, str],
            "column_tags": dict[str,{"updates": dict[str,str], "deletes": list[str]}],
        """

        def check_scope(key: str, managed: list[str]) -> bool:
            """Small helper function to determine if a key is in scope based on managed list.
            Returns True if managed is empty or key is in managed."""
            return (not managed) or (key in managed)

        def compare_two_dicts(
            local_dict: dict[str, str],
            remote_dict: dict[str, str],
            managed_keys: list[str],
            mode: str,
        ) -> dict[str, dict | list]:
            """Helper function to compare two dictionaries and return updates and deletes."""
            updates = {}
            deletes = []

            local_keys = set(local_dict.keys()) if local_dict else set()
            remote_keys = set(remote_dict.keys()) if remote_dict else set()

            # Identify new or updated keys
            for k in local_keys:
                if k not in remote_keys or local_dict[k] != remote_dict[k]:
                    updates[k] = local_dict[k]

            # Identify deleted keys
            if mode == "replace":
                for k in remote_keys - local_keys:
                    if check_scope(k, managed_keys):
                        deletes.append(k)
            result = {"updates": updates, "deletes": deletes}
            return result

        config = self.catalog_config

        diffs: dict = {
            "table_description": None,
            "table_tags": {"updates": {}, "deletes": []},
            "table_properties": {"updates": {}, "deletes": []},
            "column_descriptions": {},
            "column_tags": {"updates": {}, "deletes": []},
            "constraints_pk": {"create": None, "update": None, "delete": None},
            "constraints_fk": {"create": [], "update": [], "delete": []},
        }

        ### Table-level comparison
        if remote.description != local.description:
            diffs["table_description"] = local.description

        ### Table tags comparison
        # Compare table tags, respecting tag_mode and managed_tags
        diffs["table_tags"] = compare_two_dicts(
            local.tags or {},
            remote.tags or {},
            config.managed_table_tags,
            config.table_tag_mode,
        )

        # Compare table properties, respecting property_mode and managed_properties
        diffs["table_properties"] = compare_two_dicts(
            local.table_properties or {},
            remote.table_properties or {},
            config.managed_table_properties,
            config.table_property_mode,
        )

        # Constraints
        local_constraint_map = {c.name: c for c in local.constraints or []}
        remote_constraint_map = {c.name: c for c in remote.constraints or []}
        for rc_name, rc in remote_constraint_map.items():
            lc = local_constraint_map.get(rc_name)
            if lc is None:
                # Constraint not found locally; mark for deletion
                if lc.type == "primary_key":
                    diffs["constraints_pk"]["delete"] = rc
                elif lc.type == "foreign_key":
                    diffs["constraints_fk"]["delete"].append(rc)

            if lc:
                if lc.type != rc.type:
                    logger.warning(
                        "Constraint type change detected for constraint %s; manual intervention required. skipping constraint sync for %s",
                        rc_name,
                        rc_name,
                    )
                    continue

                if lc.columns != rc.columns:
                    # Columns have changed; mark for update
                    if lc.type == "primary_key":
                        diffs["constraints_pk"]["update"] = lc
                    elif lc.type == "foreign_key":
                        diffs["constraints_fk"]["update"].append(lc)
                if lc.type == "foreign_key" and (
                    lc.reference_table != rc.reference_table
                    or lc.reference_columns != rc.reference_columns
                ):
                    diffs["constraints_fk"]["update"].append(lc)
            else:
                # Compare constraint definitions (simplified comparison here)
                if rc.constraint_type != lc.constraint_type or rc.columns != lc.columns:
                    diffs["constraints_fk"]["update"].append(lc)

        ## check for remote missing constraints that exist locally and mark for creation
        for lc_name, lc in local_constraint_map.items():
            rc = remote_constraint_map.get(lc_name)
            if rc is None:
                # Constraint not found remotely; mark for creation
                if lc.type == "primary_key":
                    diffs["constraints_pk"]["create"] = lc
                elif lc.type == "foreign_key":
                    diffs["constraints_fk"]["create"].append(lc)

        # Compare column descriptions and tags
        # Build a map of remote columns for easy lookup
        local_cols_map: dict[str, Column] = {}
        for lc in local.columns or []:
            local_cols_map[lc.name] = lc
        for rc in remote.columns or []:
            lc = local_cols_map.get(rc.name)
            if lc is None:
                # Column not found locally; skip (no delete operation)
                continue
            # Compare descriptions
            if rc.description != lc.description:
                diffs["column_descriptions"][rc.name] = lc.description
            # Compare tags, respecting tag_mode and managed_tags
            col_tag_diffs = compare_two_dicts(
                lc.tags or {},
                rc.tags or {},
                config.managed_column_tags,
                config.column_tag_mode,
            )
            if col_tag_diffs["updates"] or col_tag_diffs["deletes"]:
                diffs["column_tags"][rc.name] = col_tag_diffs

        return diffs

    def get_table_info(self, full_table_name: str) -> Table | None:
        """Return a Table representing the remote table, or None if not found."""
        # Implementation to fetch table info from Unity Catalog
        return get_table_from_dbx_sdk(full_table_name)

    def update_table_description(
        self, full_table_name: str, description: str, table_type: str | None = None
    ) -> list[str]:
        """Update the table-level description/comment in the remote catalog.

        Returns:
            List of SQL queries to be executed.
        """

        ## Skip Streaming Tables
        if table_type == "streaming_table":
            logger.warning(
                "Comment on streaming tables is not supported in Unity Catalog; Use SDP definition skipping %s",
                full_table_name,
            )
            return []

        # Implementation to update table description in Unity Catalog
        query = COMMENT_ON_TYPE_QUERY.format(
            type=TABLE_TYPE_MAPPING.get(table_type, "TABLE"),
            path=full_table_name,
            comment=description.replace("'", "''"),
        )
        logger.debug("Generated query: %s", query)
        return [query]

    def update_column_description(
        self,
        full_table_name: str,
        column_name: str,
        description: str,
        table_type: str,
    ) -> list[str]:
        """Update a single column description in the remote catalog.

        Returns:
            List of SQL queries to be executed.
        """
        # Implementation to update column description in Unity Catalog

        if table_type == "streaming_table":
            query = BASE_ALTER_QUERY.format(
                table_type="STREAMING TABLE",
                full_table_name=full_table_name,
                alter_action=ALTER_COLUMN_QUERY.format(
                    column_name=column_name,
                    alter_action=SET_COMMENT_QUERY.format(comment=description.replace("'", "''")),
                ),
            )
        else:
            query = COMMENT_ON_TYPE_QUERY.format(
                type="COLUMN",
                path=f"{full_table_name}.{column_name}",
                comment=description.replace("'", "''"),
            )

        logger.debug("Generated query: %s", query)
        return [query]

    def update_table_tags(
        self, full_table_name: str, tags: dict[str, str], table_type: str
    ) -> list[str]:
        """Set/replace table tags (key->value). Implementations may merge or replace; document behaviour.

        Returns:
            List of SQL queries to be executed.
        """

        # Implementation to update table tags in Unity Catalog
        tags_str = ", ".join([f"""'{k}'='{v.replace("'", "''")}'""" for k, v in tags.items()])
        table_type = TABLE_TYPE_MAPPING.get(table_type, "TABLE")
        query = BASE_ALTER_QUERY.format(
            table_type=table_type,
            full_table_name=full_table_name,
            alter_action=SET_TAGS_QUERY.format(tags=tags_str),
        )
        logger.debug("Generated query: %s", query)
        return [query]

    def delete_table_tags(self, full_table_name, tag_keys, table_type):
        """Delete specific tags from the table.

        Returns:
            List of SQL queries to be executed.
        """
        # Implementation to delete table tags in Unity Catalog
        tags_str = ", ".join([f"""'{k}'""" for k in tag_keys])
        table_type = TABLE_TYPE_MAPPING.get(table_type, "TABLE")
        query = BASE_ALTER_QUERY.format(
            table_type=table_type,
            full_table_name=full_table_name,
            alter_action=UNSET_TAGS_QUERY.format(tags=tags_str),
        )
        logger.debug("Generated query: %s", query)
        return [query]

    def update_column_tags(
        self,
        full_table_name: str,
        column_name: str,
        tags: dict[str, str],
        table_type: str | None = None,
    ) -> list[str]:
        """Set/replace tags for a specific column.

        Returns:
            List of SQL queries to be executed.
        """
        # Implementation to update column tags in Unity Catalog
        tags_str = ", ".join([f"""'{k}'='{v.replace("'", "''")}'""" for k, v in tags.items()])
        queries = []

        if table_type == "view":
            for k, v in tags.items():
                unset_query = UNSET_TAG_ON_TYPE_QUERY.format(
                    type="COLUMN",
                    path=f"{full_table_name}.{column_name}",
                    tag_key=k,
                )
                logger.debug("Generated query (optional unset): %s", unset_query)
                queries.append(unset_query)

                query = SET_TAG_ON_TYPE_QUERY.format(
                    type="COLUMN",
                    path=f"{full_table_name}.{column_name}",
                    tag_key=k,
                    tag_value=v.replace("'", "''"),
                )
                logger.debug("Generated query: %s", query)
                queries.append(query)
            return queries

        table_type = TABLE_TYPE_MAPPING.get(table_type, "TABLE")
        query = BASE_ALTER_QUERY.format(
            table_type=table_type,
            full_table_name=full_table_name,
            alter_action=ALTER_COLUMN_QUERY.format(
                column_name=column_name,
                alter_action=SET_TAGS_QUERY.format(tags=tags_str),
            ),
        )
        logger.debug("Generated query: %s", query)
        return [query]

    def delete_column_tags(
        self,
        full_table_name: str,
        column_name: str,
        tag_keys: list[str],
        table_type: str | None = None,
    ) -> list[str]:
        """Delete specific tags from a column.

        Returns:
            List of SQL queries to be executed.
        """
        queries = []

        if table_type == "view":
            for k in tag_keys:
                query = UNSET_TAG_ON_TYPE_QUERY.format(
                    type="COLUMN",
                    path=f"{full_table_name}.{column_name}",
                    tag_key=k,
                )
                logger.debug("Generated query: %s", query)
                queries.append(query)
            return queries

        tags_str = ", ".join([f"""'{k}'""" for k in tag_keys])
        table_type = TABLE_TYPE_MAPPING.get(table_type, "TABLE")
        query = BASE_ALTER_QUERY.format(
            table_type=table_type,
            full_table_name=full_table_name,
            alter_action=ALTER_COLUMN_QUERY.format(
                column_name=column_name,
                alter_action=UNSET_TAGS_QUERY.format(tags=tags_str),
            ),
        )
        logger.debug("Generated query: %s", query)
        return [query]

    def update_table_properties(
        self,
        full_table_name: str,
        properties: dict[str, str],
        table_type: str | None = None,
    ) -> list[str]:
        """Update table properties in the remote catalog.

        Returns:
            List of SQL queries to be executed.
        """

        if table_type in ["view", "materialized_view", "streaming_table"]:
            logger.warning(
                "Table properties on views, materialized views, and streaming tables are ignored set them through definition; skipping %s",
                full_table_name,
            )
            return []

        query = BASE_ALTER_QUERY.format(
            table_type="TABLE",
            full_table_name=full_table_name,
            alter_action=SET_TABLE_PROPERTIES_QUERY.format(
                properties=", ".join(
                    [f"""'{k}'='{v.replace("'", "''")}'""" for k, v in properties.items()]
                )
            ),
        )
        logger.debug("Generated query: %s", query)
        return [query]

    def delete_table_properties(
        self,
        full_table_name: str,
        property_keys: list[str],
        table_type: str | None = None,
    ) -> list[str]:
        """Delete specific properties from the table.

        Returns:
            List of SQL queries to be executed.
        """

        if table_type in ["view", "materialized_view", "streaming_table"]:
            logger.warning(
                "Table properties on views, materialized views, and streaming tables are ignored unset them through definition; skipping %s",
                full_table_name,
            )
            return []
        query = BASE_ALTER_QUERY.format(
            table_type="TABLE",
            full_table_name=full_table_name,
            alter_action=UNSET_TABLE_PROPERTIES_QUERY.format(
                properties=", ".join([f"""'{k}'""" for k in property_keys])
            ),
        )
        logger.debug("Generated query: %s", query)
        return [query]

    def update_cluster_by(
        self,
        full_table_name: str,
        cluster_by_cols: list[str] | None = None,
        cluster_by_auto: bool | None = None,
        table_type: str | None = None,
    ) -> list[str]:
        """Update clustering columns for a table. Note that this will cause a rewrite of the table data.

        Returns:
            List of SQL queries to be executed.
        """

        if table_type in ["view", "materialized_view", "streaming_table"]:
            logger.warning(
                "Clustering columns on views, materialized views, and streaming tables are not supported in Unity Catalog; Use SDP or SQL definition. skipping %s",
                full_table_name,
            )
            return []

        if cluster_by_auto:
            query = f"ALTER TABLE {full_table_name} CLUSTER BY AUTO"
        elif not cluster_by_cols:
            query = f"ALTER TABLE {full_table_name} CLUSTER BY NONE"
        elif cluster_by_cols:
            cluster_by_str = ", ".join(cluster_by_cols)
            query = f"ALTER TABLE {full_table_name} CLUSTER BY ({cluster_by_str})"
        else:
            logger.warning(
                "No changes detected for clustering columns for %s; skipping update",
                full_table_name,
            )
            return []

        logger.debug("Generated query: %s", query)
        return [query]

    def drop_constraint(self, full_table_name: str, constraint_name: str) -> list[str]:
        """Drop a primary key or foreign key constraint from the table.

        Returns:
            List of SQL queries to be executed.
        """
        query = DROP_CONSTRAINT_QUERY.format(
            full_table_name=full_table_name,
            constraint_name=constraint_name,
        )
        logger.debug("Generated query: %s", query)
        return [query]

    def create_primary_key(
        self,
        full_table_name: str,
        constraint_name: str,
        columns: list[str],
    ) -> list[str]:
        """Create a primary key constraint on the table.

        Returns:
            List of SQL queries to be executed.
        """
        columns_str = ", ".join(columns)
        query = CREATE_PRIMARY_KEY_QUERY.format(
            full_table_name=full_table_name,
            constraint_name=constraint_name,
            columns=columns_str,
        )
        logger.debug("Generated query: %s", query)
        return [query]

    def create_foreign_key(
        self,
        full_table_name: str,
        constraint_name: str,
        columns: list[str],
        reference_table: str,
        reference_columns: list[str],
    ) -> list[str]:
        """Create a foreign key constraint on the table.

        Returns:
            List of SQL queries to be executed.
        """
        columns_str = ", ".join(columns)
        reference_columns_str = ", ".join(reference_columns)
        query = CREATE_FOREIGN_KEY_QUERY.format(
            full_table_name=full_table_name,
            constraint_name=constraint_name,
            columns=columns_str,
            reference_table=reference_table,
            reference_columns=reference_columns_str,
        )
        logger.debug("Generated query: %s", query)
        return [query]
