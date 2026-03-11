"""Tests for Unity Catalog SQL query generation."""

from __future__ import annotations

from kelp.catalog.uc_models import (
    ColumnDiff,
    ConstraintFKDiff,
    ConstraintPKDiff,
    DictDiff,
    TableDiff,
)
from kelp.catalog.uc_query_builder import UCQueryBuilder
from kelp.models.model import ForeignKeyConstraint, PrimaryKeyConstraint


class TestTableDescriptionQueries:
    """Tests for table description SQL generation."""

    def test_table_description_managed_table(self):
        """Test COMMENT ON statement for managed tables."""
        builder = UCQueryBuilder()

        diff = TableDiff(table_description="New table description")
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert queries[0] == "COMMENT ON TABLE catalog.schema.table IS 'New table description'"

    def test_table_description_view(self):
        """Test COMMENT ON statement for views."""
        builder = UCQueryBuilder()

        diff = TableDiff(table_description="View description")
        queries = builder.build("catalog.schema.my_view", diff, "view")

        assert len(queries) == 1
        assert queries[0] == "COMMENT ON VIEW catalog.schema.my_view IS 'View description'"

    def test_table_description_materialized_view(self):
        """Test COMMENT ON statement for materialized views."""
        builder = UCQueryBuilder()

        diff = TableDiff(table_description="Materialized view description")
        queries = builder.build("catalog.schema.mv", diff, "materialized_view")

        assert len(queries) == 1
        # Materialized views use TABLE type in COMMENT ON
        assert queries[0] == "COMMENT ON TABLE catalog.schema.mv IS 'Materialized view description'"

    def test_table_description_streaming_table(self):
        """Test that streaming tables skip COMMENT ON."""
        builder = UCQueryBuilder()

        diff = TableDiff(table_description="Should be skipped")
        queries = builder.build("catalog.schema.st", diff, "streaming_table")

        # Should be empty because streaming tables don't support COMMENT ON
        assert len(queries) == 0

    def test_description_with_quotes(self):
        """Test description with single quotes is properly escaped."""
        builder = UCQueryBuilder()

        diff = TableDiff(table_description="It's a table with 'quotes'")
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert (
            queries[0] == "COMMENT ON TABLE catalog.schema.table IS 'It''s a table with ''quotes'''"
        )


class TestTableTagQueries:
    """Tests for table tag SQL generation."""

    def test_set_table_tags(self):
        """Test SET TAGS for table."""
        builder = UCQueryBuilder()

        tag_diff = DictDiff(updates={"env": "dev", "owner": "team"})
        diff = TableDiff(table_tags=tag_diff)
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert "ALTER TABLE catalog.schema.table SET TAGS" in queries[0]
        assert "'env'='dev'" in queries[0]
        assert "'owner'='team'" in queries[0]

    def test_unset_table_tags(self):
        """Test UNSET TAGS for table."""
        builder = UCQueryBuilder()

        tag_diff = DictDiff(deletes=["deprecated", "old_tag"])
        diff = TableDiff(table_tags=tag_diff)
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert "ALTER TABLE catalog.schema.table UNSET TAGS" in queries[0]
        assert "'deprecated'" in queries[0]
        assert "'old_tag'" in queries[0]

    def test_set_and_unset_table_tags(self):
        """Test both SET and UNSET TAGS for table."""
        builder = UCQueryBuilder()

        tag_diff = DictDiff(
            updates={"env": "prod"},
            deletes=["deprecated"],
        )
        diff = TableDiff(table_tags=tag_diff)
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 2
        assert any("SET TAGS" in q for q in queries)
        assert any("UNSET TAGS" in q for q in queries)

    def test_view_table_tags(self):
        """Test that views use ALTER VIEW for tags."""
        builder = UCQueryBuilder()

        tag_diff = DictDiff(updates={"tag": "value"})
        diff = TableDiff(table_tags=tag_diff)
        queries = builder.build("catalog.schema.my_view", diff, "view")

        assert len(queries) == 1
        assert "ALTER VIEW catalog.schema.my_view SET TAGS" in queries[0]

    def test_tag_escaping(self):
        """Test that tag keys and values with quotes are escaped."""
        builder = UCQueryBuilder()

        tag_diff = DictDiff(updates={"key'with'quotes": "value's"})
        diff = TableDiff(table_tags=tag_diff)
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert "'key''with''quotes'='value''s'" in queries[0]


class TestTablePropertyQueries:
    """Tests for table property SQL generation."""

    def test_set_table_properties(self):
        """Test SET TBLPROPERTIES."""
        builder = UCQueryBuilder()

        prop_diff = DictDiff(
            updates={
                "delta.enableChangeDataFeed": "true",
                "delta.autoOptimize": "true",
            }
        )
        diff = TableDiff(table_properties=prop_diff)
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert "ALTER TABLE catalog.schema.table SET TBLPROPERTIES" in queries[0]
        assert "'delta.enableChangeDataFeed'='true'" in queries[0]
        assert "'delta.autoOptimize'='true'" in queries[0]

    def test_unset_table_properties(self):
        """Test UNSET TBLPROPERTIES."""
        builder = UCQueryBuilder()

        prop_diff = DictDiff(deletes=["old.property", "removed.property"])
        diff = TableDiff(table_properties=prop_diff)
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert "ALTER TABLE catalog.schema.table UNSET TBLPROPERTIES" in queries[0]
        assert "'old.property'" in queries[0]
        assert "'removed.property'" in queries[0]

    def test_properties_not_supported_for_views(self):
        """Test that views don't generate TBLPROPERTIES statements."""
        builder = UCQueryBuilder()

        prop_diff = DictDiff(updates={"prop": "value"})
        diff = TableDiff(table_properties=prop_diff)
        queries = builder.build("catalog.schema.my_view", diff, "view")

        # Should be empty because views don't support TBLPROPERTIES
        assert len(queries) == 0

    def test_properties_not_supported_for_streaming_tables(self):
        """Test that streaming tables don't generate TBLPROPERTIES statements."""
        builder = UCQueryBuilder()

        prop_diff = DictDiff(updates={"prop": "value"})
        diff = TableDiff(table_properties=prop_diff)
        queries = builder.build("catalog.schema.st", diff, "streaming_table")

        assert len(queries) == 0


class TestColumnDescriptionQueries:
    """Tests for column description SQL generation."""

    def test_column_description_regular_table(self):
        """Test COMMENT ON COLUMN for regular tables."""
        builder = UCQueryBuilder()

        col_diff = ColumnDiff(description="New column description")
        diff = TableDiff(columns={"col1": col_diff})
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert (
            queries[0] == "COMMENT ON COLUMN catalog.schema.table.col1 IS 'New column description'"
        )

    def test_column_description_streaming_table(self):
        """Test ALTER COLUMN COMMENT for streaming tables."""
        builder = UCQueryBuilder()

        col_diff = ColumnDiff(description="Streaming column desc")
        diff = TableDiff(columns={"col1": col_diff})
        queries = builder.build("catalog.schema.st", diff, "streaming_table")

        assert len(queries) == 1
        assert "ALTER STREAMING TABLE catalog.schema.st ALTER COLUMN col1 COMMENT" in queries[0]

    def test_multiple_column_descriptions(self):
        """Test multiple column description changes."""
        builder = UCQueryBuilder()

        diff = TableDiff(
            columns={
                "col1": ColumnDiff(description="Desc 1"),
                "col2": ColumnDiff(description="Desc 2"),
            }
        )
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 2
        assert any("col1" in q for q in queries)
        assert any("col2" in q for q in queries)


class TestColumnTagQueries:
    """Tests for column tag SQL generation."""

    def test_column_tags_regular_table(self):
        """Test ALTER COLUMN SET TAGS for regular tables."""
        builder = UCQueryBuilder()

        tag_diff = DictDiff(updates={"pii": "true", "sensitive": "true"})
        col_diff = ColumnDiff(tags=tag_diff)
        diff = TableDiff(columns={"email": col_diff})
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert "ALTER TABLE catalog.schema.table ALTER COLUMN email SET TAGS" in queries[0]
        assert "'pii'='true'" in queries[0]
        assert "'sensitive'='true'" in queries[0]

    def test_column_tags_unset(self):
        """Test ALTER COLUMN UNSET TAGS for regular tables."""
        builder = UCQueryBuilder()

        tag_diff = DictDiff(deletes=["old_tag", "deprecated"])
        col_diff = ColumnDiff(tags=tag_diff)
        diff = TableDiff(columns={"col1": col_diff})
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert "ALTER TABLE catalog.schema.table ALTER COLUMN col1 UNSET TAGS" in queries[0]
        assert "'old_tag'" in queries[0]
        assert "'deprecated'" in queries[0]

    def test_column_tags_view_set_tag_on(self):
        """Test SET TAG ON syntax for views."""
        builder = UCQueryBuilder()

        tag_diff = DictDiff(
            creates={"new_tag": "new_value"},
            updates={"existing_tag": "updated_value"},
        )
        col_diff = ColumnDiff(tags=tag_diff)
        diff = TableDiff(columns={"col1": col_diff})
        queries = builder.build("catalog.schema.my_view", diff, "view")

        # Should have creates + unset/set for updates
        assert len(queries) > 0
        assert any("SET TAG ON COLUMN catalog.schema.my_view.col1" in q for q in queries)

    def test_column_tags_view_unset_tag_on(self):
        """Test UNSET TAG ON syntax for views."""
        builder = UCQueryBuilder()

        tag_diff = DictDiff(deletes=["old_tag"])
        col_diff = ColumnDiff(tags=tag_diff)
        diff = TableDiff(columns={"col1": col_diff})
        queries = builder.build("catalog.schema.my_view", diff, "view")

        assert len(queries) == 1
        assert "UNSET TAG ON COLUMN catalog.schema.my_view.col1 `old_tag`" in queries[0]

    def test_column_description_and_tags(self):
        """Test column with both description and tag changes."""
        builder = UCQueryBuilder()

        tag_diff = DictDiff(updates={"pii": "true"})
        col_diff = ColumnDiff(description="New desc", tags=tag_diff)
        diff = TableDiff(columns={"email": col_diff})
        queries = builder.build("catalog.schema.table", diff, "managed")

        # Should have both description and tag queries
        assert len(queries) == 2
        assert any("COMMENT ON COLUMN" in q for q in queries)
        assert any("SET TAGS" in q for q in queries)


class TestClusterByQueries:
    """Tests for CLUSTER BY SQL generation."""

    def test_cluster_by_columns(self):
        """Test ALTER TABLE CLUSTER BY with columns."""
        builder = UCQueryBuilder()

        diff = TableDiff(
            cluster_by_changed=True,
            cluster_by_cols=["col1", "col2"],
            cluster_by_auto=False,
        )
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert queries[0] == "ALTER TABLE catalog.schema.table CLUSTER BY (col1, col2)"

    def test_cluster_by_auto(self):
        """Test ALTER TABLE CLUSTER BY AUTO."""
        builder = UCQueryBuilder()

        diff = TableDiff(
            cluster_by_changed=True,
            cluster_by_cols=None,
            cluster_by_auto=True,
        )
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert queries[0] == "ALTER TABLE catalog.schema.table CLUSTER BY AUTO"

    def test_cluster_by_none(self):
        """Test ALTER TABLE CLUSTER BY NONE."""
        builder = UCQueryBuilder()

        diff = TableDiff(
            cluster_by_changed=True,
            cluster_by_cols=None,
            cluster_by_auto=False,
        )
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert queries[0] == "ALTER TABLE catalog.schema.table CLUSTER BY NONE"

    def test_cluster_by_not_supported_for_views(self):
        """Test that views don't generate CLUSTER BY statements."""
        builder = UCQueryBuilder()

        diff = TableDiff(
            cluster_by_changed=True,
            cluster_by_cols=["col1"],
            cluster_by_auto=False,
        )
        queries = builder.build("catalog.schema.my_view", diff, "view")

        # Should be empty because views don't support CLUSTER BY
        assert len(queries) == 0

    def test_cluster_by_not_supported_for_streaming_tables(self):
        """Test that streaming tables don't generate CLUSTER BY statements."""
        builder = UCQueryBuilder()

        diff = TableDiff(
            cluster_by_changed=True,
            cluster_by_cols=["col1"],
            cluster_by_auto=False,
        )
        queries = builder.build("catalog.schema.st", diff, "streaming_table")

        assert len(queries) == 0


class TestConstraintQueries:
    """Tests for constraint SQL generation."""

    def test_add_primary_key(self):
        """Test adding a primary key constraint."""
        builder = UCQueryBuilder()

        pk = PrimaryKeyConstraint(name="pk_test", columns=["id"])
        pk_diff = ConstraintPKDiff(create=pk)
        diff = TableDiff(constraint_pk=pk_diff)
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert (
            queries[0] == "ALTER TABLE catalog.schema.table ADD CONSTRAINT pk_test PRIMARY KEY (id)"
        )

    def test_update_primary_key(self):
        """Test updating a primary key constraint (drop and recreate)."""
        builder = UCQueryBuilder()

        pk = PrimaryKeyConstraint(name="pk_test", columns=["id", "version"])
        pk_diff = ConstraintPKDiff(update=pk)
        diff = TableDiff(constraint_pk=pk_diff)
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 2
        assert "DROP CONSTRAINT pk_test" in queries[0]
        assert "ADD CONSTRAINT pk_test PRIMARY KEY (id, version)" in queries[1]

    def test_delete_primary_key(self):
        """Test dropping a primary key constraint."""
        builder = UCQueryBuilder()

        pk = PrimaryKeyConstraint(name="pk_test", columns=["id"])
        pk_diff = ConstraintPKDiff(delete=pk)
        diff = TableDiff(constraint_pk=pk_diff)
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 1
        assert queries[0] == "ALTER TABLE catalog.schema.table DROP CONSTRAINT pk_test"

    def test_add_foreign_key(self):
        """Test adding a foreign key constraint."""
        builder = UCQueryBuilder()

        fk = ForeignKeyConstraint(
            name="fk_user",
            columns=["user_id"],
            reference_table="users",
            reference_columns=["id"],
        )
        fk_diff = ConstraintFKDiff(create=[fk])
        diff = TableDiff(constraint_fk=fk_diff)
        queries = builder.build("catalog.schema.orders", diff, "managed")

        assert len(queries) == 1
        assert (
            "ALTER TABLE catalog.schema.orders ADD CONSTRAINT fk_user "
            "FOREIGN KEY (user_id) REFERENCES users (id)"
        ) in queries[0]

    def test_update_foreign_key(self):
        """Test updating a foreign key constraint (drop and recreate)."""
        builder = UCQueryBuilder()

        fk = ForeignKeyConstraint(
            name="fk_user",
            columns=["user_id"],
            reference_table="new_users",
            reference_columns=["id"],
        )
        fk_diff = ConstraintFKDiff(update=[fk])
        diff = TableDiff(constraint_fk=fk_diff)
        queries = builder.build("catalog.schema.orders", diff, "managed")

        assert len(queries) == 2
        assert "DROP CONSTRAINT fk_user" in queries[0]
        assert "ADD CONSTRAINT fk_user" in queries[1]
        assert "REFERENCES new_users" in queries[1]

    def test_delete_foreign_key(self):
        """Test dropping a foreign key constraint."""
        builder = UCQueryBuilder()

        fk = ForeignKeyConstraint(
            name="fk_user",
            columns=["user_id"],
            reference_table="users",
            reference_columns=["id"],
        )
        fk_diff = ConstraintFKDiff(delete=[fk])
        diff = TableDiff(constraint_fk=fk_diff)
        queries = builder.build("catalog.schema.orders", diff, "managed")

        assert len(queries) == 1
        assert queries[0] == "ALTER TABLE catalog.schema.orders DROP CONSTRAINT fk_user"

    def test_constraints_not_supported_for_views(self):
        """Test that views don't generate constraint statements."""
        builder = UCQueryBuilder()

        pk = PrimaryKeyConstraint(name="pk_test", columns=["id"])
        pk_diff = ConstraintPKDiff(create=pk)
        diff = TableDiff(constraint_pk=pk_diff)
        queries = builder.build("catalog.schema.my_view", diff, "view")

        # Should be empty because views don't support constraints
        assert len(queries) == 0


class TestComplexQueries:
    """Tests for complex scenarios with multiple changes."""

    def test_full_table_sync(self):
        """Test a complete table sync with all types of changes."""
        builder = UCQueryBuilder()

        diff = TableDiff(
            table_description="Updated description",
            table_tags=DictDiff(
                updates={"env": "prod"},
                deletes=["deprecated"],
            ),
            table_properties=DictDiff(
                updates={"delta.enableChangeDataFeed": "true"},
                deletes=["old.property"],
            ),
            columns={
                "id": ColumnDiff(
                    description="Primary key column",
                    tags=DictDiff(updates={"pii": "false"}),
                ),
                "email": ColumnDiff(
                    tags=DictDiff(
                        updates={"pii": "true"},
                        deletes=["old_tag"],
                    ),
                ),
            },
            cluster_by_changed=True,
            cluster_by_cols=["id"],
            cluster_by_auto=False,
            constraint_pk=ConstraintPKDiff(
                create=PrimaryKeyConstraint(name="pk_test", columns=["id"]),
            ),
        )

        queries = builder.build("catalog.schema.table", diff, "managed")

        # Should have multiple queries for all changes
        assert len(queries) > 5

        # Verify different types of statements are present
        assert any("COMMENT ON TABLE" in q for q in queries)
        assert any("SET TAGS" in q for q in queries)
        assert any("SET TBLPROPERTIES" in q for q in queries)
        assert any("COMMENT ON COLUMN" in q for q in queries)
        assert any("CLUSTER BY" in q for q in queries)
        assert any("ADD CONSTRAINT" in q for q in queries)

    def test_no_changes_produces_empty_list(self):
        """Test that no changes results in empty query list."""
        builder = UCQueryBuilder()

        diff = TableDiff()  # Empty diff
        queries = builder.build("catalog.schema.table", diff, "managed")

        assert len(queries) == 0

    def test_only_relevant_changes_for_table_type(self):
        """Test that only supported operations are generated for each table type."""
        builder = UCQueryBuilder()

        # Try to apply all changes to a view (most are unsupported)
        diff = TableDiff(
            table_description="View description",
            table_tags=DictDiff(updates={"tag": "value"}),
            table_properties=DictDiff(updates={"prop": "value"}),  # Not supported
            cluster_by_changed=True,  # Not supported
            cluster_by_cols=["col1"],
            constraint_pk=ConstraintPKDiff(  # Not supported
                create=PrimaryKeyConstraint(name="pk", columns=["id"]),
            ),
        )

        queries = builder.build("catalog.schema.my_view", diff, "view")

        # Should only have description and tags (properties, clustering, constraints skipped)
        assert len(queries) == 2  # Description + tag
        assert any("COMMENT ON VIEW" in q for q in queries)
        assert any("ALTER VIEW" in q and "SET TAGS" in q for q in queries)
        assert not any("TBLPROPERTIES" in q for q in queries)
        assert not any("CLUSTER BY" in q for q in queries)
        assert not any("CONSTRAINT" in q for q in queries)
