"""Tests for Unity Catalog diff logic."""

from __future__ import annotations

from kelp.catalog.uc_diff import TableDiffCalculator
from kelp.catalog.uc_models import Column, RemoteCatalogConfig
from kelp.catalog.uc_models import Model as Table
from kelp.models.model import ForeignKeyConstraint, PrimaryKeyConstraint, TableType


class TestDictDiff:
    """Tests for diff_dicts method with different modes."""

    def test_add_new_tags_append_mode(self, append_config: RemoteCatalogConfig):
        """Test adding new tags in append mode."""
        differ = TableDiffCalculator(append_config)

        local = {"env": "dev", "team": "data"}
        remote = {"env": "prod"}

        result = differ.diff_dicts(local, remote, [], "append")

        assert result.updates == {"env": "dev", "team": "data"}
        assert result.deletes == []

    def test_remove_tags_append_mode(self, append_config: RemoteCatalogConfig):
        """Test that tags are NOT removed in append mode."""
        differ = TableDiffCalculator(append_config)

        local = {"env": "dev"}
        remote = {"env": "dev", "deprecated": "true"}

        result = differ.diff_dicts(local, remote, [], "append")

        assert result.updates == {}
        assert result.deletes == []  # No deletions in append mode

    def test_replace_mode_adds_and_removes(self, replace_config: RemoteCatalogConfig):
        """Test that replace mode adds new and removes old tags."""
        differ = TableDiffCalculator(replace_config)

        local = {"env": "dev", "owner": "team_a"}
        remote = {"env": "prod", "deprecated": "true"}

        result = differ.diff_dicts(local, remote, [], "replace")

        assert result.updates == {"env": "dev", "owner": "team_a"}
        assert "deprecated" in result.deletes

    def test_managed_mode_only_removes_managed_keys(self, managed_config: RemoteCatalogConfig):
        """Test that managed mode only removes keys in the managed list."""
        differ = TableDiffCalculator(managed_config)

        local = {"environment": "dev"}
        remote = {"environment": "prod", "owner": "old_team", "custom_tag": "value"}

        result = differ.diff_dicts(local, remote, ["environment", "owner"], "managed")

        assert result.updates == {"environment": "dev"}
        # Only "owner" should be deleted (it's in managed list and not in local)
        assert "owner" in result.deletes
        # "custom_tag" should NOT be deleted (not in managed list)
        assert "custom_tag" not in result.deletes

    def test_empty_dicts(self, default_config: RemoteCatalogConfig):
        """Test diff with empty dictionaries."""
        differ = TableDiffCalculator(default_config)

        result = differ.diff_dicts({}, {}, [], "replace")

        assert result.updates == {}
        assert result.deletes == []

    def test_identical_dicts(self, default_config: RemoteCatalogConfig):
        """Test diff with identical dictionaries."""
        differ = TableDiffCalculator(default_config)

        tags = {"env": "dev", "owner": "team"}
        result = differ.diff_dicts(tags, tags, [], "replace")

        assert result.updates == {}
        assert result.deletes == []


class TestTableDescriptionDiff:
    """Tests for table description changes."""

    def test_description_changed(self, default_config: RemoteCatalogConfig):
        """Test detecting description changes."""
        differ = TableDiffCalculator(default_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            description="New description",
            columns=[],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            description="Old description",
            columns=[],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert diff.table_description == "New description"

    def test_description_unchanged(self, default_config: RemoteCatalogConfig):
        """Test when description hasn't changed."""
        differ = TableDiffCalculator(default_config)

        desc = "Same description"
        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            description=desc,
            columns=[],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            description=desc,
            columns=[],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert diff.table_description is None


class TestTableTagsDiff:
    """Tests for table-level tag differences."""

    def test_table_tags_added_replace_mode(
        self,
        replace_config: RemoteCatalogConfig,
        sample_local_table: Table,
        empty_remote_table: Table,
    ):
        """Test adding table tags in replace mode."""
        differ = TableDiffCalculator(replace_config)

        diff = differ.calculate(sample_local_table, empty_remote_table)

        assert diff.table_tags.updates == {"environment": "dev", "owner": "data_team"}
        assert diff.table_tags.deletes == []

    def test_table_tags_removed_replace_mode(self, replace_config: RemoteCatalogConfig):
        """Test removing table tags in replace mode."""
        differ = TableDiffCalculator(replace_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            tags={"env": "dev"},
            columns=[],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            tags={"env": "dev", "deprecated": "true", "old_tag": "value"},
            columns=[],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert "deprecated" in diff.table_tags.deletes
        assert "old_tag" in diff.table_tags.deletes

    def test_table_tags_append_mode(self, append_config: RemoteCatalogConfig):
        """Test table tags in append mode don't remove existing tags."""
        differ = TableDiffCalculator(append_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            tags={"env": "dev"},
            columns=[],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            tags={"env": "prod", "keep_this": "yes"},
            columns=[],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert diff.table_tags.updates == {"env": "dev"}
        assert diff.table_tags.deletes == []  # Append mode never deletes

    def test_table_tags_managed_mode(self, managed_config: RemoteCatalogConfig):
        """Test managed mode with specific managed tag keys."""
        differ = TableDiffCalculator(managed_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            tags={"environment": "dev"},
            columns=[],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            tags={
                "environment": "prod",
                "owner": "old_owner",
                "custom": "keep_me",
            },
            columns=[],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert diff.table_tags.updates == {"environment": "dev"}
        # Only "owner" should be deleted (it's managed and not in local)
        assert "owner" in diff.table_tags.deletes
        # "custom" should not be deleted (not in managed list)
        assert "custom" not in diff.table_tags.deletes


class TestTablePropertiesDiff:
    """Tests for table property differences."""

    def test_properties_added_append_mode(self, append_config: RemoteCatalogConfig):
        """Test adding properties in append mode."""
        differ = TableDiffCalculator(append_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            table_properties={"delta.enableChangeDataFeed": "true", "new_prop": "value"},
            columns=[],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            table_properties={},
            columns=[],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert diff.table_properties.updates == {
            "delta.enableChangeDataFeed": "true",
            "new_prop": "value",
        }
        assert diff.table_properties.deletes == []

    def test_properties_not_removed_append_mode(self, append_config: RemoteCatalogConfig):
        """Test that existing properties are not removed in append mode."""
        differ = TableDiffCalculator(append_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            table_properties={"prop1": "value1"},
            columns=[],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            table_properties={"prop1": "value1", "prop2": "value2"},
            columns=[],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert diff.table_properties.updates == {}
        assert diff.table_properties.deletes == []

    def test_properties_managed_mode(self, managed_config: RemoteCatalogConfig):
        """Test managed mode for properties."""
        differ = TableDiffCalculator(managed_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            table_properties={"delta.enableChangeDataFeed": "true"},
            columns=[],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            table_properties={
                "delta.enableChangeDataFeed": "false",
                "delta.autoOptimize": "true",
            },
            columns=[],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert diff.table_properties.updates == {"delta.enableChangeDataFeed": "true"}
        # delta.autoOptimize should NOT be deleted (not in managed list)
        assert "delta.autoOptimize" not in diff.table_properties.deletes

    def test_properties_managed_mode_ignores_unmanaged_local_keys(self):
        """Test that managed mode only updates properties in the managed list."""
        config = RemoteCatalogConfig(
            table_property_mode="managed",
            managed_table_properties=["delta.enableChangeDataFeed"],
        )
        differ = TableDiffCalculator(config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            table_properties={
                "delta.enableChangeDataFeed": "true",
                "unmanaged.prop": "should_be_ignored",
            },
            columns=[],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            table_properties={
                "delta.enableChangeDataFeed": "false",
                "other.remote.prop": "value",
            },
            columns=[],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        # Only managed key should be in updates
        assert diff.table_properties.updates == {"delta.enableChangeDataFeed": "true"}
        assert "unmanaged.prop" not in diff.table_properties.updates
        # Remote-only keys not in managed list should not be deleted
        assert "other.remote.prop" not in diff.table_properties.deletes


class TestColumnDiff:
    """Tests for column-level differences."""

    def test_column_description_changed(self, default_config: RemoteCatalogConfig):
        """Test detecting column description changes."""
        differ = TableDiffCalculator(default_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[
                Column(name="id", data_type="INT", description="New description"),
            ],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[
                Column(name="id", data_type="INT", description="Old description"),
            ],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert "id" in diff.columns
        assert diff.columns["id"].description == "New description"

    def test_column_tags_replace_mode(self, replace_config: RemoteCatalogConfig):
        """Test column tags in replace mode."""
        differ = TableDiffCalculator(replace_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[
                Column(name="email", data_type="STRING", tags={"pii": "true"}),
            ],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[
                Column(
                    name="email",
                    data_type="STRING",
                    tags={"pii": "false", "deprecated": "true"},
                ),
            ],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert "email" in diff.columns
        assert diff.columns["email"].tags is not None
        assert diff.columns["email"].tags.updates == {"pii": "true"}
        assert "deprecated" in diff.columns["email"].tags.deletes

    def test_column_tags_managed_mode(self, managed_config: RemoteCatalogConfig):
        """Test column tags with managed mode."""
        differ = TableDiffCalculator(managed_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[
                Column(name="email", data_type="STRING", tags={"pii": "true"}),
            ],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[
                Column(
                    name="email",
                    data_type="STRING",
                    tags={"pii": "false", "sensitive": "true", "custom": "keep"},
                ),
            ],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert "email" in diff.columns
        assert diff.columns["email"].tags is not None
        assert diff.columns["email"].tags.updates == {"pii": "true"}
        # "sensitive" is managed and not in local, so it should be deleted
        assert "sensitive" in diff.columns["email"].tags.deletes
        # "custom" is not managed, so it should NOT be deleted
        assert "custom" not in diff.columns["email"].tags.deletes

    def test_column_only_in_remote(self, default_config: RemoteCatalogConfig):
        """Test that columns only in remote are ignored."""
        differ = TableDiffCalculator(default_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[
                Column(name="id", data_type="INT"),
            ],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[
                Column(name="id", data_type="INT"),
                Column(name="deleted_col", data_type="STRING", description="Should be ignored"),
            ],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        # deleted_col should not appear in diff
        assert "deleted_col" not in diff.columns


class TestConstraintDiff:
    """Tests for constraint differences."""

    def test_primary_key_changed(self, default_config: RemoteCatalogConfig):
        """Test detecting primary key column changes."""
        differ = TableDiffCalculator(default_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[Column(name="id", data_type="INT")],
            constraints=[PrimaryKeyConstraint(name="pk_test", columns=["id"])],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[Column(name="id", data_type="INT")],
            constraints=[PrimaryKeyConstraint(name="pk_test", columns=["old_id"])],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert diff.constraint_pk.update is not None
        assert diff.constraint_pk.update.columns == ["id"]

    def test_primary_key_added(self, default_config: RemoteCatalogConfig):
        """Test adding a new primary key."""
        differ = TableDiffCalculator(default_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[Column(name="id", data_type="INT")],
            constraints=[PrimaryKeyConstraint(name="pk_test", columns=["id"])],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[Column(name="id", data_type="INT")],
            constraints=[],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert diff.constraint_pk.create is not None
        assert diff.constraint_pk.create.columns == ["id"]

    def test_primary_key_deleted(self, default_config: RemoteCatalogConfig):
        """Test deleting a primary key."""
        differ = TableDiffCalculator(default_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[Column(name="id", data_type="INT")],
            constraints=[],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[Column(name="id", data_type="INT")],
            constraints=[PrimaryKeyConstraint(name="pk_test", columns=["id"])],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert diff.constraint_pk.delete is not None
        assert diff.constraint_pk.delete.name == "pk_test"

    def test_foreign_key_changes(self, default_config: RemoteCatalogConfig):
        """Test foreign key constraint changes."""
        differ = TableDiffCalculator(default_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[Column(name="user_id", data_type="INT")],
            constraints=[
                ForeignKeyConstraint(
                    name="fk_user",
                    columns=["user_id"],
                    reference_table="users",
                    reference_columns=["id"],
                ),
            ],
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[Column(name="user_id", data_type="INT")],
            constraints=[
                ForeignKeyConstraint(
                    name="fk_user",
                    columns=["user_id"],
                    reference_table="old_users",
                    reference_columns=["id"],
                ),
            ],
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert len(diff.constraint_fk.update) == 1
        fk = diff.constraint_fk.update[0]
        assert isinstance(fk, ForeignKeyConstraint)
        assert fk.reference_table == "users"


class TestClusterByDiff:
    """Tests for clustering changes."""

    def test_cluster_by_columns_changed(self, default_config: RemoteCatalogConfig):
        """Test detecting cluster by column changes."""
        differ = TableDiffCalculator(default_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[Column(name="id", data_type="INT")],
            cluster_by=["id"],
            cluster_by_auto=False,
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[Column(name="id", data_type="INT")],
            cluster_by=["created_at"],
            cluster_by_auto=False,
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert diff.cluster_by_changed is True
        assert diff.cluster_by_cols == ["id"]

    def test_cluster_by_auto_changed(self, default_config: RemoteCatalogConfig):
        """Test detecting cluster by auto changes."""
        differ = TableDiffCalculator(default_config)

        local = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[Column(name="id", data_type="INT")],
            cluster_by=[],
            cluster_by_auto=True,
            table_type=TableType.MANAGED,
        )
        remote = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            columns=[Column(name="id", data_type="INT")],
            cluster_by=[],
            cluster_by_auto=False,
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(local, remote)

        assert diff.cluster_by_changed is True
        assert diff.cluster_by_auto is True


class TestComplexScenarios:
    """Tests for complex scenarios with multiple changes."""

    def test_full_sync_scenario(
        self,
        replace_config: RemoteCatalogConfig,
        sample_local_table: Table,
        sample_remote_table: Table,
    ):
        """Test a complete sync scenario with multiple changes."""
        differ = TableDiffCalculator(replace_config)

        diff = differ.calculate(sample_local_table, sample_remote_table)

        # Check description changed
        assert diff.table_description == "Test table description"

        # Check table tags updated and removed
        assert "environment" in diff.table_tags.updates
        assert "owner" in diff.table_tags.updates
        assert "deprecated" in diff.table_tags.deletes

        # Check table properties
        assert diff.table_properties.updates == {"delta.enableChangeDataFeed": "true"}

        # Check column changes
        assert "id" in diff.columns
        assert diff.columns["id"].description == "Primary key"

        # Check cluster by changed
        assert diff.cluster_by_changed is True
        assert diff.cluster_by_cols == ["id"]

    def test_no_changes_scenario(self, default_config: RemoteCatalogConfig):
        """Test when local and remote are identical."""
        differ = TableDiffCalculator(default_config)

        table = Table(
            name="test",
            catalog="cat",
            schema_="sch",
            description="Same",
            tags={"env": "dev"},
            table_properties={"prop": "value"},
            columns=[
                Column(name="id", data_type="INT", description="ID", tags={"pii": "false"}),
            ],
            constraints=[],
            cluster_by=["id"],
            cluster_by_auto=False,
            table_type=TableType.MANAGED,
        )

        diff = differ.calculate(table, table)

        assert diff.table_description is None
        assert not diff.table_tags.has_changes
        assert not diff.table_properties.has_changes
        assert len(diff.columns) == 0
        assert not diff.cluster_by_changed
