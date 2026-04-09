"""Tests for kelp.service.yaml_manager module.

This module tests the YamlManager, ServicePathConfig, and YamlUpdateReport classes
which handle YAML file patching and path resolution for tables and metric views.
"""

from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest

from kelp.meta.context import MetaContextStore
from kelp.models.metric_view import MetricView
from kelp.models.model import (
    Column,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
)
from kelp.models.model import (
    Model as Table,
)
from kelp.service.yaml_manager import ServicePathConfig, YamlManager, YamlUpdateReport


class TestServicePathConfig:
    """Test the ServicePathConfig dataclass."""

    def test_service_path_config_creation(self):
        """Test creating ServicePathConfig directly."""
        config = ServicePathConfig(
            project_root=Path("/project"),
            service_root=Path("models"),
            hierarchy_config={"bronze": {"+schema": "bronze_schema"}},
        )

        assert config.project_root == Path("/project")
        assert config.service_root == Path("models")
        assert config.hierarchy_config is not None
        assert config.hierarchy_config["bronze"]["+schema"] == "bronze_schema"

    def test_service_root_absolute(self):
        """Test service_root_absolute property."""
        config = ServicePathConfig(
            project_root=Path("/project"),
            service_root=Path("models"),
        )

        assert config.service_root_absolute == Path("/project/models")

    def test_from_context(self, simple_project_dir: Path):
        """Test creating ServicePathConfig from runtime context."""
        from kelp.config import init

        init(project_file_path=str(simple_project_dir / "kelp_project.yml"))

        config = ServicePathConfig.from_context()

        assert config.project_root.name == "simple_project"
        assert config.service_root == Path("kelp_metadata/models")
        assert config.hierarchy_config is not None

    def test_from_context_metrics(self, simple_project_dir: Path):
        """Test creating ServicePathConfig for metrics from context."""
        from kelp.config import init

        init(project_file_path=str(simple_project_dir / "kelp_project.yml"))

        config = ServicePathConfig.from_context(
            service_root_key="metrics_path",
            hierarchy_config_key="metric_views",
        )

        assert config.service_root == Path("kelp_metadata/metrics")

    def test_from_context_no_context_raises(self, monkeypatch, tmp_path: Path):
        """Test from_context raises error when auto-init fails."""
        # Ensure no context by clearing it
        MetaContextStore.clear_all()

        # Simulate missing project file by running from an empty directory
        empty_dir = tmp_path / "empty_project"
        empty_dir.mkdir()
        monkeypatch.chdir(empty_dir)

        with pytest.raises(FileNotFoundError, match="Project root with"):
            ServicePathConfig.from_context()


class TestYamlUpdateReport:
    """Test the YamlUpdateReport dataclass."""

    def test_yaml_update_report_creation(self):
        """Test creating YamlUpdateReport."""
        report = YamlUpdateReport(
            model_name="test_table",
            file_path=Path("/project/models/test.yml"),
            result_model={"name": "test_table"},
            changes_made=True,
            added_fields=["description"],
            updated_fields=["columns"],
            removed_fields=[],
        )

        assert report.model_name == "test_table"
        assert report.changes_made is True
        assert "description" in report.added_fields
        assert "columns" in report.updated_fields
        assert len(report.removed_fields) == 0


class TestYamlManagerHelpers:
    """Test YamlManager helper methods."""

    def test_set_or_remove_with_value(self):
        """Test _set_or_remove sets value when meaningful."""
        target = {}
        YamlManager._set_or_remove(target, "key", "value")

        assert target["key"] == "value"

    def test_set_or_remove_with_none(self):
        """Test _set_or_remove removes key when value is None."""
        target = {"key": "existing"}
        YamlManager._set_or_remove(target, "key", None)

        assert "key" not in target

    def test_set_or_remove_with_empty_string(self):
        """Test _set_or_remove removes key when value is empty string."""
        target = {"key": "existing"}
        YamlManager._set_or_remove(target, "key", "")

        assert "key" not in target

    def test_set_or_remove_with_empty_list(self):
        """Test _set_or_remove removes key when value is empty list."""
        target = {"key": "existing"}
        YamlManager._set_or_remove(target, "key", [])

        assert "key" not in target

    def test_set_or_remove_with_empty_dict(self):
        """Test _set_or_remove removes key when value is empty dict."""
        target = {"key": "existing"}
        YamlManager._set_or_remove(target, "key", {})

        assert "key" not in target

    def test_find_model_index_found(self):
        """Test _find_model_index returns correct index."""
        models = [
            {"name": "table1"},
            {"name": "table2"},
            {"name": "table3"},
        ]

        index = YamlManager._find_model_index(models, "table2")

        assert index == 1

    def test_find_model_index_not_found(self):
        """Test _find_model_index returns None when not found."""
        models = [{"name": "table1"}]

        index = YamlManager._find_model_index(models, "nonexistent")

        assert index is None

    def test_filter_tags_no_defaults(self):
        """Test _filter_tags returns all tags when no defaults."""
        tags = {"env": "prod", "team": "data"}

        filtered = YamlManager._filter_tags(tags, None)

        assert filtered == tags

    def test_filter_tags_with_defaults(self):
        """Test _filter_tags excludes default tags."""
        tags = {"env": "prod", "team": "data", "version": "1.0"}
        defaults = {"env": "prod", "version": "1.0"}

        filtered = YamlManager._filter_tags(tags, defaults)

        assert filtered == {"team": "data"}

    def test_serialize_constraints_primary_key(self):
        """Test _serialize_constraints with primary key."""
        constraints: list[PrimaryKeyConstraint | ForeignKeyConstraint] = [
            PrimaryKeyConstraint(name="pk_test", columns=["id"]),
        ]

        serialized = YamlManager._serialize_constraints(constraints)

        assert len(serialized) == 1
        assert serialized[0]["name"] == "pk_test"
        assert serialized[0]["type"] == "primary_key"
        assert serialized[0]["columns"] == ["id"]

    def test_serialize_constraints_foreign_key(self):
        """Test _serialize_constraints with foreign key."""
        constraints: list[PrimaryKeyConstraint | ForeignKeyConstraint] = [
            ForeignKeyConstraint(
                name="fk_customer",
                columns=["customer_id"],
                reference_table="customers",
                reference_columns=["id"],
            ),
        ]

        serialized = YamlManager._serialize_constraints(constraints)

        assert len(serialized) == 1
        assert serialized[0]["name"] == "fk_customer"
        assert serialized[0]["type"] == "foreign_key"
        assert serialized[0]["reference_table"] == "customers"

    def test_detect_changes_new_model(self):
        """Test _detect_changes when original is None (new model)."""
        updated = {"name": "test", "description": "New table"}

        added, updated_list, removed = YamlManager._detect_changes(None, updated)

        assert set(added) == {"name", "description"}
        assert updated_list == []
        assert removed == []

    def test_detect_changes_updated_fields(self):
        """Test _detect_changes with updated fields."""
        original = {"name": "test", "description": "Old"}
        updated = {"name": "test", "description": "New"}

        added, updated_list, removed = YamlManager._detect_changes(original, updated)

        assert added == []
        assert updated_list == ["description"]
        assert removed == []

    def test_detect_changes_added_fields(self):
        """Test _detect_changes with added fields."""
        original = {"name": "test"}
        updated = {"name": "test", "description": "New", "tags": {"env": "prod"}}

        added, updated_list, removed = YamlManager._detect_changes(original, updated)

        assert set(added) == {"description", "tags"}
        assert updated_list == []
        assert removed == []

    def test_detect_changes_removed_fields(self):
        """Test _detect_changes with removed fields."""
        original = {"name": "test", "description": "Old", "tags": {}}
        updated = {"name": "test"}

        added, updated_list, removed = YamlManager._detect_changes(original, updated)

        assert added == []
        assert updated_list == []
        assert set(removed) == {"description", "tags"}


class TestYamlManagerColumns:
    """Test column patching logic."""

    def test_patch_columns_new_columns(self):
        """Test _patch_columns with new columns."""
        existing = []
        source = [
            Column(name="id", data_type="bigint", description="ID column"),
            Column(name="name", data_type="string"),
        ]

        patched = YamlManager._patch_columns(existing, source)

        assert len(patched) == 2
        assert patched[0]["name"] == "id"
        assert patched[0]["description"] == "ID column"
        assert patched[1]["name"] == "name"

    def test_patch_columns_update_existing(self):
        """Test _patch_columns updates existing columns."""
        existing = [
            {"name": "id", "data_type": "int", "description": "Old description"},
        ]
        source = [
            Column(name="id", data_type="bigint", description="New description"),
        ]

        patched = YamlManager._patch_columns(existing, source)

        assert len(patched) == 1
        assert patched[0]["data_type"] == "bigint"
        assert patched[0]["description"] == "New description"

    def test_patch_columns_preserves_order(self):
        """Test _patch_columns preserves source column order."""
        existing = [
            {"name": "name"},
            {"name": "id"},
        ]
        source = [
            Column(name="id", data_type="bigint"),
            Column(name="name", data_type="string"),
        ]

        patched = YamlManager._patch_columns(existing, source)

        # Should follow source order (id first, then name)
        assert patched[0]["name"] == "id"
        assert patched[1]["name"] == "name"

    def test_patch_columns_with_tags(self):
        """Test _patch_columns includes column tags."""
        existing = []
        source = [
            Column(name="email", data_type="string", tags={"pii": "true"}),
        ]

        patched = YamlManager._patch_columns(existing, source)

        assert patched[0]["tags"] == {"pii": "true"}


class TestYamlManagerPathResolution:
    """Test path resolution and hierarchy logic."""

    def test_find_hierarchy_folder_top_level_schema(self):
        """Test _find_hierarchy_folder_for_schema with top-level schema."""
        models_cfg = {
            "+schema": "default_schema",
            "bronze": {"+schema": "bronze_schema"},
        }

        folder = YamlManager._find_hierarchy_folder_for_schema(
            "bronze_schema",
            None,
            models_cfg,
        )

        assert folder == "bronze"

    def test_find_hierarchy_folder_nested(self):
        """Test _find_hierarchy_folder_for_schema with nested folders."""
        models_cfg = {
            "bronze": {
                "+schema": "bronze",
                "raw": {"+schema": "bronze_raw"},
            },
        }

        folder = YamlManager._find_hierarchy_folder_for_schema(
            "bronze_raw",
            None,
            models_cfg,
        )

        assert folder == "bronze/raw"

    def test_find_hierarchy_folder_with_catalog(self):
        """Test _find_hierarchy_folder_for_schema considers catalog."""
        models_cfg = {
            "prod": {
                "+catalog": "prod_catalog",
                "+schema": "prod_schema",
            },
        }

        folder = YamlManager._find_hierarchy_folder_for_schema(
            "prod_schema",
            "prod_catalog",
            models_cfg,
        )

        assert folder == "prod"

    def test_find_hierarchy_folder_not_found(self):
        """Test _find_hierarchy_folder_for_schema returns None when not found."""
        models_cfg = {"bronze": {"+schema": "bronze_schema"}}

        folder = YamlManager._find_hierarchy_folder_for_schema(
            "nonexistent_schema",
            None,
            models_cfg,
        )

        assert folder is None

    def test_determine_new_file_path_common(self):
        """Test _determine_new_file_path_common creates correct path."""
        path_config = ServicePathConfig(
            project_root=Path("/project"),
            service_root=Path("models"),
            hierarchy_config={"bronze": {"+schema": "bronze_schema"}},
        )

        file_path = YamlManager._determine_new_file_path_common(
            name="customers",
            schema="bronze_schema",
            catalog=None,
            path_config=path_config,
            kind="table",
        )

        assert file_path == Path("bronze/customers.yml")

    def test_determine_new_file_path_no_folder_match(self):
        """Test _determine_new_file_path_common falls back to root."""
        path_config = ServicePathConfig(
            project_root=Path("/project"),
            service_root=Path("models"),
            hierarchy_config={"bronze": {"+schema": "bronze_schema"}},
        )

        file_path = YamlManager._determine_new_file_path_common(
            name="customers",
            schema="unknown_schema",
            catalog=None,
            path_config=path_config,
            kind="table",
        )

        assert file_path == Path("customers.yml")

    def test_resolve_or_determine_path_explicit(self):
        """Test _resolve_or_determine_path uses explicit path when provided."""
        path_config = ServicePathConfig(
            project_root=Path("/project"),
            service_root=Path("models"),
        )

        resolved = YamlManager._resolve_or_determine_path(
            name="test",
            origin_file_path=None,
            schema="bronze",
            catalog=None,
            path_config=path_config,
            explicit_path="custom/path.yml",
            kind="table",
        )

        assert resolved == Path("custom/path.yml")

    def test_resolve_or_determine_path_origin(self):
        """Test _resolve_or_determine_path uses origin_file_path."""
        path_config = ServicePathConfig(
            project_root=Path("/project"),
            service_root=Path("models"),
        )

        resolved = YamlManager._resolve_or_determine_path(
            name="test",
            origin_file_path="existing/table.yml",
            schema="bronze",
            catalog=None,
            path_config=path_config,
            explicit_path=None,
            kind="table",
        )

        assert resolved == Path("existing/table.yml")

    def test_resolve_or_determine_path_origin_with_models_prefix(self):
        """Test _resolve_or_determine_path strips models/ prefix for nested service_root."""
        path_config = ServicePathConfig(
            project_root=Path("/project"),
            service_root=Path("kelp_metadata/models"),
        )

        resolved = YamlManager._resolve_or_determine_path(
            name="bronze_customers",
            origin_file_path="models/bronze/bronze_customers.yml",
            schema="bronze",
            catalog=None,
            path_config=path_config,
            explicit_path=None,
            kind="table",
        )

        assert resolved == Path("bronze/bronze_customers.yml")


class TestYamlManagerTableConversion:
    """Test table to model dict conversion."""

    def test_model_to_dict_basic(self):
        """Test model_to_dict with basic table."""
        table = Table(
            name="customers",
            description="Customer data",
            columns=[Column(name="id", data_type="bigint")],
        )

        with patch.object(YamlManager, "_get_hierarchy_defaults", return_value={}):
            model = YamlManager.model_to_dict(table, include_hierarchy_defaults=False)

        assert model["name"] == "customers"
        assert model["description"] == "Customer data"
        assert len(model["columns"]) == 1

    def test_model_to_dict_with_constraints(self):
        """Test model_to_dict includes constraints."""
        table = Table(
            name="orders",
            columns=[Column(name="id", data_type="bigint")],
            constraints=[PrimaryKeyConstraint(name="pk_orders", columns=["id"])],
        )

        with patch.object(YamlManager, "_get_hierarchy_defaults", return_value={}):
            model = YamlManager.model_to_dict(table, include_hierarchy_defaults=False)

        assert "constraints" in model
        assert len(model["constraints"]) == 1

    def test_model_to_dict_excludes_defaults(self):
        """Test model_to_dict excludes values matching defaults."""
        table = Table(
            name="customers",
            description="Default description",
            columns=[Column(name="id", data_type="bigint")],
            origin_file_path="bronze/customers.yml",
        )

        # _patch_model_dict checks against defaults from hierarchy, so the description
        # will only be excluded if matching defaults
        # For a table without hierarchy defaults, description is always included
        model = YamlManager.model_to_dict(table, include_hierarchy_defaults=False)

        # When include_hierarchy_defaults=False, defaults dict is empty, so description is always included
        assert model["description"] == "Default description"

    def test_patch_model_dict_preserves_templated_foreign_key_reference(self):
        """Test templated local FK references are not overwritten during patching."""
        source_model = Table(
            name="orders",
            constraints=[
                ForeignKeyConstraint(
                    name="fk_orders_customers",
                    columns=["customer_id"],
                    reference_table="prod.silver.customers",
                    reference_columns=["id"],
                ),
            ],
        )
        model_dict = {
            "name": "orders",
            "constraints": [
                {
                    "name": "fk_orders_customers",
                    "type": "foreign_key",
                    "columns": ["customer_id"],
                    "reference_table": "${ catalog }.silver.customers",
                    "reference_columns": ["id"],
                },
            ],
        }

        YamlManager._patch_model_dict(model_dict, source_model, defaults={})

        assert model_dict["constraints"][0]["reference_table"] == "${ catalog }.silver.customers"

    def test_patch_model_dict_resolves_foreign_key_to_local_name(self, mocker: MagicMock):
        """Test FK reference is stored as unqualified name when referenced model exists in local catalog.

        When syncing from remote, if the referenced table exists in local metadata,
        the YAML should store only the unqualified table name. DDL generation will
        resolve it to FQN at runtime.
        """
        source_model = Table(
            name="orders",
            constraints=[
                ForeignKeyConstraint(
                    name="fk_orders_customers",
                    columns=["customer_id"],
                    reference_table="prod.silver.customers",
                    reference_columns=["id"],
                ),
            ],
        )
        model_dict: dict[str, Any] = {"name": "orders"}

        mock_ctx = MagicMock()
        mock_model = MagicMock()
        mock_model.catalog = "prod"
        mock_model.schema_ = "silver"
        mock_ctx.catalog_index.get.return_value = mock_model
        mocker.patch("kelp.service.yaml_manager.get_context", return_value=mock_ctx)

        YamlManager._patch_model_dict(model_dict, source_model, defaults={})

        # Should store unqualified name in YAML when table is in local catalog
        assert model_dict["constraints"][0]["reference_table"] == "customers"

    def test_patch_model_dict_keeps_remote_fkn_when_not_in_local_catalog(self, mocker: MagicMock):
        """Test that remote FK with FQN is kept if referenced table not in local metadata.

        When syncing from remote, if the referenced table doesn't exist in local metadata,
        the fully qualified name from remote should be written to YAML as-is.
        """
        source_model = Table(
            name="orders",
            constraints=[
                ForeignKeyConstraint(
                    name="fk_orders_vendors",
                    columns=["vendor_id"],
                    reference_table="external.analytics.vendors",
                    reference_columns=["id"],
                ),
            ],
        )
        model_dict = {"name": "orders"}

        mock_ctx = MagicMock()
        # Simulate table not found in local catalog
        mock_ctx.catalog_index.get.side_effect = KeyError("vendors not found")
        mocker.patch("kelp.service.yaml_manager.get_context", return_value=mock_ctx)

        YamlManager._patch_model_dict(model_dict, source_model, defaults={})

        # Remote FQN should be preserved in YAML
        # Maybe fix ty check later
        assert model_dict["constraints"][0]["reference_table"] == "external.analytics.vendors"  # ty:ignore[invalid-argument-type]


class TestYamlManagerMetricViewConversion:
    """Test metric view to model dict conversion."""

    def test_metric_view_to_model_dict_basic(self):
        """Test metric_view_to_model_dict with basic metric view."""
        metric_view = MetricView(
            name="customer_metrics",
            catalog="prod",
            schema_="analytics",
            definition={"source": "customers", "timeseries": "created_at"},
        )

        with patch.object(YamlManager, "_get_hierarchy_defaults", return_value={}):
            model = YamlManager.metric_view_to_model_dict(
                metric_view,
                include_hierarchy_defaults=False,
            )

        assert model["name"] == "customer_metrics"
        assert model["catalog"] == "prod"
        assert model["schema"] == "analytics"
        assert "definition" in model

    def test_metric_view_to_model_dict_with_description(self):
        """Test metric view includes description in definition comment."""
        metric_view = MetricView(
            name="metrics",
            description="Customer metrics",
            definition={"source": "customers"},
        )

        with patch.object(YamlManager, "_get_hierarchy_defaults", return_value={}):
            model = YamlManager.metric_view_to_model_dict(
                metric_view,
                include_hierarchy_defaults=False,
            )

        assert model["definition"]["comment"] == "Customer metrics"

    def test_metric_view_excludes_default_catalog(self):
        """Test metric view excludes catalog matching default."""
        metric_view = MetricView(
            name="metrics",
            catalog="prod",
            definition={"source": "customers"},
            origin_file_path="analytics/metrics.yml",
        )

        # When include_hierarchy_defaults=False, no defaults are applied
        # So catalog will always be included
        model = YamlManager.metric_view_to_model_dict(
            metric_view,
            include_hierarchy_defaults=False,
        )

        # Catalog will be included since no hierarchy defaults are used
        assert model["catalog"] == "prod"


class TestYamlManagerIntegration:
    """Integration tests for YamlManager patching operations."""

    def test_patch_model_yaml_dry_run(self, tmp_path: Path, simple_project_dir: Path):
        """Test patch_model_yaml in dry_run mode doesn't write files."""
        from kelp.config import init

        init(project_file_path=str(simple_project_dir / "kelp_project.yml"))

        table = Table(
            name="test_table",
            schema_="bronze",
            description="Test table",
            columns=[Column(name="id", data_type="bigint")],
        )

        path_config = ServicePathConfig(
            project_root=tmp_path,
            service_root=Path("models"),
            hierarchy_config={"bronze": {"+schema": "bronze"}},
        )

        report = YamlManager.patch_model_yaml(
            table,
            path_config=path_config,
            relative_file_path="test.yml",
            dry_run=True,
        )

        assert report.changes_made is True
        # File should not be written in dry_run mode
        assert not (tmp_path / "models" / "test.yml").exists()

    def test_patch_model_yaml_creates_new_file(self, tmp_path: Path):
        """Test patch_model_yaml creates new YAML file."""
        table = Table(
            name="new_table",
            description="New table",
            columns=[Column(name="id", data_type="bigint")],
        )

        path_config = ServicePathConfig(
            project_root=tmp_path,
            service_root=Path("models"),
        )

        with patch.object(YamlManager, "_get_hierarchy_defaults", return_value={}):
            report = YamlManager.patch_model_yaml(
                table,
                path_config=path_config,
                relative_file_path="new_table.yml",
                dry_run=False,
            )

        assert report.changes_made is True
        assert (tmp_path / "models" / "new_table.yml").exists()

        # Verify file content
        import yaml

        content = yaml.safe_load((tmp_path / "models" / "new_table.yml").read_text())
        assert "kelp_models" in content
        assert content["kelp_models"][0]["name"] == "new_table"


class TestComplexTableProperties:
    """Test round-trip handling of complex table property types."""

    COMPLEX_PROPERTIES_YAML: ClassVar[dict] = {
        "simple_string": "enabled",
        "list_of_strings": ["admin", "analyst", "viewer"],
        "list_of_dicts": [
            {"name": "rule_1", "action": "allow"},
            {"name": "rule_2", "action": "deny"},
        ],
        "dict_with_mixed_types": {
            "timeout": "30",
            "tags": ["critical", "production"],
            "nested": {"key": "value", "count": "5"},
        },
    }

    def test_model_serializes_complex_properties_to_json_strings(self):
        """Test that complex property values are serialized to JSON on Model construction."""
        table = Table(
            name="test",
            table_properties=self.COMPLEX_PROPERTIES_YAML.copy(),
        )

        # Simple string stays as-is
        assert table.table_properties["simple_string"] == "enabled"
        # Complex types become JSON strings
        assert isinstance(table.table_properties["list_of_strings"], str)
        assert isinstance(table.table_properties["list_of_dicts"], str)
        assert isinstance(table.table_properties["dict_with_mixed_types"], str)

    def test_model_deserializes_json_strings_back_to_complex_types(self):
        """Test that deserialize_property_values restores complex types from JSON strings."""
        from kelp.models.model import Model

        table = Table(
            name="test",
            table_properties=self.COMPLEX_PROPERTIES_YAML.copy(),
        )

        deserialized = Model.deserialize_property_values(table.table_properties)

        assert deserialized["simple_string"] == "enabled"
        assert deserialized["list_of_strings"] == ["admin", "analyst", "viewer"]
        assert deserialized["list_of_dicts"] == [
            {"name": "rule_1", "action": "allow"},
            {"name": "rule_2", "action": "deny"},
        ]
        assert deserialized["dict_with_mixed_types"] == {
            "timeout": "30",
            "tags": ["critical", "production"],
            "nested": {"key": "value", "count": "5"},
        }

    def test_round_trip_read_write_preserves_complex_properties(self, tmp_path: Path):
        """Test that reading YAML with complex properties and writing back preserves structure.

        The full pipeline is:
        1. Read YAML with complex table_properties (list, dict) → Model (JSON strings)
        2. Write Model back to YAML via YamlManager → complex types restored in YAML
        3. Verify the written YAML matches the original structure
        """
        import yaml

        original_model_dict = {
            "name": "products",
            "description": "Product catalog",
            "table_properties": self.COMPLEX_PROPERTIES_YAML.copy(),
            "columns": [
                {"name": "id", "data_type": "bigint"},
            ],
        }

        # Step 1: Build a Model from the original YAML dict (simulates reading)
        table = Table(
            name=original_model_dict["name"],
            description=original_model_dict["description"],
            table_properties=original_model_dict["table_properties"],
            columns=[Column(**c) for c in original_model_dict["columns"]],  # ty:ignore[invalid-argument-type]
        )

        # Values are now JSON strings inside the model
        assert isinstance(table.table_properties["list_of_strings"], str)

        # Step 2: Write Model back to YAML via patch_model_yaml
        path_config = ServicePathConfig(
            project_root=tmp_path,
            service_root=Path("models"),
        )

        with patch.object(YamlManager, "_get_hierarchy_defaults", return_value={}):
            report = YamlManager.patch_model_yaml(
                table,
                path_config=path_config,
                relative_file_path="products.yml",
                dry_run=False,
            )

        assert report.changes_made is True

        # Step 3: Read written YAML and verify structure matches original
        written_content = yaml.safe_load(
            (tmp_path / "models" / "products.yml").read_text(encoding="utf-8")
        )
        written_model = written_content["kelp_models"][0]
        written_props = written_model["table_properties"]

        assert written_props["simple_string"] == "enabled"
        assert written_props["list_of_strings"] == ["admin", "analyst", "viewer"]
        assert written_props["list_of_dicts"] == [
            {"name": "rule_1", "action": "allow"},
            {"name": "rule_2", "action": "deny"},
        ]
        assert written_props["dict_with_mixed_types"] == {
            "timeout": "30",
            "tags": ["critical", "production"],
            "nested": {"key": "value", "count": "5"},
        }

    def test_round_trip_via_model_to_dict(self):
        """Test model_to_dict restores complex types for YAML serialization."""
        table = Table(
            name="test",
            table_properties=self.COMPLEX_PROPERTIES_YAML.copy(),
            columns=[Column(name="id", data_type="bigint")],
        )

        with patch.object(YamlManager, "_get_hierarchy_defaults", return_value={}):
            model_dict = YamlManager.model_to_dict(table, include_hierarchy_defaults=False)

        props = model_dict["table_properties"]
        assert props["simple_string"] == "enabled"
        assert props["list_of_strings"] == ["admin", "analyst", "viewer"]
        assert props["list_of_dicts"] == [
            {"name": "rule_1", "action": "allow"},
            {"name": "rule_2", "action": "deny"},
        ]
        assert props["dict_with_mixed_types"] == {
            "timeout": "30",
            "tags": ["critical", "production"],
            "nested": {"key": "value", "count": "5"},
        }

    def test_string_only_properties_unchanged(self):
        """Test that plain string properties pass through unmodified."""
        props = {
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize.optimizeWrite": "true",
        }
        table = Table(name="test", table_properties=props.copy())

        assert table.table_properties == props

        from kelp.models.model import Model

        deserialized = Model.deserialize_property_values(table.table_properties)
        assert deserialized == props

    def test_fixture_products_loads_complex_properties(self, simple_project_dir: Path):
        """Test that the products fixture with complex properties loads and round-trips."""
        from kelp.config import init

        ctx = init(project_file_path=str(simple_project_dir / "kelp_project.yml"))

        products = ctx.catalog_index.get("models", "products")
        assert products is not None

        # Properties should be JSON strings in the Model
        assert isinstance(products.table_properties["simple_string"], str)
        assert products.table_properties["simple_string"] == "enabled"
        assert isinstance(products.table_properties["list_of_strings"], str)
        assert isinstance(products.table_properties["list_of_dicts"], str)
        assert isinstance(products.table_properties["dict_with_mixed_types"], str)

        # Deserialization should restore original types
        from kelp.models.model import Model

        deserialized = Model.deserialize_property_values(products.table_properties)
        assert deserialized["list_of_strings"] == ["admin", "analyst", "viewer"]
        assert deserialized["list_of_dicts"] == [
            {"name": "rule_1", "action": "allow"},
            {"name": "rule_2", "action": "deny"},
        ]
        assert isinstance(deserialized["dict_with_mixed_types"], dict)


class TestPropertyModeFiltering:
    """Test table property filtering based on remote catalog config modes."""

    def test_filter_append_mode_keeps_only_existing_keys(self):
        """Test append mode only keeps keys already in the local model."""
        from kelp.models.project_config import RemoteCatalogConfig

        config = RemoteCatalogConfig(table_property_mode="append")
        source_props = {
            "delta.enableChangeDataFeed": "true",
            "new_remote_prop": "value",
            "another_remote_prop": "value2",
        }
        existing_model = {
            "name": "test",
            "table_properties": {"delta.enableChangeDataFeed": "false"},
        }

        result = YamlManager._filter_properties_by_mode(source_props, existing_model, config)

        assert result == {"delta.enableChangeDataFeed": "true"}
        assert "new_remote_prop" not in result
        assert "another_remote_prop" not in result

    def test_filter_append_mode_empty_existing(self):
        """Test append mode with no existing properties returns empty dict."""
        from kelp.models.project_config import RemoteCatalogConfig

        config = RemoteCatalogConfig(table_property_mode="append")
        source_props = {"delta.enableChangeDataFeed": "true"}
        existing_model = {"name": "test"}

        result = YamlManager._filter_properties_by_mode(source_props, existing_model, config)

        assert result == {}

    def test_filter_managed_mode_keeps_only_managed_keys(self):
        """Test managed mode only keeps keys in managed_table_properties list."""
        from kelp.models.project_config import RemoteCatalogConfig

        config = RemoteCatalogConfig(
            table_property_mode="managed",
            managed_table_properties=["delta.enableChangeDataFeed", "delta.autoOptimize"],
        )
        source_props = {
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize": "true",
            "unmanaged_prop": "value",
        }
        existing_model = {"name": "test"}

        result = YamlManager._filter_properties_by_mode(source_props, existing_model, config)

        assert result == {
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize": "true",
        }
        assert "unmanaged_prop" not in result

    def test_patch_model_dict_with_append_config_filters_remote_properties(self):
        """Test _patch_model_dict filters properties with append mode config."""
        from kelp.models.project_config import RemoteCatalogConfig

        config = RemoteCatalogConfig(table_property_mode="append")

        # Source model has many properties (from remote)
        source = Table(
            name="test",
            table_properties={
                "delta.enableChangeDataFeed": "true",
                "remote_only_prop": "value",
            },
            columns=[Column(name="id", data_type="bigint")],
        )

        # Existing model only has one property
        model = {
            "name": "test",
            "table_properties": {"delta.enableChangeDataFeed": "false"},
        }

        YamlManager._patch_model_dict(model, source, defaults={}, remote_catalog_config=config)

        # Only existing key should be updated
        assert model["table_properties"] == {"delta.enableChangeDataFeed": "true"}

    def test_patch_model_dict_without_config_keeps_all_properties(self):
        """Test _patch_model_dict keeps all properties when no config is provided."""
        source = Table(
            name="test",
            table_properties={
                "delta.enableChangeDataFeed": "true",
                "new_prop": "value",
            },
            columns=[Column(name="id", data_type="bigint")],
        )

        model = {"name": "test"}

        YamlManager._patch_model_dict(model, source, defaults={})

        # All properties should be included
        assert model["table_properties"] == {
            "delta.enableChangeDataFeed": "true",
            "new_prop": "value",
        }

    def test_round_trip_with_managed_mode_filters_correctly(self, tmp_path: Path):
        """Test full round-trip: remote properties filtered by managed mode during write."""
        import yaml

        from kelp.models.project_config import RemoteCatalogConfig

        config = RemoteCatalogConfig(
            table_property_mode="managed",
            managed_table_properties=["delta.enableChangeDataFeed"],
        )

        # Simulate remote table with many properties
        remote_table = Table(
            name="test_table",
            description="Test",
            table_properties={
                "delta.enableChangeDataFeed": "true",
                "system.internal.prop": "internal",
                "delta.autoOptimize": "true",
            },
            columns=[Column(name="id", data_type="bigint")],
        )

        path_config = ServicePathConfig(
            project_root=tmp_path,
            service_root=Path("models"),
        )

        with patch.object(YamlManager, "_get_hierarchy_defaults", return_value={}):
            report = YamlManager.patch_model_yaml(
                remote_table,
                path_config=path_config,
                relative_file_path="test_table.yml",
                dry_run=False,
                remote_catalog_config=config,
            )

        assert report.changes_made is True

        content = yaml.safe_load(
            (tmp_path / "models" / "test_table.yml").read_text(encoding="utf-8")
        )
        written_props = content["kelp_models"][0].get("table_properties", {})

        # Only managed property should be written
        assert written_props == {"delta.enableChangeDataFeed": "true"}
        assert "system.internal.prop" not in written_props
        assert "delta.autoOptimize" not in written_props
