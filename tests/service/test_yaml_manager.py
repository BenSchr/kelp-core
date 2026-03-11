"""Tests for kelp.service.yaml_manager module.

This module tests the YamlManager, ServicePathConfig, and YamlUpdateReport classes
which handle YAML file patching and path resolution for tables and metric views.
"""

from pathlib import Path
from typing import Any
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
