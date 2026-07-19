"""Tests for Metric View DDL generation (spec-verbatim definitions)."""

import pytest

from kelp.catalog.query_builders.metric_view import (
    _create_tag_diff,
    _strip_managed_tags,
    generate_alter_metric_view_column_tags_ddl,
    generate_alter_metric_view_definition_ddl,
    generate_alter_metric_view_tags_ddl,
    generate_create_metric_view_ddl,
    generate_drop_metric_view_ddl,
)
from kelp.models.metric_view import MetricView


class TestStripManagedTags:
    """Tests for _strip_managed_tags — the only transformation applied to definitions."""

    def test_removes_tags_from_fields_dimensions_and_measures(self):
        definition = {
            "version": "1.1",
            "source": "cat.sch.orders",
            "fields": [{"name": "region", "expr": "region", "tags": {"pii": "false"}}],
            "dimensions": [{"name": "store", "expr": "store", "tags": {"a": "b"}}],
            "measures": [{"name": "revenue", "expr": "SUM(amount)", "tags": {"x": "y"}}],
        }

        stripped = _strip_managed_tags(definition)

        assert "tags" not in stripped["fields"][0]
        assert "tags" not in stripped["dimensions"][0]
        assert "tags" not in stripped["measures"][0]
        # Original is untouched
        assert definition["fields"][0]["tags"] == {"pii": "false"}

    def test_passes_everything_else_through_verbatim(self):
        """No normalization: comment, legacy keys, extra fields all stay as-is."""
        definition = {
            "source": "cat.sch.orders",
            "comment": "stays in the definition",
            "table": "legacy-key-passed-through",
            "filter": "region != 'test'",
            "joins": [{"name": "c", "source": "cat.sch.customers", "on": "c.id = orders.cid"}],
            "measures": [{"name": "m", "expr": "SUM(x)", "display_name": "Total"}],
        }

        assert _strip_managed_tags(definition) == definition

    def test_no_version_default_is_applied(self):
        definition = {"source": "cat.sch.orders"}

        assert "version" not in _strip_managed_tags(definition)


class TestCreateMetricViewDDL:
    """Tests for generate_create_metric_view_ddl function."""

    def test_basic_metric_view_creation(self):
        """Test basic metric view DDL generation."""
        metric_view = MetricView(
            name="sales_metrics",
            catalog="main",
            schema_="analytics",
            definition={
                "version": "1.1",
                "source": "main.raw.sales",
                "comment": "Sales metrics",
                "fields": [{"name": "category", "expr": "category"}],
                "measures": [{"name": "total_sales", "expr": "SUM(amount)"}],
            },
        )

        ddl = generate_create_metric_view_ddl(metric_view)

        assert "CREATE OR REPLACE VIEW main.analytics.sales_metrics" in ddl
        assert "WITH METRICS" in ddl
        assert "LANGUAGE YAML" in ddl
        assert "AS $$" in ddl
        assert "$$" in ddl
        assert "source: main.raw.sales" in ddl
        assert "comment: Sales metrics" in ddl
        assert "category" in ddl
        assert "total_sales" in ddl

    def test_definition_is_not_normalized(self):
        """Legacy keys are passed through untouched — no table/description mapping."""
        metric_view = MetricView(
            name="legacy_metrics",
            definition={
                "table": "cat.sch.orders",
                "metrics": [{"name": "m", "expression": "SUM(x)"}],
            },
        )

        ddl = generate_create_metric_view_ddl(metric_view)

        assert "table: cat.sch.orders" in ddl
        assert "source:" not in ddl
        assert "expression: SUM(x)" in ddl
        assert "version" not in ddl

    def test_tags_are_stripped_from_ddl(self):
        metric_view = MetricView(
            name="tagged",
            definition={
                "version": "1.1",
                "source": "cat.sch.orders",
                "fields": [{"name": "region", "expr": "region", "tags": {"pii": "false"}}],
            },
        )

        ddl = generate_create_metric_view_ddl(metric_view)

        assert "tags" not in ddl
        assert "pii" not in ddl

    def test_metric_view_without_catalog_schema(self):
        """Test metric view DDL without catalog/schema."""
        metric_view = MetricView(
            name="my_metrics",
            definition={
                "source": "sales",
                "measures": [{"name": "count", "expr": "COUNT(*)"}],
            },
        )

        ddl = generate_create_metric_view_ddl(metric_view)

        assert "CREATE OR REPLACE VIEW my_metrics" in ddl
        assert "main.analytics" not in ddl

    def test_requires_name(self):
        """Test that name is required."""
        metric_view = MetricView(
            name="",
            definition={"source": "table"},
        )

        with pytest.raises(ValueError, match="Metric view name is required"):
            generate_create_metric_view_ddl(metric_view)

    def test_requires_definition(self):
        """Test that definition is required."""
        metric_view = MetricView(
            name="test_view",
            definition={},
        )

        with pytest.raises(ValueError, match="must have a definition"):
            generate_create_metric_view_ddl(metric_view)

    def test_yaml_format_in_ddl(self):
        """Test that definition is properly formatted as YAML."""
        metric_view = MetricView(
            name="test_metrics",
            catalog="cat",
            schema_="sch",
            definition={
                "version": "1.1",
                "source": "cat.sch.sales",
                "fields": [
                    {"name": "category", "expr": "category"},
                    {"name": "region", "expr": "region"},
                ],
                "measures": [
                    {"name": "revenue", "expr": "SUM(amount)"},
                    {"name": "count", "expr": "COUNT(*)"},
                ],
            },
        )

        ddl = generate_create_metric_view_ddl(metric_view)

        # Should have proper YAML structure
        assert "fields:" in ddl
        assert "- name: category" in ddl
        assert "- name: region" in ddl
        assert "measures:" in ddl
        assert "- name: revenue" in ddl
        assert "expr: SUM(amount)" in ddl


class TestDropMetricViewDDL:
    """Tests for generate_drop_metric_view_ddl function."""

    def test_drop_metric_view(self):
        """Test DROP VIEW DDL generation."""
        metric_view = MetricView(
            name="sales_metrics",
            catalog="main",
            schema_="analytics",
            definition={"source": "table"},
        )

        ddl = generate_drop_metric_view_ddl(metric_view)

        assert ddl == "DROP VIEW IF EXISTS main.analytics.sales_metrics"

    def test_drop_metric_view_without_catalog(self):
        """Test DROP VIEW without catalog/schema."""
        metric_view = MetricView(
            name="my_metrics",
            definition={"source": "table"},
        )

        ddl = generate_drop_metric_view_ddl(metric_view)

        assert ddl == "DROP VIEW IF EXISTS my_metrics"


class TestAlterMetricViewTagsDDL:
    """Tests for generate_alter_metric_view_tags_ddl function."""

    def test_set_single_tag(self):
        """Test setting a single tag on metric view."""
        metric_view = MetricView(
            name="metrics",
            catalog="cat",
            schema_="sch",
            definition={"source": "table"},
        )

        tags = {"environment": "prod"}
        statements = generate_alter_metric_view_tags_ddl(metric_view, tags)

        assert len(statements) == 1
        assert statements[0] == "ALTER VIEW cat.sch.metrics SET TAGS ('environment' = 'prod')"

    def test_set_multiple_tags(self):
        """Test setting multiple tags on metric view."""
        metric_view = MetricView(
            name="metrics",
            catalog="cat",
            schema_="sch",
            definition={"source": "table"},
        )

        tags = {"environment": "prod", "owner": "data_team"}
        statements = generate_alter_metric_view_tags_ddl(metric_view, tags)

        assert len(statements) == 2
        assert any("'environment' = 'prod'" in s for s in statements)
        assert any("'owner' = 'data_team'" in s for s in statements)

    def test_empty_tags(self):
        """Test with empty tags dict."""
        metric_view = MetricView(
            name="metrics",
            catalog="cat",
            schema_="sch",
            definition={"source": "table"},
        )

        statements = generate_alter_metric_view_tags_ddl(metric_view, {})

        assert len(statements) == 0

    def test_tag_value_escaping(self):
        """Test that tag values with quotes are escaped."""
        metric_view = MetricView(
            name="metrics",
            catalog="cat",
            schema_="sch",
            definition={"source": "table"},
        )

        tags = {"description": "It's great"}
        statements = generate_alter_metric_view_tags_ddl(metric_view, tags)

        assert len(statements) == 1
        assert "'description' = 'It''s great'" in statements[0]


class TestAlterMetricViewDefinitionDDL:
    """Tests for generate_alter_metric_view_definition_ddl function."""

    def test_alter_metric_view_definition(self):
        """Test ALTER VIEW statement for updating definition."""
        metric_view = MetricView(
            name="sales_metrics",
            catalog="main",
            schema_="analytics",
            definition={
                "version": "1.1",
                "source": "main.raw.sales",
                "comment": "Updated description",
                "fields": [{"name": "category", "expr": "category"}],
                "measures": [{"name": "revenue", "expr": "SUM(amount)"}],
            },
        )

        ddl = generate_alter_metric_view_definition_ddl(metric_view)

        assert "ALTER VIEW main.analytics.sales_metrics" in ddl
        assert "AS $$" in ddl
        assert "$$" in ddl
        assert "source: main.raw.sales" in ddl
        assert "comment: Updated description" in ddl

    def test_requires_name_for_alter(self):
        """Test that name is required for ALTER."""
        metric_view = MetricView(
            name="",
            definition={"source": "table"},
        )

        with pytest.raises(ValueError, match="Metric view name is required"):
            generate_alter_metric_view_definition_ddl(metric_view)

    def test_requires_definition_for_alter(self):
        """Test that definition is required for ALTER."""
        metric_view = MetricView(
            name="test_view",
            definition={},
        )

        with pytest.raises(ValueError, match="must have a definition"):
            generate_alter_metric_view_definition_ddl(metric_view)


class TestCreateTagDiff:
    """Tests for _create_tag_diff helper function."""

    def test_creates_for_new_tags(self):
        """Test that new tags are in creates."""
        local = {"tag1": "value1", "tag2": "value2"}
        remote = {}

        diff = _create_tag_diff(local, remote)

        assert diff.creates == {"tag1": "value1", "tag2": "value2"}
        assert not diff.deletes

    def test_updates_for_changed_tags(self):
        """Test that changed tags are in updates."""
        local = {"tag1": "new_value"}
        remote = {"tag1": "old_value"}

        diff = _create_tag_diff(local, remote)

        assert diff.updates == {"tag1": "new_value"}

    def test_deletes_for_removed_tags(self):
        """Test that removed tags are in deletes."""
        local = {}
        remote = {"tag1": "value1", "tag2": "value2"}

        diff = _create_tag_diff(local, remote)

        assert set(diff.deletes) == {"tag1", "tag2"}

    def test_no_changes_when_identical(self):
        """Test that identical tags produce no changes."""
        tags = {"tag1": "value1", "tag2": "value2"}

        diff = _create_tag_diff(tags, tags)

        assert not diff.creates
        # Note: updates will contain all tags even if identical (this is by design)
        assert not diff.deletes


class TestAlterMetricViewColumnTagsDDL:
    """Tests for generate_alter_metric_view_column_tags_ddl function."""

    def test_field_tags_added(self):
        """Test adding tags to fields (preferred spec key)."""
        metric_view = MetricView(
            name="metrics",
            catalog="cat",
            schema_="sch",
            definition={"source": "table"},
        )

        local_def = {
            "fields": [
                {"name": "category", "expr": "category", "tags": {"pii": "false"}},
            ],
        }
        remote_def = {
            "fields": [
                {"name": "category", "expr": "category"},
            ],
        }

        statements = generate_alter_metric_view_column_tags_ddl(metric_view, local_def, remote_def)

        assert len(statements) > 0
        assert any("category" in s for s in statements)
        assert any("SET TAG ON COLUMN" in s for s in statements)

    def test_dimension_tags_added(self):
        """The spec's 'dimensions' synonym is handled the same way."""
        metric_view = MetricView(
            name="metrics",
            catalog="cat",
            schema_="sch",
            definition={"source": "table"},
        )

        local_def = {
            "dimensions": [
                {"name": "category", "expr": "category", "tags": {"pii": "false"}},
            ],
        }
        remote_def = {
            "dimensions": [
                {"name": "category", "expr": "category"},
            ],
        }

        statements = generate_alter_metric_view_column_tags_ddl(metric_view, local_def, remote_def)

        assert len(statements) > 0
        assert any("SET TAG ON COLUMN" in s for s in statements)

    def test_measure_tags_added(self):
        """Test adding tags to measures."""
        metric_view = MetricView(
            name="metrics",
            catalog="cat",
            schema_="sch",
            definition={"source": "table"},
        )

        local_def = {
            "measures": [
                {"name": "revenue", "expr": "SUM(amount)", "tags": {"sensitive": "true"}},
            ],
        }
        remote_def = {
            "measures": [
                {"name": "revenue", "expr": "SUM(amount)"},
            ],
        }

        statements = generate_alter_metric_view_column_tags_ddl(metric_view, local_def, remote_def)

        assert len(statements) > 0
        assert any("revenue" in s for s in statements)

    def test_field_tags_removed(self):
        """Test removing tags from fields."""
        metric_view = MetricView(
            name="metrics",
            catalog="cat",
            schema_="sch",
            definition={"source": "table"},
        )

        local_def = {
            "fields": [
                {"name": "category", "expr": "category"},
            ],
        }
        remote_def = {
            "fields": [
                {"name": "category", "expr": "category", "tags": {"old_tag": "value"}},
            ],
        }

        statements = generate_alter_metric_view_column_tags_ddl(metric_view, local_def, remote_def)

        assert len(statements) > 0
        assert any("UNSET TAG ON COLUMN" in s for s in statements)
        assert any("old_tag" in s for s in statements)

    def test_mixed_field_and_measure_tags(self):
        """Test changes to both field and measure tags."""
        metric_view = MetricView(
            name="metrics",
            catalog="cat",
            schema_="sch",
            definition={"source": "table"},
        )

        local_def = {
            "fields": [
                {"name": "category", "expr": "category", "tags": {"pii": "false"}},
            ],
            "measures": [
                {"name": "revenue", "expr": "SUM(amount)", "tags": {"sensitive": "true"}},
            ],
        }
        remote_def = {
            "fields": [
                {"name": "category", "expr": "category"},
            ],
            "measures": [
                {"name": "revenue", "expr": "SUM(amount)"},
            ],
        }

        statements = generate_alter_metric_view_column_tags_ddl(metric_view, local_def, remote_def)

        assert len(statements) > 0
        assert any("category" in s for s in statements)
        assert any("revenue" in s for s in statements)

    def test_no_tag_changes(self):
        """Test when there are no tag changes."""
        metric_view = MetricView(
            name="metrics",
            catalog="cat",
            schema_="sch",
            definition={"source": "table"},
        )

        definition = {
            "fields": [
                {"name": "category", "expr": "category", "tags": {"pii": "false"}},
            ],
        }

        statements = generate_alter_metric_view_column_tags_ddl(metric_view, definition, definition)

        # Should be empty when local and remote are identical
        assert len(statements) == 0

    def test_enforce_tags_mode(self):
        """Test enforce_tags mode that only adds tags, doesn't remove."""
        metric_view = MetricView(
            name="metrics",
            catalog="cat",
            schema_="sch",
            definition={"source": "table"},
        )

        local_def = {
            "fields": [
                {"name": "category", "expr": "category", "tags": {"pii": "false"}},
            ],
        }
        remote_def = {
            "fields": [
                {"name": "category", "expr": "category", "tags": {"old_tag": "keep_me"}},
            ],
        }

        statements = generate_alter_metric_view_column_tags_ddl(
            metric_view, local_def, remote_def, enforce_tags=True
        )

        # Should only have SET TAG statements, no UNSET
        assert len(statements) > 0
        assert all("SET TAG ON COLUMN" in s for s in statements)
        assert not any("UNSET TAG" in s for s in statements)


class TestComplexMetricViewScenarios:
    """Tests for complex metric view scenarios."""

    def test_full_metric_view_with_all_features(self):
        """Test a complete metric view with all features."""
        metric_view = MetricView(
            name="sales_analytics",
            catalog="prod",
            schema_="analytics",
            definition={
                "version": "1.1",
                "source": "prod.raw.sales",
                "comment": "Comprehensive sales analytics",
                "fields": [
                    {"name": "category", "expr": "category"},
                    {"name": "region", "expr": "region"},
                    {"name": "date", "expr": "DATE(order_date)"},
                ],
                "measures": [
                    {"name": "total_revenue", "expr": "SUM(amount)"},
                    {"name": "order_count", "expr": "COUNT(DISTINCT order_id)"},
                    {"name": "avg_order_value", "expr": "AVG(amount)"},
                ],
            },
            tags={"environment": "prod", "owner": "analytics_team"},
        )

        # Test CREATE
        create_ddl = generate_create_metric_view_ddl(metric_view)
        assert "CREATE OR REPLACE VIEW prod.analytics.sales_analytics" in create_ddl
        assert "fields:" in create_ddl
        assert "measures:" in create_ddl
        assert "comment: Comprehensive sales analytics" in create_ddl
        assert "category" in create_ddl
        assert "total_revenue" in create_ddl

        # Test DROP
        drop_ddl = generate_drop_metric_view_ddl(metric_view)
        assert drop_ddl == "DROP VIEW IF EXISTS prod.analytics.sales_analytics"

        # Test ALTER tags
        tag_statements = generate_alter_metric_view_tags_ddl(metric_view, metric_view.tags)
        assert len(tag_statements) == 2
        assert any("environment" in s for s in tag_statements)
        assert any("owner" in s for s in tag_statements)
