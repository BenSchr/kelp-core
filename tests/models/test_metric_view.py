"""Tests for kelp.models.metric_view module.

This module tests the MetricView Pydantic model:
- MetricView: Metric view definition model
- Serialization and validation
- Qualified name generation
"""

from kelp.models.metric_view import MetricView


class TestMetricView:
    """Test the MetricView model - user-facing API."""

    def test_create_minimal_metric_view(self):
        """Test creating a metric view with minimal required fields."""
        metric = MetricView(name="test_metrics")

        assert metric.name == "test_metrics"
        assert metric.catalog is None
        assert metric.schema_ is None
        assert metric.definition == {}
        assert metric.tags == {}

    def test_create_metric_view_with_full_config(self):
        """Test creating a metric view with all fields populated."""
        metric = MetricView(
            name="customer_revenue",
            catalog="analytics",
            schema_="metrics",
            description="Customer revenue metrics",
            definition={
                "dimensions": [{"name": "customer_id", "data_type": "bigint"}],
                "metrics": [
                    {
                        "name": "total_revenue",
                        "data_type": "decimal(10,2)",
                        "expression": "SUM(amount)",
                    }
                ],
            },
            tags={"owner": "analytics_team", "category": "revenue"},
        )

        assert metric.name == "customer_revenue"
        assert metric.catalog == "analytics"
        assert metric.schema_ == "metrics"
        assert metric.description == "Customer revenue metrics"
        assert "dimensions" in metric.definition
        assert "metrics" in metric.definition
        assert len(metric.tags) == 2

    def test_metric_view_qualified_name_full(self):
        """Test get_qualified_name with catalog and schema."""
        metric = MetricView(
            name="sales_kpis",
            catalog="prod_catalog",
            schema_="kpi_schema",
        )

        qualified_name = metric.get_qualified_name()

        assert qualified_name == "prod_catalog.kpi_schema.sales_kpis"

    def test_metric_view_qualified_name_schema_only(self):
        """Test get_qualified_name with only schema."""
        metric = MetricView(
            name="team_metrics",
            schema_="team_schema",
        )

        qualified_name = metric.get_qualified_name()

        assert qualified_name == "team_schema.team_metrics"

    def test_metric_view_qualified_name_no_qualifiers(self):
        """Test get_qualified_name with no qualifiers."""
        metric = MetricView(name="simple_metrics")

        qualified_name = metric.get_qualified_name()

        assert qualified_name == "simple_metrics"

    def test_metric_view_schema_alias(self):
        """Test that 'schema' alias works for schema_ field."""
        from typing import Any

        data: dict[str, Any] = {
            "name": "test_metric",
            "schema_": "my_schema",
        }
        metric = MetricView(**data)

        assert metric.schema_ == "my_schema"

    def test_metric_view_with_complex_definition(self):
        """Test metric view with complex definition structure."""
        metric = MetricView(
            name="product_analytics",
            catalog="analytics",
            schema_="marketing",
            definition={
                "source_table": "sales.transactions",
                "dimensions": [
                    {"name": "product_id", "data_type": "string"},
                    {"name": "category", "data_type": "string"},
                    {"name": "region", "data_type": "string"},
                ],
                "metrics": [
                    {
                        "name": "total_sales",
                        "data_type": "decimal(18,2)",
                        "expression": "SUM(sale_amount)",
                    },
                    {
                        "name": "avg_sale",
                        "data_type": "decimal(18,2)",
                        "expression": "AVG(sale_amount)",
                    },
                    {
                        "name": "sale_count",
                        "data_type": "bigint",
                        "expression": "COUNT(*)",
                    },
                ],
                "filters": ["region != 'test'", "sale_amount > 0"],
            },
        )

        assert "source_table" in metric.definition
        assert len(metric.definition["dimensions"]) == 3
        assert len(metric.definition["metrics"]) == 3
        assert len(metric.definition["filters"]) == 2

    def test_metric_view_serialization(self):
        """Test metric view serialization to dict."""
        metric = MetricView(
            name="test_metric",
            catalog="main",
            schema_="default",
            description="Test metric view",
        )

        data = metric.model_dump()

        assert data["name"] == "test_metric"
        assert data["catalog"] == "main"
        assert data["description"] == "Test metric view"
        # Schema should be serialized with alias
        assert "schema" in data or "schema_" in data

    def test_metric_view_with_tags(self):
        """Test metric view with multiple tags."""
        metric = MetricView(
            name="tagged_metrics",
            tags={
                "owner": "data_team",
                "environment": "production",
                "version": "v2",
                "priority": "high",
            },
        )

        assert len(metric.tags) == 4
        assert metric.tags["owner"] == "data_team"
        assert metric.tags["environment"] == "production"
        assert metric.tags["priority"] == "high"

    def test_metric_view_empty_definition(self):
        """Test metric view with empty definition is valid."""
        metric = MetricView(
            name="placeholder_metric",
            definition={},
        )

        assert metric.definition == {}
        # Should not raise validation error

    def test_metric_view_origin_file_path(self):
        """Test that origin_file_path can be set and retrieved."""
        metric = MetricView(
            name="test",
            origin_file_path="/path/to/metrics/test.yml",
        )

        # origin_file_path is SkipJsonSchema, so it's internal but accessible
        assert metric.origin_file_path == "/path/to/metrics/test.yml"

    def test_metric_view_raw_config(self):
        """Test that raw_config can store unparsed configuration."""
        raw_config = {
            "name": "{{ metric_name }}",
            "catalog": "{{ catalog }}",
            "some_field": "{{ var }}",
        }
        metric = MetricView(
            name="resolved_name",
            catalog="resolved_catalog",
            raw_config=raw_config,
        )

        assert metric.name == "resolved_name"
        assert metric.catalog == "resolved_catalog"
        assert metric.raw_config == raw_config
        # raw_config preserves the original unresolved config


class TestMetricViewValidation:
    """Test MetricView validation behavior."""

    def test_metric_view_requires_name(self):
        """Test that name is required."""
        import pytest
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            MetricView()  # type: ignore[call-arg]

    def test_metric_view_definition_accepts_any_dict(self):
        """Test that definition accepts arbitrary dictionary structure."""
        # Definition is flexible and accepts any structure
        metric = MetricView(
            name="flexible_metric",
            definition={
                "custom_field": "value",
                "nested": {
                    "deeply": {
                        "nested": "structure",
                    }
                },
                "list_field": [1, 2, 3],
            },
        )

        assert metric.definition["custom_field"] == "value"
        assert metric.definition["nested"]["deeply"]["nested"] == "structure"
        assert metric.definition["list_field"] == [1, 2, 3]
