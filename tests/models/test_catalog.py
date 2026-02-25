"""Tests for kelp.models.catalog module.

This module tests the Catalog model - the primary user-facing API for
managing collections of tables and metric views:
- Catalog: Main catalog model
- table_index and metrics_index properties
- get_table() and get_metric_view() methods
- Index building and caching
"""

import pytest

from kelp.models.catalog import Catalog
from kelp.models.metric_view import MetricView
from kelp.models.table import Column, Table


class TestCatalog:
    """Test the Catalog model - primary user-facing API."""

    def test_create_empty_catalog(self):
        """Test creating an empty catalog."""
        catalog = Catalog()

        assert catalog.models == []
        assert catalog.metric_views == []
        assert catalog.table_index == {}
        assert catalog.metrics_index == {}

    def test_catalog_with_tables(self):
        """Test creating a catalog with tables."""
        tables = [
            Table(name="customers", catalog="main", schema_="default"),
            Table(name="orders", catalog="main", schema_="default"),
        ]
        catalog = Catalog(models=tables)

        assert len(catalog.models) == 2
        assert len(catalog.table_index) == 2
        assert "customers" in catalog.table_index
        assert "orders" in catalog.table_index

    def test_catalog_with_metric_views(self):
        """Test creating a catalog with metric views."""
        metrics = [
            MetricView(name="sales_metrics", catalog="main", schema_="default"),
            MetricView(name="customer_metrics", catalog="main", schema_="default"),
        ]
        catalog = Catalog(metric_views=metrics)

        assert len(catalog.metric_views) == 2
        assert len(catalog.metrics_index) == 2
        assert "sales_metrics" in catalog.metrics_index
        assert "customer_metrics" in catalog.metrics_index

    def test_catalog_get_table(self):
        """Test retrieving a table by name."""
        table = Table(name="users", catalog="main", schema_="analytics")
        catalog = Catalog(models=[table])

        retrieved = catalog.get_table("users")

        assert retrieved is table
        assert retrieved.name == "users"

    def test_catalog_get_table_not_found_raises(self):
        """Test that get_table raises KeyError when table not found."""
        catalog = Catalog()

        with pytest.raises(KeyError, match="Table not found"):
            catalog.get_table("nonexistent")

    def test_catalog_get_table_soft_handle(self):
        """Test get_table with soft_handle returns placeholder."""
        catalog = Catalog()

        table = catalog.get_table("nonexistent", soft_handle=True)

        assert table.name == "nonexistent"
        # Soft handle returns a placeholder Table

    def test_catalog_get_metric_view(self):
        """Test retrieving a metric view by name."""
        metric = MetricView(name="revenue_metrics", catalog="main", schema_="analytics")
        catalog = Catalog(metric_views=[metric])

        retrieved = catalog.get_metric_view("revenue_metrics")

        assert retrieved is metric
        assert retrieved.name == "revenue_metrics"

    def test_catalog_get_metric_view_not_found_raises(self):
        """Test that get_metric_view raises KeyError when not found."""
        catalog = Catalog()

        with pytest.raises(KeyError, match="Metric view not found"):
            catalog.get_metric_view("nonexistent")

    def test_catalog_get_metric_view_soft_handle(self):
        """Test get_metric_view with soft_handle returns placeholder."""
        catalog = Catalog()

        metric = catalog.get_metric_view("nonexistent", soft_handle=True)

        assert metric.name == "nonexistent"

    def test_catalog_get_tables(self):
        """Test get_tables returns all tables."""
        tables = [
            Table(name="t1"),
            Table(name="t2"),
            Table(name="t3"),
        ]
        catalog = Catalog(models=tables)

        all_tables = catalog.get_tables()

        assert len(all_tables) == 3
        assert all_tables == tables

    def test_catalog_get_metric_views(self):
        """Test get_metric_views returns all metric views."""
        metrics = [
            MetricView(name="m1"),
            MetricView(name="m2"),
        ]
        catalog = Catalog(metric_views=metrics)

        all_metrics = catalog.get_metric_views()

        assert len(all_metrics) == 2
        assert all_metrics == metrics


class TestCatalogIndexing:
    """Test catalog indexing behavior and caching."""

    def test_index_built_lazily(self):
        """Test that index is built lazily on first access."""
        tables = [Table(name="users"), Table(name="orders")]
        catalog = Catalog(models=tables)

        # Index should not be built initially
        assert catalog._table_index_built is False

        # Accessing index should build it
        _ = catalog.table_index

        assert catalog._table_index_built is True

    def test_duplicate_table_names_keep_first(self):
        """Test that duplicate table names keep first occurrence."""
        tables = [
            Table(name="users", catalog="catalog1"),
            Table(name="users", catalog="catalog2"),  # duplicate
        ]
        catalog = Catalog(models=tables)

        retrieved = catalog.get_table("users")

        # Should keep first occurrence
        assert retrieved.catalog == "catalog1"

    def test_duplicate_metric_names_keep_first(self):
        """Test that duplicate metric names keep first occurrence."""
        metrics = [
            MetricView(name="revenue", catalog="catalog1"),
            MetricView(name="revenue", catalog="catalog2"),  # duplicate
        ]
        catalog = Catalog(metric_views=metrics)

        retrieved = catalog.get_metric_view("revenue")

        # Should keep first occurrence
        assert retrieved.catalog == "catalog1"

    def test_refresh_index(self):
        """Test refreshing indexes after modifying catalog."""
        catalog = Catalog()

        # Initially empty
        assert len(catalog.table_index) == 0

        # Add tables directly to models list
        catalog.models.append(Table(name="new_table"))

        # Index should still be empty (cached)
        if catalog._table_index_built:
            assert len(catalog.table_index) == 0

        # Refresh index
        catalog.refresh_index()

        # Now index should include new table
        assert len(catalog.table_index) == 1
        assert "new_table" in catalog.table_index

    def test_index_caching(self):
        """Test that index is cached and not rebuilt on every access."""
        catalog = Catalog(models=[Table(name="test")])

        # First access builds index
        index1 = catalog.table_index

        # Second access should return same dict object (cached)
        index2 = catalog.table_index

        assert index1 is index2


class TestCatalogWithComplexData:
    """Test catalog with more complex, realistic data."""

    def test_catalog_with_tables_and_columns(self):
        """Test catalog with tables that have columns."""
        tables = [
            Table(
                name="customers",
                catalog="prod",
                schema_="sales",
                columns=[
                    Column(name="id", data_type="bigint"),
                    Column(name="name", data_type="string"),
                    Column(name="email", data_type="string"),
                ],
            ),
            Table(
                name="orders",
                catalog="prod",
                schema_="sales",
                columns=[
                    Column(name="order_id", data_type="bigint"),
                    Column(name="customer_id", data_type="bigint"),
                    Column(name="amount", data_type="decimal(10,2)"),
                ],
            ),
        ]
        catalog = Catalog(models=tables)

        customers = catalog.get_table("customers")
        orders = catalog.get_table("orders")

        assert len(customers.columns) == 3
        assert len(orders.columns) == 3
        assert customers.catalog == "prod"
        assert orders.schema_ == "sales"

    def test_catalog_mixed_tables_and_metrics(self):
        """Test catalog with both tables and metric views."""
        from kelp.models.table import TableType

        tables = [
            Table(name="raw_data", table_type=TableType.MANAGED),
            Table(name="clean_data", table_type=TableType.VIEW),
        ]
        metrics = [
            MetricView(name="kpi_dashboard"),
            MetricView(name="sales_dashboard"),
        ]
        catalog = Catalog(models=tables, metric_views=metrics)

        assert len(catalog.get_tables()) == 2
        assert len(catalog.get_metric_views()) == 2

        # Can retrieve both types
        raw_data = catalog.get_table("raw_data")
        kpi_dashboard = catalog.get_metric_view("kpi_dashboard")

        assert raw_data.name == "raw_data"
        assert kpi_dashboard.name == "kpi_dashboard"

    def test_catalog_get_qualified_names(self):
        """Test getting qualified table names from catalog."""
        tables = [
            Table(name="t1", catalog="c1", schema_="s1"),
            Table(name="t2", catalog="c2", schema_="s2"),
            Table(name="t3"),  # No qualifiers
        ]
        catalog = Catalog(models=tables)

        t1 = catalog.get_table("t1")
        t2 = catalog.get_table("t2")
        t3 = catalog.get_table("t3")

        assert t1.get_qualified_name() == "c1.s1.t1"
        assert t2.get_qualified_name() == "c2.s2.t2"
        assert t3.get_qualified_name() == "t3"
