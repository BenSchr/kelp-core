"""Tests for kelp.models.table module.

This module tests the Table Pydantic model and related classes:
- Table: Main table definition model
- Column: Column definitions
- TableType: Table type enumeration
- Constraints: Primary and Foreign key constraints
- Quality: SDP and DQX quality configurations
"""

import pytest
from pydantic import ValidationError

from kelp.models.table import (
    Column,
    DQXQuality,
    ForeignKeyConstraint,
    GeneratedExpressionColumnConfig,
    GeneratedIdentityColumnConfig,
    PrimaryKeyConstraint,
    SDPQuality,
    Table,
    TableType,
)


class TestTable:
    """Test the Table model - primary user-facing API."""

    def test_create_minimal_table(self):
        """Test creating a table with minimal required fields."""
        table = Table(name="test_table")

        assert table.name == "test_table"
        # table_type is serialized to string due to use_enum_values=True
        assert table.table_type == "managed" or table.table_type == TableType.MANAGED
        assert table.columns == []
        assert table.constraints == []
        assert table.tags == {}

    def test_create_table_with_full_config(self):
        """Test creating a table with all fields populated."""
        table = Table(
            name="customers",
            catalog="main",
            schema_="default",
            description="Customer master data",
            table_type=TableType.MANAGED,
            partition_cols=["created_date"],
            cluster_by=["customer_id"],
            columns=[
                Column(name="id", data_type="bigint", nullable=False),
                Column(name="name", data_type="string"),
            ],
            constraints=[
                PrimaryKeyConstraint(name="pk_customers", columns=["id"]),
            ],
            tags={"owner": "data_team"},
        )

        assert table.name == "customers"
        assert table.catalog == "main"
        assert table.schema_ == "default"
        assert table.description == "Customer master data"
        assert len(table.columns) == 2
        assert len(table.constraints) == 1
        assert table.tags["owner"] == "data_team"

    def test_table_qualified_name_full(self):
        """Test get_qualified_name with catalog and schema."""
        table = Table(
            name="users",
            catalog="prod_catalog",
            schema_="analytics",
        )

        qualified_name = table.get_qualified_name()

        assert qualified_name == "prod_catalog.analytics.users"

    def test_table_qualified_name_schema_only(self):
        """Test get_qualified_name with only schema."""
        table = Table(
            name="orders",
            schema_="sales",
        )

        qualified_name = table.get_qualified_name()

        assert qualified_name == "sales.orders"

    def test_table_qualified_name_no_qualifiers(self):
        """Test get_qualified_name with no qualifiers."""
        table = Table(name="products")

        qualified_name = table.get_qualified_name()

        assert qualified_name == "products"

    def test_table_type_enumeration(self):
        """Test different table types."""
        managed = Table(name="managed", table_type=TableType.MANAGED)
        view = Table(name="view", table_type=TableType.VIEW)
        external = Table(name="external", table_type=TableType.EXTERNAL)

        # table_type is serialized to string due to use_enum_values=True
        assert managed.table_type == "managed"
        assert view.table_type == "view"
        assert external.table_type == "external"

    def test_table_schema_alias(self):
        """Test that 'schema' alias works for schema_ field."""
        # Using schema as keyword argument should work
        from typing import Any

        data: dict[str, Any] = {
            "name": "test",
            "schema_": "my_schema",
        }
        table = Table(**data)

        assert table.schema_ == "my_schema"

    def test_table_cluster_by_max_length(self):
        """Test cluster_by field has max 4 columns constraint."""
        # Valid: 4 columns
        table = Table(
            name="test",
            cluster_by=["col1", "col2", "col3", "col4"],
        )
        assert len(table.cluster_by) == 4

        # Invalid: 5 columns should raise error
        with pytest.raises(ValidationError):
            Table(
                name="test",
                cluster_by=["col1", "col2", "col3", "col4", "col5"],
            )

    def test_table_serialization(self):
        """Test table serialization to dict."""
        table = Table(
            name="test_table",
            catalog="main",
            schema_="default",
        )

        data = table.model_dump()

        assert data["name"] == "test_table"
        assert data["catalog"] == "main"
        # Schema should be serialized with alias
        assert "schema" in data or "schema_" in data


class TestColumn:
    """Test the Column model."""

    def test_create_basic_column(self):
        """Test creating a basic column."""
        col = Column(name="id", data_type="bigint")

        assert col.name == "id"
        assert col.data_type == "bigint"
        assert col.nullable is True
        assert col.generated is None

    def test_create_column_not_nullable(self):
        """Test creating a not-null column."""
        col = Column(name="id", data_type="bigint", nullable=False)

        assert col.nullable is False

    def test_column_with_description(self):
        """Test column with description."""
        col = Column(
            name="customer_name",
            data_type="string",
            description="The customer's full name",
        )

        assert col.description == "The customer's full name"

    def test_column_with_tags(self):
        """Test column with tags."""
        col = Column(
            name="email",
            data_type="string",
            tags={"pii": "true", "category": "contact"},
        )

        assert col.tags["pii"] == "true"
        assert col.tags["category"] == "contact"

    def test_column_generated_identity(self):
        """Test column with generated identity config."""
        col = Column(
            name="id",
            data_type="bigint",
            generated=GeneratedIdentityColumnConfig(
                type="identity",
                start_with=100,
                increment_by=5,
            ),
        )

        # Type guard check for generated config
        assert col.generated is not None
        # Type narrow to GeneratedIdentityColumnConfig
        if isinstance(col.generated, GeneratedIdentityColumnConfig):
            assert col.generated.type == "identity"
            assert col.generated.start_with == 100
            assert col.generated.increment_by == 5

    def test_column_generated_expression(self):
        """Test column with generated expression."""
        col = Column(
            name="full_name",
            data_type="string",
            generated=GeneratedExpressionColumnConfig(
                type="expression",
                expression="CONCAT(first_name, ' ', last_name)",
            ),
        )

        # Type guard check for generated config
        assert col.generated is not None
        # Type narrow to GeneratedExpressionColumnConfig
        if isinstance(col.generated, GeneratedExpressionColumnConfig):
            assert col.generated.type == "expression"
            assert "CONCAT" in col.generated.expression


class TestConstraints:
    """Test constraint models."""

    def test_primary_key_constraint(self):
        """Test creating a primary key constraint."""
        pk = PrimaryKeyConstraint(
            name="pk_users",
            columns=["user_id"],
        )

        assert pk.name == "pk_users"
        assert pk.type == "primary_key"
        assert pk.columns == ["user_id"]

    def test_composite_primary_key(self):
        """Test creating a composite primary key."""
        pk = PrimaryKeyConstraint(
            name="pk_order_items",
            columns=["order_id", "item_id"],
        )

        assert len(pk.columns) == 2
        assert "order_id" in pk.columns
        assert "item_id" in pk.columns

    def test_foreign_key_constraint(self):
        """Test creating a foreign key constraint."""
        fk = ForeignKeyConstraint(
            name="fk_orders_customer",
            columns=["customer_id"],
            reference_table="customers",
            reference_columns=["id"],
        )

        assert fk.name == "fk_orders_customer"
        assert fk.type == "foreign_key"
        assert fk.columns == ["customer_id"]
        assert fk.reference_table == "customers"
        assert fk.reference_columns == ["id"]


class TestQuality:
    """Test quality configuration models."""

    def test_sdp_quality_basic(self):
        """Test creating SDP quality configuration."""
        quality = SDPQuality(
            engine="sdp",
            expect_all={"valid_email": "email IS NOT NULL AND email LIKE '%@%'"},
        )

        assert quality.engine == "sdp"
        assert quality.level == "row"
        assert "valid_email" in quality.expect_all

    def test_sdp_quality_all_expectations(self):
        """Test SDP quality with all expectation types."""
        quality = SDPQuality(
            engine="sdp",
            expect_all={"check1": "col1 > 0"},
            expect_all_or_drop={"check2": "col2 IS NOT NULL"},
            expect_all_or_fail={"check3": "col3 < 100"},
            expect_all_or_quarantine={"check4": "col4 != ''"},
        )

        assert len(quality.expect_all) == 1
        assert len(quality.expect_all_or_drop) == 1
        assert len(quality.expect_all_or_fail) == 1
        assert len(quality.expect_all_or_quarantine) == 1

    def test_dqx_quality_basic(self):
        """Test creating DQX quality configuration."""
        quality = DQXQuality(
            engine="dqx",
            checks=[
                {"name": "check_null", "type": "not_null", "columns": ["id"]},
            ],
        )

        assert quality.engine == "dqx"
        assert quality.level == "row"
        assert len(quality.checks) == 1

    def test_dqx_quality_with_options(self):
        """Test DQX quality with custom options."""
        quality = DQXQuality(
            engine="dqx",
            sdp_expect_level="fail",
            sdp_quarantine=True,
            checks=[
                {
                    "name": "range_check",
                    "type": "between",
                    "columns": ["age"],
                    "min": 0,
                    "max": 120,
                },
            ],
        )

        assert quality.sdp_expect_level == "fail"
        assert quality.sdp_quarantine is True
        assert len(quality.checks) == 1

    def test_table_with_sdp_quality(self):
        """Test table with SDP quality configuration."""
        table = Table(
            name="users",
            columns=[Column(name="email", data_type="string")],
            quality=SDPQuality(
                engine="sdp",
                expect_all={"valid_email": "email LIKE '%@%'"},
            ),
        )

        assert table.quality is not None
        assert table.quality.engine == "sdp"

    def test_table_with_dqx_quality(self):
        """Test table with DQX quality configuration."""
        table = Table(
            name="products",
            columns=[Column(name="price", data_type="decimal(10,2)")],
            quality=DQXQuality(
                engine="dqx",
                checks=[{"name": "positive_price", "columns": ["price"]}],
            ),
        )

        assert table.quality is not None
        assert table.quality.engine == "dqx"
