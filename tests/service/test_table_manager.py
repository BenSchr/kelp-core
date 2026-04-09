"""Tests for kelp.service.model_manager module.

This module tests the ModelManager, KelpModel, KelpSdpModel, and SparkSchemaBuilder classes
which handle table metadata and schema generation.
"""

from pathlib import Path

from kelp.models.model import (
    Column,
    DQXQuality,
    ForeignKeyConstraint,
    GeneratedExpressionColumnConfig,
    GeneratedIdentityColumnConfig,
    PrimaryKeyConstraint,
    SDPQuality,
)
from kelp.models.model import (
    Model as Table,
)
from kelp.service.model_manager import KelpModel, KelpSdpModel, ModelManager, SparkSchemaBuilder


class TestKelpModel:
    """Test the KelpModel dataclass."""

    def test_kelp_table_basic_creation(self):
        """Test creating a basic KelpModel."""
        table = KelpModel(
            name="test_table",
            fqn="catalog.schema.test_table",
            schema="col1 string, col2 int",
        )

        assert table.name == "test_table"
        assert table.fqn == "catalog.schema.test_table"
        assert table.schema == "col1 string, col2 int"

    def test_kelp_table_get_dqx_check_obj_with_dqx_quality(self):
        """Test get_dqx_check_obj returns DQX quality object."""
        root_model = Table(
            name="test",
            quality=DQXQuality(engine="dqx", checks=[{"name": "test_check"}]),
        )
        kelp_table = KelpModel(name="test", root_model=root_model)

        dqx_obj = kelp_table.get_dqx_check_obj()

        assert dqx_obj is not None
        assert isinstance(dqx_obj, DQXQuality)
        assert len(dqx_obj.checks) == 1

    def test_kelp_table_get_dqx_check_obj_with_sdp_quality(self):
        """Test get_dqx_check_obj returns None for SDP quality."""
        root_model = Table(
            name="test",
            quality=SDPQuality(engine="sdp"),
        )
        kelp_table = KelpModel(name="test", root_model=root_model)

        dqx_obj = kelp_table.get_dqx_check_obj()

        assert dqx_obj is None

    def test_kelp_table_get_dqx_check_obj_no_quality(self):
        """Test get_dqx_check_obj returns None when no quality defined."""
        kelp_table = KelpModel(name="test")

        dqx_obj = kelp_table.get_dqx_check_obj()

        assert dqx_obj is None


class TestKelpSdpModel:
    """Test the KelpSdpModel dataclass."""

    def test_kelp_sdp_table_params(self):
        """Test params method excludes quality parameters."""
        table = KelpSdpModel(
            name="test_table",
            fqn="catalog.schema.test_table",
            schema="col1 string",
            comment="Test comment",
            expect_all={"rule1": "col1 IS NOT NULL"},
            expect_all_or_quarantine={"rule2": "col2 > 0"},
        )

        params = table.params()

        assert "name" in params
        assert "schema" in params
        assert "comment" in params
        # Quality params should be excluded
        assert "expect_all" not in params
        assert "expect_all_or_quarantine" not in params

    def test_kelp_sdp_table_params_raw(self):
        """Test params_raw includes all quality parameters."""
        table = KelpSdpModel(
            name="test_table",
            fqn="catalog.schema.test_table",
            expect_all={"rule1": "col1 IS NOT NULL"},
        )

        params = table.params_raw()

        assert "name" in params
        assert "expect_all" in params

    def test_kelp_sdp_table_params_cst(self):
        """Test params_cst excludes quarantine parameters."""
        table = KelpSdpModel(
            name="test_table",
            fqn="catalog.schema.test_table",
            expect_all_or_quarantine={"rule": "test"},
            expect_all_or_fail={"rule2": "test2"},
        )

        params = table.params_cst()

        assert "expect_all_or_quarantine" not in params
        # But other expectations should be included
        assert "expect_all_or_fail" in params

    def test_kelp_sdp_table_get_sdp_params(self):
        """Test get_sdp_params returns all non-None parameters."""
        table = KelpSdpModel(
            name="test",
            fqn="catalog.schema.test",
            comment="Test table",
            schema="col1 string",
            path=None,
            spark_conf={"key": "value"},
        )

        params = table.get_sdp_params()

        assert params["name"] == "catalog.schema.test"
        assert params["comment"] == "Test table"
        assert params["schema"] == "col1 string"
        assert "path" not in params  # None values excluded
        assert params["spark_conf"] == {"key": "value"}

    def test_kelp_sdp_table_get_sdp_params_with_exclude(self):
        """Test get_sdp_params respects exclude list."""
        table = KelpSdpModel(
            name="test",
            fqn="catalog.schema.test",
            comment="Test",
            schema="col1 string",
        )

        params = table.get_sdp_params(exclude=["comment", "schema"])

        assert "name" in params
        assert "comment" not in params
        assert "schema" not in params


class TestSparkSchemaBuilder:
    """Test the SparkSchemaBuilder utility class."""

    def test_build_raw_simple_schema(self):
        """Test building a raw schema with basic columns."""
        table = Table(
            name="test_table",
            columns=[
                Column(name="id", data_type="bigint", nullable=False),
                Column(name="name", data_type="string"),
            ],
        )

        builder = SparkSchemaBuilder(table)
        schema = builder.add_columns().build_raw()

        assert "id bigint NOT NULL" in schema
        assert "name string" in schema

    def test_build_raw_with_constraints(self):
        """Test building schema with constraints."""
        table = Table(
            name="test_table",
            columns=[
                Column(name="id", data_type="bigint", nullable=False),
            ],
            constraints=[
                PrimaryKeyConstraint(name="pk_test", columns=["id"]),
            ],
        )

        builder = SparkSchemaBuilder(table)
        schema = builder.add_columns().add_constraints().build_raw()

        assert "id bigint NOT NULL" in schema
        assert "CONSTRAINT pk_test PRIMARY KEY (id)" in schema

    def test_build_raw_foreign_key_constraint(self):
        """Test building schema with foreign key constraint."""
        table = Table(
            name="orders",
            columns=[
                Column(name="order_id", data_type="bigint", nullable=False),
                Column(name="customer_id", data_type="bigint"),
            ],
            constraints=[
                ForeignKeyConstraint(
                    name="fk_customer",
                    columns=["customer_id"],
                    reference_table="customers",
                    reference_columns=["id"],
                ),
            ],
        )

        builder = SparkSchemaBuilder(table)
        schema = builder.add_columns().add_constraints().build_raw()

        assert (
            "CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers (id)" in schema
        )

    def test_column_with_description(self):
        """Test column with description/comment."""
        table = Table(
            name="test",
            columns=[
                Column(name="id", data_type="bigint", description="Unique identifier"),
            ],
        )

        builder = SparkSchemaBuilder(table)
        schema = builder.add_columns().build_raw()

        assert "COMMENT 'Unique identifier'" in schema

    def test_column_with_generated_identity(self):
        """Test column with generated identity."""
        table = Table(
            name="test",
            columns=[
                Column(
                    name="id",
                    data_type="bigint",
                    generated=GeneratedIdentityColumnConfig(
                        type="identity",
                        as_default=False,
                        start_with=1,
                        increment_by=1,
                    ),
                ),
            ],
        )

        builder = SparkSchemaBuilder(table)
        schema = builder.add_columns(add_generated=True).build_raw()

        assert "GENERATED AS ALWAYS" in schema
        assert "IDENTITY (START WITH 1 INCREMENT BY 1)" in schema

    def test_column_with_generated_expression(self):
        """Test column with generated expression."""
        table = Table(
            name="test",
            columns=[
                Column(
                    name="full_name",
                    data_type="string",
                    generated=GeneratedExpressionColumnConfig(
                        type="expression",
                        expression="CONCAT(first_name, ' ', last_name)",
                    ),
                ),
            ],
        )

        builder = SparkSchemaBuilder(table)
        schema = builder.add_columns(add_generated=True).build_raw()

        assert "GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name))" in schema

    def test_build_ddl_basic(self):
        """Test building full DDL statement."""
        table = Table(
            name="test_table",
            catalog="test_catalog",
            schema_="test_schema",
            columns=[
                Column(name="id", data_type="bigint", nullable=False),
            ],
        )

        builder = SparkSchemaBuilder(table)
        ddl = builder.add_columns().build_ddl()

        assert "CREATE Table test_catalog.test_schema.test_table" in ddl
        assert "id bigint NOT NULL" in ddl

    def test_build_ddl_with_if_not_exists(self):
        """Test DDL with IF NOT EXISTS clause."""
        table = Table(name="test", columns=[Column(name="id", data_type="bigint")])

        builder = SparkSchemaBuilder(table)
        ddl = builder.add_columns().build_ddl(if_not_exists=True)

        assert "CREATE Table IF NOT EXISTS" in ddl

    def test_build_ddl_with_or_refresh(self):
        """Test DDL with OR REFRESH clause."""
        table = Table(name="test", columns=[Column(name="id", data_type="bigint")])

        builder = SparkSchemaBuilder(table)
        ddl = builder.add_columns().build_ddl(or_refresh=True)

        assert "CREATE OR REFRESH Table" in ddl

    def test_build_ddl_with_clustering(self):
        """Test DDL with clustering."""
        table = Table(
            name="test",
            columns=[Column(name="id", data_type="bigint")],
            cluster_by=["id"],
        )

        builder = SparkSchemaBuilder(table)
        ddl = builder.add_columns().add_clustering().build_ddl()

        assert "CLUSTERED BY (id)" in ddl

    def test_build_ddl_with_partitioning(self):
        """Test DDL with partitioning."""
        table = Table(
            name="test",
            columns=[Column(name="id", data_type="bigint")],
            partition_cols=["date"],
        )

        builder = SparkSchemaBuilder(table)
        ddl = builder.add_columns().add_clustering().build_ddl()

        assert "PARTITIONED BY (date)" in ddl

    def test_build_ddl_with_table_properties(self):
        """Test DDL with table properties."""
        table = Table(
            name="test",
            columns=[Column(name="id", data_type="bigint")],
            table_properties={"delta.enableChangeDataFeed": "true"},
        )

        builder = SparkSchemaBuilder(table)
        ddl = builder.add_columns().add_table_properties().build_ddl()

        assert "TBLPROPERTIES ('delta.enableChangeDataFeed'='true')" in ddl


class TestSchemaExclude:
    """Test column exclusion in schema generation."""

    def test_get_spark_schema_excludes_single_column(self):
        """Test excluding a single column from schema."""
        table = Table(
            name="test",
            columns=[
                Column(name="id", data_type="bigint", nullable=False),
                Column(name="name", data_type="string"),
                Column(name="email", data_type="string"),
            ],
        )

        schema = ModelManager.get_spark_schema(table, exclude=["email"])

        assert "id bigint NOT NULL" in schema
        assert "name string" in schema
        assert "email string" not in schema

    def test_get_spark_schema_excludes_multiple_columns(self):
        """Test excluding multiple columns from schema."""
        table = Table(
            name="test",
            columns=[
                Column(name="id", data_type="bigint"),
                Column(name="name", data_type="string"),
                Column(name="internal_id", data_type="string"),
                Column(name="created_ts", data_type="timestamp"),
            ],
        )

        schema = ModelManager.get_spark_schema(table, exclude=["internal_id", "created_ts"])

        assert "id bigint" in schema
        assert "name string" in schema
        assert "internal_id" not in schema
        assert "created_ts" not in schema

    def test_schema_exclude_is_case_insensitive(self):
        """Test that column exclude matching is case-insensitive."""
        table = Table(
            name="test",
            columns=[
                Column(name="ID", data_type="bigint"),
                Column(name="Name", data_type="string"),
            ],
        )

        schema = ModelManager.get_spark_schema(table, exclude=["id", "NAME"])

        assert "ID" not in schema
        assert "Name" not in schema

    def test_schema_exclude_nonexistent_column_noop(self):
        """Test excluding a non-existent column is a no-op."""
        table = Table(
            name="test",
            columns=[
                Column(name="id", data_type="bigint"),
                Column(name="name", data_type="string"),
            ],
        )

        schema = ModelManager.get_spark_schema(table, exclude=["nonexistent"])

        assert "id bigint" in schema
        assert "name string" in schema

    def test_add_columns_with_complex_column_definitions(self):
        """Test exclude with complex column definitions (NOT NULL, COMMENT, etc)."""
        table = Table(
            name="test",
            columns=[
                Column(
                    name="id",
                    data_type="bigint",
                    nullable=False,
                    description="Primary key",
                ),
                Column(name="status", data_type="string", description="Order status"),
                Column(
                    name="created_at",
                    data_type="timestamp",
                    description="Creation timestamp",
                ),
            ],
        )

        schema = ModelManager.get_spark_schema(table, exclude=["created_at"])

        assert "id bigint NOT NULL COMMENT 'Primary key'" in schema
        assert "status string COMMENT 'Order status'" in schema
        assert "created_at" not in schema

    def test_exclude_with_constraints_preserves_constraints(self):
        """Test that non-referencing constraints are preserved when columns excluded."""
        table = Table(
            name="test",
            columns=[
                Column(name="id", data_type="bigint", nullable=False),
                Column(name="email", data_type="string"),
            ],
            constraints=[
                PrimaryKeyConstraint(name="pk_test", columns=["id"]),
            ],
        )

        schema = ModelManager.get_spark_schema(table, include_constraints=True, exclude=["email"])

        assert "id bigint NOT NULL" in schema
        assert "email" not in schema
        assert "CONSTRAINT pk_test PRIMARY KEY (id)" in schema

    def test_exclude_column_affecting_constraint_omits_constraint(self):
        """Test that constraints referencing excluded columns are omitted."""
        table = Table(
            name="orders",
            columns=[
                Column(name="id", data_type="bigint", nullable=False),
                Column(name="customer_id", data_type="bigint", nullable=False),
                Column(name="amount", data_type="double"),
            ],
            constraints=[
                PrimaryKeyConstraint(name="pk_orders", columns=["id", "customer_id"]),
            ],
        )

        schema = ModelManager.get_spark_schema(
            table,
            include_constraints=True,
            exclude=["customer_id"],
        )

        assert "id bigint NOT NULL" in schema
        assert "customer_id" not in schema
        assert "amount double" in schema
        # Constraint references excluded column so it should be omitted
        assert "CONSTRAINT pk_orders" not in schema

    def test_exclude_with_foreign_key_constraint(self):
        """Test exclude behavior with foreign key constraints."""
        table = Table(
            name="orders",
            columns=[
                Column(name="id", data_type="bigint", nullable=False),
                Column(name="customer_id", data_type="bigint"),
                Column(name="notes", data_type="string"),
            ],
            constraints=[
                ForeignKeyConstraint(
                    name="fk_customer",
                    columns=["customer_id"],
                    reference_table="customers",
                    reference_columns=["id"],
                ),
            ],
        )

        schema = ModelManager.get_spark_schema(
            table,
            include_constraints=True,
            exclude=["notes"],
        )

        # Notes excluded, but FK constraint should remain
        assert "notes" not in schema
        assert "CONSTRAINT fk_customer FOREIGN KEY (customer_id)" in schema


class TestModelManager:
    """Test the ModelManager class methods."""

    def test_get_spark_schema_basic(self):
        """Test getting basic Spark schema."""
        table = Table(
            name="test",
            columns=[
                Column(name="col1", data_type="string"),
                Column(name="col2", data_type="int"),
            ],
        )

        schema = ModelManager.get_spark_schema(table)

        assert "col1 string" in schema
        assert "col2 int" in schema

    def test_get_spark_schema_with_constraints(self):
        """Test getting schema with constraints."""
        table = Table(
            name="test",
            columns=[Column(name="id", data_type="bigint", nullable=False)],
            constraints=[PrimaryKeyConstraint(name="pk", columns=["id"])],
        )

        schema = ModelManager.get_spark_schema(table, include_constraints=True)

        assert "CONSTRAINT pk PRIMARY KEY (id)" in schema

    def test_build_qualified_model_name_full(self):
        """Test building fully qualified table name."""
        fqn = ModelManager.build_qualified_model_name(
            catalog="my_catalog",
            schema="my_schema",
            name="my_table",
        )

        assert fqn == "my_catalog.my_schema.my_table"

    def test_build_qualified_model_name_no_catalog(self):
        """Test building qualified name without catalog."""
        fqn = ModelManager.build_qualified_model_name(
            catalog=None,
            schema="my_schema",
            name="my_table",
        )

        assert fqn == "my_schema.my_table"

    def test_build_qualified_model_name_only_name(self):
        """Test building name without qualifiers."""
        fqn = ModelManager.build_qualified_model_name(
            catalog=None,
            schema=None,
            name="my_table",
        )

        assert fqn == "my_table"

    def test_get_qualified_name_from_model(self):
        """Test getting qualified name from Table object."""
        table = Table(
            name="test_table",
            catalog="test_catalog",
            schema_="test_schema",
        )

        fqn = ModelManager.get_qualified_name_from_model(table)

        assert fqn == "test_catalog.test_schema.test_table"

    def test_get_qualified_name_from_model_with_overrides(self):
        """Test getting qualified name with overrides."""
        table = Table(
            name="test_table",
            catalog="original_catalog",
            schema_="original_schema",
        )

        fqn = ModelManager.get_qualified_name_from_model(
            table,
            catalog="override_catalog",
            schema="override_schema",
        )

        assert fqn == "override_catalog.override_schema.test_table"

    def test_build_validation_model_name(self, simple_project_dir: Path):
        """Test building validation table name."""
        from kelp.config import init

        ctx = init(project_file_path=str(simple_project_dir / "kelp_project.yml"))

        validation_name = ModelManager.build_validation_model_name(
            ctx=ctx,
            model_name="orders",
            schema="sales",
            catalog="prod",
        )

        # Default suffix is "_validation"
        assert validation_name == "prod.sales.orders_validation"

    def test_build_quarantine_model_name(self, simple_project_dir: Path):
        """Test building quarantine table name."""
        from kelp.config import init

        ctx = init(project_file_path=str(simple_project_dir / "kelp_project.yml"))

        quarantine_name = ModelManager.build_quarantine_model_name(
            ctx=ctx,
            model_name="orders",
            schema="sales",
            catalog="prod",
        )

        # Default suffix is "_quarantine"
        assert quarantine_name == "prod.sales.orders_quarantine"
