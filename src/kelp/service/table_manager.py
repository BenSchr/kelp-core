from dataclasses import dataclass

from kelp.config import get_context
from kelp.models.runtime_context import RuntimeContext
from kelp.models.table import (
    Column,
    DQXQuality,
    GeneratedExpressionColumnConfig,
    GeneratedIdentityColumnConfig,
    SDPQuality,
    Table,
)


@dataclass
class KelpTable:
    name: str
    table_type: str | None = None
    comment: str | None = None
    table_properties: dict | None = None
    spark_conf: dict | None = None
    path: str | None = None
    partition_cols: list[str] | None = None
    cluster_by_auto: bool | None = None
    cluster_by: list[str] | None = None
    row_filter: str | None = None
    fqn: str | None = None
    schema: str | None = (
        None  # Schema including constraints and generated columns for use with @dp.table
    )
    schema_lite: str | None = (
        None  # Raw schema without any modifications for use with Struct operations
    )
    dqx_checks: list[dict] | None = None

    validation_table: str | None = None
    quarantine_table: str | None = None
    target_table: str | None = (
        None  # The table that should be used as the target in flows, based on quality config
    )
    root_table: Table | None = None  # Provide the root table object for edge cases

    def get_dqx_check_obj(self) -> DQXQuality | None:
        """Get the DQX quality object if defined."""
        if self.root_table.quality and isinstance(self.root_table.quality, DQXQuality):
            return self.root_table.quality
        return None

    def get_ddl(self, if_not_exists=True) -> str | None:
        """Get the DDL for creating the table, if available."""
        mapped_type = _UC_TYPE.get(self.table_type.lower(), "TABLE") if self.table_type else "TABLE"
        return (
            TableManager.get_spark_schema_ddl(
                self.root_table, table_type=mapped_type, if_not_exists=if_not_exists
            )
            if self.root_table
            else None
        )


@dataclass
class KelpSdpTable(KelpTable):
    """Utility class to use a table object flexibly"""

    expect_all: dict | None = None
    expect_all_or_fail: dict | None = None
    expect_all_or_drop: dict | None = None
    expect_all_or_quarantine: dict | None = None

    def params(self, exclude: list[str] = []) -> dict[str, str]:
        default_exclude = [
            "expect_all",
            "expect_all_or_drop",
            "expect_all_or_fail",
            "expect_all_or_quarantine",
        ]
        exclude = list(set(exclude) | set(default_exclude))
        return self.get_sdp_params(exclude=exclude)

    def params_raw(self, exclude: list[str] = []) -> dict[str, str]:
        """Get the raw parameters without excluding any quality parameters, for use in cases like cloning where we want to preserve the original quality config."""
        return self.get_sdp_params(exclude=exclude)

    def params_cst(self, exclude: list[str] = []) -> dict[str, str]:
        """Params for create_streaming_table api"""
        default_exclude = [
            "expect_all_or_quarantine",
        ]
        exclude = list(set(exclude) | set(default_exclude))
        return self.get_sdp_params(exclude=exclude)

    def get_sdp_params(self, exclude: list[str] = []) -> dict[str, str]:
        """Get the streaming table parameters as a dictionary for use with @dp.table."""

        params = {
            "name": self.fqn,
            "comment": self.comment,
            "spark_conf": self.spark_conf,
            "table_properties": self.table_properties,
            "path": self.path,
            "partition_cols": self.partition_cols,
            "cluster_by_auto": self.cluster_by_auto,
            "cluster_by": self.cluster_by,
            "schema": self.schema if self.schema else None,
            "row_filter": self.row_filter,
            "expect_all": self.expect_all,
            "expect_all_or_drop": self.expect_all_or_drop,
            "expect_all_or_fail": self.expect_all_or_fail,
            "expect_all_or_quarantine": self.expect_all_or_quarantine,
        }
        params = {k: v for k, v in params.items() if (v is not None or "") and k not in exclude}
        return params

    def get_ddl(self, if_not_exists=False, or_refresh=True) -> str | None:
        """Get the DDL for creating the table, if available."""
        mapped_type = _UC_TYPE.get(self.table_type.lower(), "TABLE") if self.table_type else "TABLE"
        return (
            TableManager.get_spark_schema_ddl(
                self.root_table,
                table_type=mapped_type,
                if_not_exists=if_not_exists,
                or_refresh=or_refresh,
            )
            if self.root_table
            else None
        )


class TableManager:
    @classmethod
    def get_spark_schema(
        cls, table: Table, include_constraints: bool = False, add_generated: bool = False
    ) -> str:
        """Get the raw Spark schema without any modifications or additions."""
        builder = SparkSchemaBuilder(table).add_columns(add_generated=add_generated)
        if include_constraints:
            builder = builder.add_constraints()
        return builder.build_raw()

    @classmethod
    def get_spark_schema_ddl(
        cls,
        table: Table,
        table_type: str = "Table",
        if_not_exists: bool = False,
        or_refresh: bool = False,
    ) -> str | None:
        builder = SparkSchemaBuilder(table)
        builder.add_columns(
            add_generated=True
        ).add_constraints().add_clustering().add_table_properties()
        return builder.build_ddl(
            table_type=table_type, if_not_exists=if_not_exists, or_refresh=or_refresh
        )

    @classmethod
    def build_validation_table_name(
        cls,
        ctx: RuntimeContext,
        table_name: str,
        schema: str | None = None,
        catalog: str | None = None,
    ) -> str:
        """Get the validation table name for a given table name."""
        prefix = ctx.project_config.quarantine_config.validation_prefix
        suffix = ctx.project_config.quarantine_config.validation_suffix
        name = table_name
        validation_table_name = f"{prefix}{name}{suffix}"
        return cls.build_qualified_table_name(catalog, schema, validation_table_name)

    @classmethod
    def build_quarantine_table_name(
        cls,
        ctx: RuntimeContext,
        table_name: str,
        schema: str | None = None,
        catalog: str | None = None,
    ):
        """Get the quarantine table name for a given table name."""
        prefix = ctx.project_config.quarantine_config.quarantine_prefix
        suffix = ctx.project_config.quarantine_config.quarantine_suffix
        quarantine_catalog = ctx.project_config.quarantine_config.quarantine_catalog
        quarantine_schema = ctx.project_config.quarantine_config.quarantine_schema
        name = table_name
        qnt_name = f"{prefix}{name}{suffix}"
        schema = quarantine_schema or schema
        catalog = quarantine_catalog or catalog
        return cls.build_qualified_table_name(catalog, schema, qnt_name)

    @classmethod
    def build_qualified_table_name(cls, catalog: str | None, schema: str | None, name: str) -> str:
        """Build a fully qualified table name from its components"""
        parts = []
        if catalog:
            parts.append(catalog)
        if schema:
            parts.append(schema)
        parts.append(name)
        return ".".join(parts)

    @classmethod
    def get_qualified_tablename_from_table(
        cls, table: Table, catalog: str | None = None, schema: str | None = None
    ) -> str:
        """Get the fully qualified table name including database/schema if applicable."""
        parts = []
        catalog = catalog or table.catalog
        schema = schema or table.schema_
        if catalog:
            parts.append(catalog)
        if schema:
            parts.append(schema)
        parts.append(table.name)
        return ".".join(parts)

    @classmethod
    def build_sdp_table(
        cls, table_name: str, table: Table | None = None, ctx: RuntimeContext | None = None
    ) -> KelpSdpTable:
        ctx = ctx or get_context()
        table = table or ctx.catalog.get_table(table_name)
        sdp_table = KelpSdpTable(name=table.name)
        sdp_table.comment = table.description
        sdp_table.spark_conf = table.spark_conf
        sdp_table.table_properties = table.table_properties
        sdp_table.path = table.path
        sdp_table.partition_cols = table.partition_cols
        sdp_table.cluster_by_auto = table.cluster_by_auto
        sdp_table.cluster_by = table.cluster_by
        sdp_table.row_filter = table.row_filter
        sdp_table.fqn = cls.get_qualified_tablename_from_table(table)
        sdp_table.schema = cls.get_spark_schema(table, include_constraints=True, add_generated=True)
        sdp_table.schema_lite = cls.get_spark_schema(
            table, include_constraints=False, add_generated=False
        )

        if table.quality and isinstance(table.quality, SDPQuality):
            sdp_table.expect_all = table.quality.expect_all
            sdp_table.expect_all_or_drop = table.quality.expect_all_or_drop
            sdp_table.expect_all_or_fail = table.quality.expect_all_or_fail
            sdp_table.expect_all_or_quarantine = table.quality.expect_all_or_quarantine
        elif table.quality and isinstance(table.quality, DQXQuality):
            sdp_table.dqx_checks = table.quality.checks

        sdp_table.validation_table = cls.build_validation_table_name(
            ctx, table.name, table.schema_, table.catalog
        )
        sdp_table.quarantine_table = cls.build_quarantine_table_name(
            ctx, table.name, table.schema_, table.catalog
        )
        sdp_table.target_table = (
            sdp_table.validation_table if sdp_table.expect_all_or_quarantine else sdp_table.fqn
        )
        sdp_table.root_table = table

        return sdp_table

    @classmethod
    def build_table(
        cls, table_name: str, table: Table | None = None, ctx: RuntimeContext | None = None
    ) -> KelpTable:
        ctx = ctx or get_context()
        table = table or ctx.catalog.get_table(table_name)
        kelp_table = KelpTable(name=table.name)
        kelp_table.comment = table.description
        kelp_table.spark_conf = table.spark_conf
        kelp_table.table_properties = table.table_properties
        kelp_table.path = table.path
        kelp_table.partition_cols = table.partition_cols
        kelp_table.cluster_by_auto = table.cluster_by_auto
        kelp_table.cluster_by = table.cluster_by
        kelp_table.row_filter = table.row_filter
        kelp_table.fqn = cls.get_qualified_tablename_from_table(table)
        kelp_table.schema = cls.get_spark_schema(
            table, include_constraints=True, add_generated=True
        )
        kelp_table.schema_lite = cls.get_spark_schema(
            table, include_constraints=False, add_generated=False
        )

        if table.quality and isinstance(table.quality, DQXQuality):
            kelp_table.dqx_checks = table.quality.checks

        return kelp_table


_UC_TYPE: dict[str, str] = {
    "managed": "TABLE",
    "view": "VIEW",
    "materialized_view": "MATERIALIZED VIEW",
    "streaming_table": "STREAMING TABLE",
}


class SparkSchemaBuilder:
    """Utility class to build Spark schema strings from Table definitions.
    SparkSchemaBuilder(table).add_columns().add_constraints().add_clustering().build()
    There are parts in the table definition (columns and constraints) and some outside (clustering, partitioning)
    """

    def __init__(self, table: Table):
        self.table = table
        self.table_parts = []
        self.outer_parts = []

    def add_columns(self, add_generated: bool = False):
        for col in self.table.columns:
            self.table_parts.append(self._column_to_string(col, add_generated=add_generated))
        return self

    def add_constraints(self):
        for constraint in self.table.constraints:
            if constraint.type == "primary_key":
                cols = ", ".join(constraint.columns)
                self.table_parts.append(f"CONSTRAINT {constraint.name} PRIMARY KEY ({cols})")
            elif constraint.type == "foreign_key":
                cols = ", ".join(constraint.columns)
                self.table_parts.append(
                    f"CONSTRAINT {constraint.name} FOREIGN KEY ({cols}) REFERENCES {constraint.reference_table} ({', '.join(constraint.reference_columns)})"
                )
        return self

    def add_clustering(self):
        if self.table.cluster_by_auto:
            self.outer_parts.append("CLUSTERED BY (AUTO)")
        elif self.table.cluster_by:
            self.outer_parts.append(f"CLUSTERED BY ({', '.join(self.table.cluster_by)})")
        elif self.table.partition_cols:
            self.outer_parts.append(f"PARTITIONED BY ({', '.join(self.table.partition_cols)})")
        return self

    def add_table_properties(self):
        if self.table.table_properties:
            props = ", ".join(f"'{k}'='{v}'" for k, v in self.table.table_properties.items())
            self.outer_parts.append(f"TBLPROPERTIES ({props})")
        return self

    def build_raw(self) -> str:
        """Build the base Spark schema wihout outer parts like clustering or table properties."""
        return ", ".join(self.table_parts)

    def build_ddl(
        self, table_type: str = "Table", if_not_exists: bool = False, or_refresh=False
    ) -> str:
        """Build the Spark schema in full DDL format, including column definitions and constraints."""
        table_schema = ",\n".join(self.table_parts)
        ddl = "CREATE "
        if or_refresh:
            ddl += "OR REFRESH "
        ddl += f"{table_type} "
        if if_not_exists:
            ddl += "IF NOT EXISTS "
        ddl += f"{self.table.get_qualified_name()} (\n{table_schema}\n)"

        if self.outer_parts:
            ddl += "\n" + "\n".join(self.outer_parts)

        return ddl

    def _column_to_string(self, column: Column, add_generated: bool = False) -> str:
        """Convert a Column object to its string representation for Spark DDL."""
        col_str = f"{column.name} {column.data_type}"
        if not column.nullable:
            col_str += " NOT NULL"
        if column.generated and add_generated:
            if column.generated.type == "identity":
                gen = column.generated
                identity_str = "GENERATED "
                if isinstance(gen, GeneratedIdentityColumnConfig):
                    if gen.as_default:
                        identity_str += "AS DEFAULT "
                    else:
                        identity_str += "AS ALWAYS "
                    identity_str += (
                        f"IDENTITY (START WITH {gen.start_with} INCREMENT BY {gen.increment_by})"
                    )
                col_str += f" {identity_str}"
            elif column.generated.type == "expression":
                gen = column.generated
                if isinstance(gen, GeneratedExpressionColumnConfig):
                    col_str += f" GENERATED ALWAYS AS ({gen.expression})"
        if column.description:
            col_str += f" COMMENT '{column.description}'"
        return col_str
