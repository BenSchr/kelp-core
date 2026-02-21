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
from dataclasses import dataclass


@dataclass
class SDPTable:
    """Utility class to use a table object flexibly"""

    name: str
    comment: str | None = None
    spark_conf: dict | None = None
    table_properties: dict | None = None
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

    expect_all: dict | None = None
    expect_all_or_fail: dict | None = None
    expect_all_or_drop: dict | None = None
    expect_all_or_quarantine: dict | None = None
    dqx_checks: list[dict] | None = None

    validation_table: str | None = None
    quarantine_table: str | None = None
    target_table: str | None = (
        None  # The table that should be used as the target in flows, based on quality config
    )
    root_table: Table | None = None  # Provide the root table object for edge cases

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
        params = {k: v for k, v in params.items() if (
            v is not None or "") and k not in exclude}
        return params

    def get_dqx_check_obj(self) -> DQXQuality | None:
        """Get the DQX quality object if defined."""
        if self.root_table.quality and isinstance(self.root_table.quality, DQXQuality):
            return self.root_table.quality
        return None


class TableManager:
    # table: Table
    # _ctx: RuntimeContext

    # def __init__(
    #     self,
    #     table_name: str | None = None,
    #     table: Table | None = None,
    #     ctx: RuntimeContext = None,
    #     soft_handle: bool = False,
    # ):
    #     self._ctx = ctx or get_context()

    #     if table is not None:
    #         self.table = table
    #     elif table_name is not None:
    #         self.table = self._ctx.catalog.get_table(table_name, soft_handle=soft_handle)
    #     else:  # This case should be unreachable due to the first check, but mypy needs this for type narrowing
    #         raise ValueError("Either table_name or table must be provided.")

    # def get_table(self) -> Table:
    #     return self.table

    # def get_target(self) -> str:
    #     """returns the target name for the table if quarantine or validation is enabled, else returns the main table name"""
    #     if self.table.quality:
    #         if self.table.quality.expect_all_or_quarantine:
    #             return self.get_validation_table_name()
    #     return self.get_qualitified_table_name()

    # def get_source(self) -> str:
    #     """returns the source name for the table, which is always the main table name"""
    #     return self.get_qualitified_table_name()

    # def get_spark_schema(
    #     self, include_constraints: bool = False, add_generated: bool = False
    # ) -> str:
    #     """Get the raw Spark schema without any modifications or additions."""
    #     builder = SparkSchemaBuilder(self.table).add_columns(add_generated=add_generated)
    #     if include_constraints:
    #         builder = builder.add_constraints()
    #     return builder.build_raw()

    # def get_spark_schema_ddl(
    #     self, table_type: str = "Table", if_not_exists: bool = False, add_generated: bool = False
    # ) -> str | None:
    #     builder = SparkSchemaBuilder(self.table)
    #     builder.add_columns(
    #         add_generated=True
    #     ).add_constraints().add_clustering().add_table_properties()
    #     return builder.build_ddl(table_type=table_type, if_not_exists=if_not_exists)

    # def get_qualitified_table_name(
    #     self, catalog: str = None, schema: str = None, name: str = None
    # ) -> str:
    #     """Get the fully qualified table name including database/schema if applicable."""
    #     catalog = catalog or self.table.catalog
    #     schema = schema or self.table.schema_
    #     name = name or self.table.name
    #     parts = []
    #     if catalog:
    #         parts.append(catalog)
    #     if schema:
    #         parts.append(schema)
    #     parts.append(name)
    #     return ".".join(parts)

    # def get_quarantine_table_name(self) -> str:
    #     """Get the quarantine table name based on the main table name."""
    #     prefix = self._ctx.project_config.quarantine_config.quarantine_prefix
    #     suffix = self._ctx.project_config.quarantine_config.quarantine_suffix
    #     quarantine_catalog = self._ctx.project_config.quarantine_config.quarantine_catalog
    #     quarantine_schema = self._ctx.project_config.quarantine_config.quarantine_schema
    #     name = self.table.name
    #     qnt_name = f"{prefix}{name}{suffix}"
    #     return self.get_qualitified_table_name(quarantine_catalog, quarantine_schema, qnt_name)

    # def get_validation_table_name(self) -> str:
    #     """Get the validation table name based on the main table name. Can be used for append_flows"""
    #     prefix = self._ctx.project_config.quarantine_config.validation_prefix
    #     suffix = self._ctx.project_config.quarantine_config.validation_suffix
    #     name = self.table.name
    #     vld_name = f"{prefix}{name}{suffix}"
    #     return self.get_qualitified_table_name(None, None, vld_name)

    # def get_sdp_table_params_as_dict(self, exclude: list[str] = []) -> dict[str, str]:
    #     """Get the streaming table parameters as a dictionary for use with @dp.table."""

    #     quality = self.table.quality
    #     quality_params = {}
    #     if quality and isinstance(quality, SDPQuality):
    #         quality_params = {
    #             "expect_all": quality.expect_all,
    #             "expect_all_or_drop": quality.expect_all_or_drop,
    #             "expect_all_or_fail": quality.expect_all_or_fail,
    #             "expect_all_or_quarantine": quality.expect_all_or_quarantine,
    #         }

    #     params = {
    #         "name": self.get_qualitified_table_name(),
    #         "comment": self.table.description,
    #         "spark_conf": self.table.spark_conf,
    #         "table_properties": self.table.table_properties,
    #         "path": self.table.path,
    #         "partition_cols": self.table.partition_cols,
    #         "cluster_by_auto": self.table.cluster_by_auto,
    #         "cluster_by": self.table.cluster_by,
    #         "schema": self.get_spark_schema(include_constraints=True, add_generated=True),
    #         "row_filter": self.table.rowfilter,
    #         "expect_all": quality_params.get("expect_all"),
    #         "expect_all_or_drop": quality_params.get("expect_all_or_drop"),
    #         "expect_all_or_fail": quality_params.get("expect_all_or_fail"),
    #         "expect_all_or_quarantine": quality_params.get("expect_all_or_quarantine"),
    #     }
    #     params = {k: v for k, v in params.items() if (v is not None or "") and k not in exclude}
    #     return params

    # def get_dqx_checks(self) -> list[dict]:
    #     """Get the DQX ruleset if defined."""
    #     checks = {}
    #     if self.table.quality and isinstance(self.table.quality, DQXQuality):
    #         return self.table.quality.checks
    #     return checks

    # def get_dqx_check_obj(self) -> DQXQuality | None:
    #     """Get the DQX quality object if defined."""
    #     if self.table.quality and isinstance(self.table.quality, DQXQuality):
    #         return self.table.quality
    #     return None

    @classmethod
    def get_spark_schema(
        cls, table: Table, include_constraints: bool = False, add_generated: bool = False
    ) -> str:
        """Get the raw Spark schema without any modifications or additions."""
        builder = SparkSchemaBuilder(table).add_columns(
            add_generated=add_generated)
        if include_constraints:
            builder = builder.add_constraints()
        return builder.build_raw()

    @classmethod
    def get_spark_schema_ddl(
        cls,
        table: Table,
        table_type: str = "Table",
        if_not_exists: bool = False,
    ) -> str | None:
        builder = SparkSchemaBuilder(table)
        builder.add_columns(
            add_generated=True
        ).add_constraints().add_clustering().add_table_properties()
        return builder.build_ddl(table_type=table_type, if_not_exists=if_not_exists)

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
    ) -> SDPTable:
        ctx = ctx or get_context()
        table = table or ctx.catalog.get_table(table_name)
        sdp_table = SDPTable(name=table.name)
        sdp_table.comment = table.description
        sdp_table.spark_conf = table.spark_conf
        sdp_table.table_properties = table.table_properties
        sdp_table.path = table.path
        sdp_table.partition_cols = table.partition_cols
        sdp_table.cluster_by_auto = table.cluster_by_auto
        sdp_table.cluster_by = table.cluster_by
        sdp_table.row_filter = table.row_filter
        sdp_table.fqn = cls.get_qualified_tablename_from_table(table)
        sdp_table.schema = cls.get_spark_schema(
            table, include_constraints=True, add_generated=True)
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
            self.table_parts.append(self._column_to_string(
                col, add_generated=add_generated))
        return self

    def add_constraints(self):
        for constraint in self.table.constraints:
            if constraint.type == "primary_key":
                cols = ", ".join(constraint.columns)
                self.table_parts.append(
                    f"CONSTRAINT {constraint.name} PRIMARY KEY ({cols})")
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
            self.outer_parts.append(
                f"CLUSTERED BY ({', '.join(self.table.cluster_by)})")
        elif self.table.partition_cols:
            self.outer_parts.append(
                f"PARTITIONED BY ({', '.join(self.table.partition_cols)})")
        return self

    def add_table_properties(self):
        if self.table.table_properties:
            props = ", ".join(f"'{k}'='{v}'" for k,
                              v in self.table.table_properties.items())
            self.outer_parts.append(f"TBLPROPERTIES ({props})")
        return self

    def build_raw(self) -> str:
        """Build the base Spark schema wihout outer parts like clustering or table properties."""
        return ", ".join(self.table_parts)

    def build_ddl(self, table_type: str = "Table", if_not_exists: bool = False) -> str:
        """Build the Spark schema in full DDL format, including column definitions and constraints."""
        table_schema = ",\n".join(self.table_parts)

        ddl = f"CREATE {table_type} "
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
