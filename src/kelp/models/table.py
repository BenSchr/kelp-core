from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.json_schema import SkipJsonSchema


class TableType(Enum):
    EXTERNAL = "external"
    EXTERNAL_SHALLOW_CLONE = "external_shallow_clone"
    FOREIGN = "foreign"
    MANAGED = "managed"
    MANAGED_SHALLOW_CLONE = "managed_shallow_clone"
    MATERIALIZED_VIEW = "materialized_view"
    METRIC_VIEW = "metric_view"
    STREAMING_TABLE = "streaming_table"
    VIEW = "view"


class Table(BaseModel):
    """Table definition in Unity Catalog.

    Represents a table in Databricks Unity Catalog with all its configuration,
    including schema, properties, quality checks, and constraints.

    Attributes:
        origin_file_path: Path to the source YAML file defining this table.
        table_type: Type of table (managed, external, view, streaming_table, etc.).
        catalog: Unity Catalog name where the table resides.
        schema_: Schema/database name where the table resides.
        name: Table name.
        description: Human-readable description of the table.
        spark_conf: Spark configuration properties for the table.
        table_properties: Databricks table properties.
        path: Physical path for external tables or custom locations.
        partition_cols: List of column names to use for partitioning.
        cluster_by_auto: Enable automatic clustering optimization.
        cluster_by: List of column names to use for explicit clustering (max 4).
        row_filter: SQL expression to filter rows based on security policies.
        columns: List of column definitions for the table.
        quality: Data quality configuration (SDPQuality or DQXQuality).
        constraints: List of constraints (primary key, foreign key).
        tags: Metadata tags for the table.
        raw_config: Original unparsed configuration (preserves placeholder variables).
    """

    origin_file_path: SkipJsonSchema[str] | None = Field(
        default=None,
        description="Path to the source YAML file defining this table",
    )
    table_type: TableType = Field(
        default=TableType.MANAGED,
        validate_default=True,
        description="Type of table: managed, external, view, streaming_table, etc.",
    )
    catalog: str | None = Field(
        default=None,
        description="Unity Catalog name",
    )
    schema_: str | None = Field(
        default=None,
        alias="schema",
        description="Schema/database name",
    )
    name: str = Field(
        description="Table name",
    )
    description: str | None = Field(
        default=None,
        description="Human-readable description of the table",
    )
    spark_conf: dict = Field(
        default_factory=dict,
        description="Spark configuration properties",
    )
    table_properties: dict = Field(
        default_factory=dict,
        description="Databricks table properties",
    )
    path: str | None = Field(
        default=None,
        description="Physical path for external tables or custom locations",
    )
    partition_cols: list[str] = Field(
        default_factory=list,
        description="List of column names for partitioning",
    )
    cluster_by_auto: bool = Field(
        default=False,
        description="Enable automatic clustering optimization",
    )
    cluster_by: list[str] = Field(
        default_factory=list,
        max_length=4,
        description="List of column names for explicit clustering (max 4)",
    )
    row_filter: str | None = Field(
        default=None,
        description="SQL expression to filter rows based on security policies",
    )
    columns: list[Column] = Field(
        default_factory=list,
        description="Column definitions for the table",
    )
    quality: SDPQuality | DQXQuality | None = Field(
        default=None,
        discriminator="engine",
        description="Data quality configuration using SDPQuality or DQXQuality",
    )
    constraints: list[PrimaryKeyConstraint | ForeignKeyConstraint] = Field(
        default_factory=list,
        description="Constraints like primary key or foreign key",
    )
    tags: dict[str, str] = Field(
        default_factory=dict,
        description="Metadata tags for the table",
    )
    raw_config: SkipJsonSchema[dict] = Field(
        default_factory=dict,
        description="Original unparsed configuration preserving placeholder variables",
    )

    # Model Config
    model_config = ConfigDict(
        validate_by_name=True,
        validate_by_alias=True,
        serialize_by_alias=True,
        use_enum_values=True,
    )

    def get_qualified_name(self) -> str:
        """Get the fully qualified table name including database/schema if applicable."""
        parts = []
        if self.catalog:
            parts.append(self.catalog)
        if self.schema_:
            parts.append(self.schema_)
        parts.append(self.name)
        return ".".join(parts)


class Column(BaseModel):
    """Column definition for a table.

    Represents a column with its name, description, data type, nullability,
    and optional generated column configuration.

    Attributes:
        name: Column name.
        description: Human-readable description of the column.
        data_type: SQL data type of the column.
        nullable: Whether the column allows NULL values.
        generated: Configuration for generated columns (identity or expression).
        tags: Metadata tags for the column.
    """

    name: str = Field(
        description="Column name",
    )
    description: str | None = Field(
        default=None,
        description="Human-readable description of the column",
    )
    data_type: str | None = Field(
        default=None,
        description="SQL data type of the column",
    )
    nullable: bool = Field(
        default=True,
        description="Whether the column allows NULL values",
    )
    generated: GeneratedIdentityColumnConfig | GeneratedExpressionColumnConfig | None = Field(
        default=None,
        discriminator="type",
        description="Configuration for generated columns (identity or expression based)",
    )
    tags: dict[str, str] = Field(
        default_factory=dict,
        description="Metadata tags for the column",
    )


class GeneratedIdentityColumnConfig(BaseModel):
    """Configuration for auto-incrementing identity columns.

    Attributes:
        type: Column type identifier (always "identity").
        as_default: If True, identity is generated as default; if False, always generated.
        start_with: Starting value for the identity sequence.
        increment_by: Increment step for the identity sequence.
    """

    type: Literal["identity"] = Field(
        description="Column type identifier",
    )
    as_default: bool = Field(
        default=False,
        description="Generated as default (True) or always (False)",
    )
    start_with: int = Field(
        default=1,
        description="Starting value for the identity sequence",
    )
    increment_by: int = Field(
        default=1,
        description="Increment step for the identity sequence",
    )


class GeneratedExpressionColumnConfig(BaseModel):
    """Configuration for expression-based generated columns.

    Attributes:
        type: Column type identifier (always "expression").
        expression: SQL expression used to generate the column value.
    """

    type: Literal["expression"] = Field(
        description="Column type identifier",
    )
    expression: str = Field(
        description="SQL expression used to generate the column value",
    )


class Constraint(BaseModel):
    """Base class for table constraints.

    Attributes:
        name: Constraint name.
    """

    name: str = Field(
        description="Constraint name",
    )


class PrimaryKeyConstraint(Constraint):
    """Primary key constraint definition.

    Attributes:
        type: Constraint type identifier (always "primary_key").
        columns: List of column names that form the primary key.
    """

    type: str = Field(
        default="primary_key",
        description="Constraint type identifier",
    )
    columns: list[str] = Field(
        default_factory=list,
        description="List of column names forming the primary key",
    )


class ForeignKeyConstraint(Constraint):
    """Foreign key constraint definition.

    Attributes:
        type: Constraint type identifier (always "foreign_key").
        columns: List of local column names.
        reference_table: Fully qualified name of the referenced table.
        reference_columns: List of column names in the referenced table.
    """

    type: str = Field(
        default="foreign_key",
        description="Constraint type identifier",
    )
    columns: list[str] = Field(
        default_factory=list,
        description="List of local column names",
    )
    reference_table: str = Field(
        description="Fully qualified name of the referenced table",
    )
    reference_columns: list[str] = Field(
        default_factory=list,
        description="List of column names in the referenced table",
    )


class Quality(BaseModel):
    """Base class for data quality configuration.

    Attributes:
        engine: Quality engine type (e.g., "sdp", "dqx").
        level: Level at which quality is enforced ("row" or "table").
    """

    engine: str = Field(
        description="Quality engine type",
    )
    level: Literal["row", "table"] = Field(
        default="row",
        description="Level at which quality is enforced",
    )


class SDPQuality(Quality):
    """Spark Data Pruning (SDP) quality configuration.

    Attributes:
        engine: Quality engine type (always "sdp").
        level: Level at which quality is enforced (always "row" for SDP).
        expect_all: Dictionary of SQL expressions that must pass.
        expect_all_or_drop: Dictionary of SQL expressions; rows failing drop.
        expect_all_or_fail: Dictionary of SQL expressions; job fails if any fail.
        expect_all_or_quarantine: Dictionary of SQL expressions; failures quarantine rows.
    """

    engine: Literal["sdp"] = Field(
        description="Quality engine type",
    )
    level: Literal["row"] = Field(
        default="row",
        description="Quality enforcement level",
    )
    expect_all: dict[str, str] = Field(
        default_factory=dict,
        description="SQL expressions that must pass",
    )
    expect_all_or_drop: dict[str, str] = Field(
        default_factory=dict,
        description="SQL expressions; failing rows are dropped",
    )
    expect_all_or_fail: dict[str, str] = Field(
        default_factory=dict,
        description="SQL expressions; job fails if any expression fails",
    )
    expect_all_or_quarantine: dict[str, str] = Field(
        default_factory=dict,
        description="SQL expressions; failing rows are quarantined",
    )


class DQXQuality(Quality):
    """DQX (Databricks Quality X) quality configuration.

    Attributes:
        engine: Quality engine type (always "dqx").
        sdp_expect_level: Action for quality violations in SDP layer.
        sdp_quarantine: Whether to quarantine rows failing quality checks.
        checks: List of quality check configurations.
    """

    engine: Literal["dqx"] = Field(
        description="Quality engine type",
    )
    sdp_expect_level: Literal["warn", "fail", "drop", "deactivate"] = Field(
        default="warn",
        description="Action for quality violations: warn, fail, drop, or deactivate",
    )
    sdp_quarantine: bool = Field(
        default=False,
        description="Whether to quarantine rows failing quality checks",
    )
    checks: list[dict] = Field(
        default_factory=list,
        description="Quality check configurations",
    )
