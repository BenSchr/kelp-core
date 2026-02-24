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
    origin_file_path: SkipJsonSchema[str] | None = Field(
        default=None,
    )
    table_type: TableType = Field(default=TableType.MANAGED, validate_default=True)
    catalog: str | None = Field(default=None)
    schema_: str | None = Field(default=None, alias="schema")
    name: str = Field()
    description: str | None = Field(default=None)
    spark_conf: dict = Field(default_factory=dict)
    table_properties: dict = Field(default_factory=dict)
    path: str | None = Field(default=None)
    partition_cols: list[str] = Field(default_factory=list)
    cluster_by_auto: bool = Field(default=False)
    cluster_by: list[str] = Field(default_factory=list, max_length=4)
    row_filter: str | None = Field(default=None)
    columns: list[Column] = Field(default_factory=list)
    quality: SDPQuality | DQXQuality | None = Field(default=None, discriminator="engine")
    constraints: list[PrimaryKeyConstraint | ForeignKeyConstraint] = Field(default_factory=list)
    tags: dict[str, str] = Field(default_factory=dict)
    # Preserve original, unparsed table config (including placeholder vars)
    raw_config: SkipJsonSchema[dict] = Field(default_factory=dict)

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
    """Column definition for a table."""

    name: str = Field()
    description: str | None = Field(default=None)
    data_type: str | None = Field(default=None)
    nullable: bool = Field(default=True)
    generated: GeneratedIdentityColumnConfig | GeneratedExpressionColumnConfig | None = Field(
        default=None, discriminator="type"
    )
    tags: dict[str, str] = Field(default_factory=dict)


class GeneratedIdentityColumnConfig(BaseModel):
    type: Literal["identity"]
    as_default: bool = Field(default=False, description="Generated as default or always")
    start_with: int = Field(default=1)
    increment_by: int = Field(default=1)


class GeneratedExpressionColumnConfig(BaseModel):
    type: Literal["expression"]
    expression: str = Field()  # The expression used to generate the column


class Constraint(BaseModel):
    name: str = Field()


class PrimaryKeyConstraint(Constraint):
    type: str = "primary_key"
    columns: list[str] = Field(default_factory=list)


class ForeignKeyConstraint(Constraint):
    type: str = "foreign_key"
    columns: list[str] = Field(default_factory=list)
    reference_table: str = Field()
    reference_columns: list[str] = Field(default_factory=list)


class Quality(BaseModel):
    engine: str = Field()
    level: Literal["row", "table"] = Field(default="row")


class SDPQuality(Quality):
    engine: Literal["sdp"]
    level: Literal["row"] = Field(default="row")
    expect_all: dict[str, str] = Field(default_factory=dict)
    expect_all_or_drop: dict[str, str] = Field(default_factory=dict)
    expect_all_or_fail: dict[str, str] = Field(default_factory=dict)
    expect_all_or_quarantine: dict[str, str] = Field(default_factory=dict)


class DQXQuality(Quality):
    engine: Literal["dqx"]
    sdp_expect_level: Literal["warn", "fail", "drop", "deactivate"] = Field(default="warn")
    sdp_quarantine: bool = Field(default=False)
    checks: list[dict] = Field(default_factory=list)
