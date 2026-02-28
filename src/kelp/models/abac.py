"""ABAC policy model for Unity Catalog policy definitions."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.json_schema import SkipJsonSchema


class AbacMatchColumn(BaseModel):
    """A MATCH COLUMNS clause item."""

    condition: str = Field(description="hasTag/hasTagValue condition expression")
    alias: str = Field(description="Alias used in USING COLUMNS")


class AbacPolicy(BaseModel):
    """ABAC policy definition for Unity Catalog."""

    origin_file_path: SkipJsonSchema[str] | None = Field(default=None)
    name: str = Field(description="Policy name")
    securable_type: Literal["CATALOG", "SCHEMA", "TABLE"] = Field(
        description="Target securable type",
    )
    securable_name: str = Field(description="Target securable fully qualified name")
    description: str | None = Field(default=None, description="Policy comment")
    mode: Literal["ROW_FILTER", "COLUMN_MASK"] = Field(
        description="Policy operation mode",
    )
    udf_name: str = Field(description="UDF name referenced by policy")
    target_column: str | None = Field(
        default=None,
        description="Target column for COLUMN MASK policies",
    )
    principals_to: list[str] = Field(
        default_factory=list,
        description="Principals in TO clause",
    )
    principals_except: list[str] = Field(
        default_factory=list,
        description="Principals in EXCEPT clause",
    )
    for_tables_when: str | None = Field(
        default=None,
        description="Optional FOR TABLES WHEN expression",
    )
    match_columns: list[AbacMatchColumn] = Field(
        default_factory=list,
        description="MATCH COLUMNS clauses",
    )
    using_columns: list[str] = Field(
        default_factory=list,
        description="USING COLUMNS aliases",
    )
    raw_config: SkipJsonSchema[dict] = Field(default_factory=dict)

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)
