"""Function model for Unity Catalog SQL and Python functions."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.json_schema import SkipJsonSchema


class FunctionParameter(BaseModel):
    """Function parameter definition."""

    name: str = Field(description="Parameter name")
    data_type: str = Field(description="Parameter SQL data type")
    default_expression: str | None = Field(
        default=None,
        description="Optional SQL default expression",
    )
    comment: str | None = Field(default=None, description="Optional parameter comment")


class FunctionReturnColumn(BaseModel):
    """Column definition for table-valued function return schema."""

    name: str = Field(description="Return column name")
    data_type: str = Field(description="Return column SQL data type")
    comment: str | None = Field(default=None, description="Optional return column comment")


class FunctionEnvironment(BaseModel):
    """Environment clause for Python functions."""

    dependencies: list[str] = Field(
        default_factory=list,
        description="Python package dependencies and wheel references",
    )
    environment_version: str | None = Field(
        default=None,
        description="Python environment version (Databricks currently supports 'None')",
    )


class KelpFunction(BaseModel):
    """Function definition in Unity Catalog.

    This model validates function metadata and body source location, while leaving
    SQL/Python body parsing to Databricks at execution time.
    """

    origin_file_path: SkipJsonSchema[str] | None = Field(default=None)
    name: str = Field(description="Function name")
    catalog: str | None = Field(default=None, description="Unity Catalog name")
    schema_: str | None = Field(default=None, alias="schema", description="Schema name")
    language: Literal["SQL", "PYTHON"] = Field(
        default="SQL",
        description="Function implementation language",
    )
    temporary: bool = Field(
        default=False,
        description="Whether the function is temporary (session scoped)",
    )
    if_not_exists: bool = Field(
        default=False,
        description="Create only if function does not already exist",
    )
    or_replace: bool = Field(
        default=True,
        description="Use CREATE OR REPLACE semantics",
    )
    deterministic: bool | None = Field(
        default=None,
        description="Deterministic characteristic",
    )
    data_access: Literal["CONTAINS SQL", "READS SQL DATA"] | None = Field(
        default=None,
        description="Optional SQL data access characteristic",
    )
    default_collation: str | None = Field(
        default=None,
        description="Default collation for SQL functions",
    )
    description: str | None = Field(default=None, description="Function comment")
    parameters: list[FunctionParameter] = Field(
        default_factory=list,
        description="Function parameter definitions",
    )
    returns_data_type: str | None = Field(
        default=None,
        description="Scalar return data type",
    )
    returns_table: list[FunctionReturnColumn] = Field(
        default_factory=list,
        description="Table-valued return columns (for RETURNS TABLE)",
    )
    body: str = Field(
        default="",
        description="Function body text, normalized from inline content or file source",
    )
    body_path: str | None = Field(
        default=None,
        description="Optional path to external .sql/.py body file, relative to project root",
    )
    environment: FunctionEnvironment | None = Field(
        default=None,
        description="Python environment declaration",
    )
    tags: dict[str, str] = Field(default_factory=dict, description="Metadata tags")
    meta: dict[str, Any] = Field(
        default_factory=dict,
        description="Generic user-defined metadata for filtering and grouping",
    )
    raw_config: SkipJsonSchema[dict] = Field(default_factory=dict)

    model_config = ConfigDict(
        validate_by_name=True,
        validate_by_alias=True,
        serialize_by_alias=True,
    )

    def get_qualified_name(self) -> str:
        """Get fully qualified function name."""
        parts = []
        if self.catalog:
            parts.append(self.catalog)
        if self.schema_:
            parts.append(self.schema_)
        parts.append(self.name)
        return ".".join(parts)
