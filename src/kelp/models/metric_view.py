"""MetricView model for Databricks Metric Views.

Metric views in Databricks allow you to define metrics that can be used across
analytics and dashboards. They provide a consistent way to define business metrics
and their calculations.

See:
- https://docs.databricks.com/aws/en/metric-views/create/sql
- https://docs.databricks.com/aws/en/metric-views/data-modeling/
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from pydantic.json_schema import SkipJsonSchema


class MetricView(BaseModel):
    """Metric View definition for Databricks.

    Attributes:
        name: The name of the metric view.
        catalog: The catalog where the metric view will be created.
        schema_: The schema where the metric view will be created.
        description: Optional description of the metric view.
        definition: The metric view definition as a dictionary. This should contain
                   the full metric view specification including dimensions, metrics,
                   and the underlying table.
        tags: Optional tags for the metric view.
        origin_file_path: Path to the source YAML file (internal use).
        raw_config: Preserve original, unparsed config (including placeholder vars).
    """

    origin_file_path: SkipJsonSchema[str] | None = Field(default=None)
    name: str = Field()
    catalog: str | None = Field(default=None)
    schema_: str | None = Field(default=None, alias="schema")
    description: str | None = Field(default=None)
    definition: dict[str, Any] = Field(
        default_factory=dict,
        description="The metric view definition including dimensions, metrics, and source table",
    )
    tags: dict[str, str] = Field(default_factory=dict)
    raw_config: SkipJsonSchema[dict] = Field(default_factory=dict)

    # Model Config
    model_config = ConfigDict(
        validate_by_name=True,
        validate_by_alias=True,
        serialize_by_alias=True,
    )

    def get_qualified_name(self) -> str:
        """Get the fully qualified metric view name including catalog/schema if applicable."""
        parts = []
        if self.catalog:
            parts.append(self.catalog)
        if self.schema_:
            parts.append(self.schema_)
        parts.append(self.name)
        return ".".join(parts)
