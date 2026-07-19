"""MetricView model for Databricks Metric Views.

Metric views in Databricks allow you to define metrics that can be used across
analytics and dashboards. They provide a consistent way to define business metrics
and their calculations.

The ``definition`` field holds the metric view YAML specification verbatim —
including its ``comment`` — and is passed through to DDL as-is. The only Kelp
extension is an optional ``tags`` map on dimension/field/measure entries,
which Kelp manages via ``ALTER`` statements and strips from the DDL body.

See:
- https://docs.databricks.com/aws/en/uc-semantics/metric-views/yaml-reference
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.json_schema import SkipJsonSchema


class MetricView(BaseModel):
    """Metric View definition for Databricks.

    The ``dimensions`` key (the spec synonym for ``fields``) is canonicalized
    to ``fields`` on construction, so local YAML and remote-fetched state
    always compare and render consistently — Unity Catalog currently returns
    stored definitions with ``dimensions`` even when created with ``fields``.

    Attributes:
        name: The name of the metric view.
        catalog: The catalog where the metric view will be created.
        schema_: The schema where the metric view will be created.
        definition: The metric view specification as a dictionary, following
                    the Databricks metric view YAML reference (``version``,
                    ``source``, ``comment``, ``filter``, ``joins``,
                    ``fields``, ``measures``, …).
        tags: Optional tags for the metric view.
        origin_file_path: Path to the source YAML file (internal use).
        raw_config: Preserve original, unparsed config (including placeholder vars).

    """

    origin_file_path: SkipJsonSchema[str] | None = Field(default=None)
    name: str = Field()
    catalog: str | None = Field(default=None)
    schema_: str | None = Field(default=None, alias="schema")
    definition: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "The metric view specification following the Databricks metric view "
            "YAML reference, passed through to DDL verbatim (plus optional Kelp-managed "
            "'tags' on field/measure entries)"
        ),
    )

    @field_validator("definition")
    @classmethod
    def _canonicalize_fields_key(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Rename the ``dimensions`` synonym to ``fields``, preserving key order."""
        if "dimensions" in value and "fields" not in value:
            value = {("fields" if key == "dimensions" else key): v for key, v in value.items()}
        return value

    tags: dict[str, str] = Field(default_factory=dict)
    meta: dict[str, Any] = Field(
        default_factory=dict,
        description="Generic user-defined metadata for filtering and grouping",
    )
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
