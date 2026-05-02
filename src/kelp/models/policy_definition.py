"""Policy definition model for metadata governance."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field
from pydantic.json_schema import SkipJsonSchema

from kelp.models.policy import ColumnPolicyRule, ModelPolicyRule


class Policy(BaseModel):
    """Policy definition for a specific scope or layer.

    Represents a set of governance rules that apply to tables in specified paths.

    Attributes:
        origin_file_path: Path to the source YAML file defining this policy.
        name: Policy name (e.g., "bronze_layer", "data_quality").
        applies_to: Glob pattern for models this policy applies to (e.g., "models/bronze/*").
            Must start with "models/" and use forward slashes. Matched against the
            `origin_file_path` of models. See module docstring for examples.
        model: Rules applied at the model level.
        column: Rules applied at the column level.
        raw_config: Original unparsed configuration.
    """

    origin_file_path: SkipJsonSchema[str] | None = Field(
        default=None,
        description="Path to the source YAML file defining this policy",
    )
    name: str = Field(description="Policy name")
    # TODO: add pattern validation # noqa: TD002, TD003
    applies_to: str | None = Field(
        default=None,
        description=(
            "Glob pattern for models this applies to (e.g. 'models/bronze/*'). "
            "Must start with 'models/' and use forward slashes."
        ),
    )
    model: ModelPolicyRule = Field(
        default_factory=ModelPolicyRule,
        description="Model-level policy rules",
    )
    column: ColumnPolicyRule = Field(
        default_factory=ColumnPolicyRule,
        description="Column-level policy rules",
    )
    meta: dict[str, Any] = Field(
        default_factory=dict,
        description="Generic user-defined metadata for filtering and grouping",
    )
    raw_config: SkipJsonSchema[dict] = Field(
        default_factory=dict,
        description="Original unparsed configuration preserving placeholder variables",
    )
