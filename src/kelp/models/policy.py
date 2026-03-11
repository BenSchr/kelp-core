"""Policy models for metadata governance validation.

Policies define rules that assert quality and consistency of the metadata itself —
checking that tables and columns meet governance requirements like having descriptions,
required tags, constraints, and following naming conventions.

Policies support hierarchical application via path patterns (glob format, e.g., "models/bronze/*").
Path patterns must start with "models/" and use forward slashes. They are matched against
the `origin_file_path` of models (e.g., "models/bronze/customers.yml").

Examples of valid patterns:
  - "models/bronze/*" — all models in models/bronze/
  - "models/*/silver_*" — models matching models/<any>/silver_*
  - "models/**/*.yml" — recursive match in models/

Invalid patterns (will not match):
  - "bronze/*" — missing "models/" prefix
  - "*/models/*" — "models" not at start
  - "models" — no wildcard to match files

See fnmatch documentation for full glob syntax support.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class PolicySeverity(StrEnum):
    """Severity level for policy violations.

    Attributes:
        warn: Log a warning but do not fail.
        error: Raise an error and stop processing.
    """

    warn = "warn"
    error = "error"


class NamingPattern(BaseModel):
    """Naming convention rule for a specific data type.

    Attributes:
        data_type: SQL data type this pattern applies to (e.g., "STRING", "BOOLEAN").
        pattern: Regex pattern that column names must match (e.g., "^(is_|has_).*").
    """

    data_type: str = Field(description="SQL data type (e.g. STRING, BOOLEAN, INT)")
    pattern: str = Field(description="Regex pattern column names must match")


class ModelPolicyRule(BaseModel):
    """Policy rules for table-level governance checks.

    Attributes:
        require_description: Table must have a non-empty description.
        require_tags: Specific tag keys that must be present on the table.
        require_any_tag: At least one tag must exist on the table.
        require_constraints: Constraint types that must be defined (e.g. "primary_key").
        naming_pattern: Regex pattern that table names must match.
        has_columns: Column names that must be present in the table.
        has_table_property: Table properties that must exist (partial match, allows extra keys).
        has_quality_check: Table must have quality checks configured.
        not_: Negate checks for this rule block (YAML key: ``not``).
        severity: Severity when this rule is violated.
    """

    require_description: bool = Field(
        default=False,
        description="Table must have a non-empty description",
    )
    require_tags: list[str] = Field(
        default_factory=list,
        description="Specific tag keys that must be present on the table",
    )
    require_any_tag: bool = Field(
        default=False,
        description="At least one tag must exist on the table",
    )
    require_constraints: list[str] = Field(
        default_factory=list,
        description="Constraint types that must be defined (e.g. 'primary_key', 'foreign_key')",
    )
    naming_pattern: str | None = Field(
        default=None,
        description="Regex pattern that table names must match",
    )
    has_columns: list[str] = Field(
        default_factory=list,
        description="Column names that must be present in the table",
    )
    has_table_property: dict = Field(
        default_factory=dict,
        description="Table properties that must exist (partial match, allows extra keys)",
    )
    has_quality_check: bool = Field(
        default=False,
        description="Table must have quality checks configured",
    )
    not_: bool = Field(
        default=False,
        alias="not",
        description="Negate checks for this rule block",
    )
    severity: PolicySeverity = Field(
        default=PolicySeverity.warn,
        description="Severity level when a rule is violated",
    )

    model_config = ConfigDict(populate_by_name=True)


class ColumnPolicyRule(BaseModel):
    """Policy rules for column-level governance checks.

    Attributes:
        require_description: Each column must have a non-empty description.
        require_tags: Specific tag keys that must be present on each column.
        require_any_tag: At least one tag must exist on each column.
        naming_pattern: Regex pattern that column names must match.
        naming_patterns_by_type: Naming patterns for specific data types.
        not_: Negate checks for this rule block (YAML key: ``not``).
        severity: Severity when this rule is violated.
    """

    require_description: bool = Field(
        default=False,
        description="Each column must have a non-empty description",
    )
    require_tags: list[str] = Field(
        default_factory=list,
        description="Specific tag keys that must be present on each column",
    )
    require_any_tag: bool = Field(
        default=False,
        description="At least one tag must exist on each column",
    )
    naming_pattern: str | None = Field(
        default=None,
        description="Regex pattern that all column names must match",
    )
    naming_patterns_by_type: list[NamingPattern] = Field(
        default_factory=list,
        description="Regex patterns for column names by data type",
    )
    not_: bool = Field(
        default=False,
        alias="not",
        description="Negate checks for this rule block",
    )
    severity: PolicySeverity = Field(
        default=PolicySeverity.warn,
        description="Severity level when a rule is violated",
    )

    model_config = ConfigDict(populate_by_name=True)


class PolicyConfig(BaseModel):
    """Top-level policy execution switch.

    Policy rules are authored in policy files (``kelp_policies``). This model
    intentionally keeps only the global enable/disable flag for applying policy
    checks during context initialization and via the ``check-policies`` CLI.

    Attributes:
        enabled: Master switch to activate policy checks (default: False).
        fast_exit: Stop policy evaluation on first violating policy per model.
    """

    enabled: bool = Field(
        default=False,
        description="Master switch to activate policy checks",
    )
    fast_exit: bool = Field(
        default=False,
        description="Stop policy evaluation on first violating policy per model",
    )


class PolicyViolation(BaseModel):
    """A single policy rule violation.

    Attributes:
        model_name: Qualified name of the model with the violation.
        column_name: Column name if this is a column-level violation, else None.
        rule: The policy rule that was violated.
        message: Human-readable description of the violation.
        severity: Severity of this violation.
    """

    model_name: str = Field(description="Qualified name of the offending model")
    column_name: str | None = Field(
        default=None,
        description="Column name for column-level violations",
    )
    rule: str = Field(description="Policy rule identifier that was violated")
    message: str = Field(description="Human-readable violation description")
    severity: PolicySeverity = Field(description="Violation severity")
