---
title: Policies
---

# Policies

Reference for the metadata governance policy manager. `PolicyManager` evaluates `Policy` definitions against loaded models and returns a list of `PolicyViolation` objects.

::: kelp.service.policy_manager.PolicyManager

## Model Policy Rules

The `ModelPolicyRule` class defines governance checks for tables:

```python
from kelp.models.policy import ModelPolicyRule, PolicySeverity

rule = ModelPolicyRule(
    require_description=True,           # Table must have description
    require_any_tag=True,               # At least one tag must exist
    require_tags=["owner", "domain"],   # Specific tags required
    require_constraints=["primary_key"], # Constraint types required
    naming_pattern=r"^(bronze|silver|gold)_.*",  # Regex pattern
    has_columns=["id", "created_at"],   # Columns that must exist
    not_=False,                         # Use True to invert checks
    has_table_property={"owner": "data_team"},  # Properties required
    has_quality_check=True,             # Quality checks must be defined
    severity=PolicySeverity.error,
)
```

::: kelp.models.policy.ModelPolicyRule

## Column Policy Rules

The `ColumnPolicyRule` class defines governance checks for columns:

::: kelp.models.policy.ColumnPolicyRule

## Policy Severity

::: kelp.models.policy.PolicySeverity

## Policy Violation

::: kelp.models.policy.PolicyViolation

## Policy Config

`PolicyConfig` controls global policy execution settings (including `enabled`
and `fast_exit`).

::: kelp.models.policy.PolicyConfig

## See Also

- [Metadata Governance Policies Guide](../guides/policies.md) — Complete guide with examples
- [Policy Models](../reference/models/policy.md)
