# Metadata Governance Policies

Kelp's governance policy system lets you define and enforce metadata quality rules against your local YAML definitions. Policies check that models and columns meet standards like having descriptions, required tags, naming conventions, and constraint definitions.

Policy checks:

- Run automatically on every `init()` call when `policy_config.enabled: true`
- Run on demand with `kelp check-policies`
- Operate on your **local YAML metadata only** — no Unity Catalog connection needed

## Setup

### 1. Create a Policies Directory

Add a directory for your policy YAML files alongside your other metadata:

```
my_project/
├── kelp_project.yml
└── kelp_metadata/
    ├── models/
    ├── functions/
    └── policies/              ← Add this
        └── data_standards.yml
```

### 2. Configure the Project

Point Kelp to your policies directory and enable checks in `kelp_project.yml`:

```yaml
kelp_project:
  models_path: "./kelp_metadata/models"
  policies_path: "./kelp_metadata/policies"

  policy_config:
    enabled: true
    fast_exit: false
```

`policy_config` supports:

- `enabled`: global on/off switch for policy checks
- `fast_exit`: stop evaluation at the first violating policy per model

All governance rules are defined in the policy YAML files themselves.

## Policy YAML Structure

Policy files use the `kelp_policies` key and contain a list of named policy definitions:

```yaml
kelp_policies:
  - name: global_standards
    applies_to: "*"
    model:
      require_description: true
      require_any_tag: true
      severity: warn
    column:
      require_description: true
      severity: error
```

### Policy Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Unique policy identifier |
| `applies_to` | string (glob) | Path pattern for models this policy applies to |
| `model` | ModelPolicyRule | Rules applied at the model level |
| `column` | ColumnPolicyRule | Rules applied at the column level |

### `applies_to` Patterns

The `applies_to` field is matched against each model's source file path.
In practice, use patterns rooted at `models/...` so they match paths like
`.../kelp_metadata/models/bronze/bronze_customers.yml`. Standard glob wildcards apply:

```yaml
applies_to: "models/bronze/*"    # All models in the bronze/ subdirectory
applies_to: "models/silver/*"    # All models in the silver/ subdirectory
applies_to: "models/gold/*"      # All models in the gold/ subdirectory
applies_to: "*"           # All models (catch-all)
```

Kelp evaluates policies in order and applies **all matching** policies to each model.
If `fast_exit` is enabled, evaluation stops at the first matching policy that
produces one or more violations for that model.

## Model-Level Rules

Rules under `model:` apply to the model itself:

```yaml
model:
  require_description: true            # Model must have a non-empty description
  require_any_tag: true                # At least one tag must exist
  require_tags:                        # Specific tag keys that must be present
    - owner
    - domain
  require_constraints:                 # Constraint types that must be defined
    - primary_key
  naming_pattern: "^(bronze|silver|gold)_.*"  # Regex pattern for model names
  not: false                           # Optional: invert checks in this rule block
  severity: warn                       # warn or error
```

### Model Rule Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `require_description` | bool | `false` | Model must have a non-empty description |
| `require_any_tag` | bool | `false` | At least one tag must exist on the model |
| `require_tags` | list[str] | `[]` | Specific tag keys that must be present |
| `require_constraints` | list[str] | `[]` | Constraint types that must be defined (`primary_key`, `foreign_key`) |
| `naming_pattern` | string | `null` | Regex pattern that model names must match |
| `has_columns` | list[str] | `[]` | Column names that must be present in the model |
| `has_table_property` | dict | `{}` | Table properties that must exist (partial match, extra keys allowed) |
| `has_quality_check` | bool | `false` | Model must have quality checks configured |
| `not` | bool | `false` | Invert checks in this rule block (forbid instead of require) |
| `severity` | `warn`/`error` | `warn` | Severity when this rule is violated |

## Column-Level Rules

Rules under `column:` apply to **every column** in matching models:

```yaml
column:
  require_description: true            # Every column must have a description
  require_any_tag: true                # Every column must have at least one tag
  require_tags:                        # Specific tag keys required on every column
    - data_classification
  naming_pattern: "^[a-z][a-z0-9_]*$" # Snake case enforcement
  naming_patterns_by_type:            # Per data type naming rules
    - data_type: BOOLEAN
      pattern: "^(is_|has_|can_).*"
    - data_type: TIMESTAMP
      pattern: ".*_at$"
  not: false                          # Optional: invert checks in this rule block
  severity: error                      # warn or error
```

### Column Rule Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `require_description` | bool | `false` | Every column must have a non-empty description |
| `require_any_tag` | bool | `false` | Every column must have at least one tag |
| `require_tags` | list[str] | `[]` | Specific tag keys required on every column |
| `naming_pattern` | string | `null` | Regex pattern that all column names must match |
| `naming_patterns_by_type` | list | `[]` | Naming patterns scoped to a specific SQL data type |
| `not` | bool | `false` | Invert checks in this rule block (forbid instead of require) |
| `severity` | `warn`/`error` | `warn` | Severity when this rule is violated |

### Data Type Naming Conventions

Use `naming_patterns_by_type` to enforce naming conventions for a specific SQL data type. This is evaluated only for columns that have a `data_type` set:

```yaml
column:
  naming_patterns_by_type:
    - data_type: BOOLEAN
      pattern: "^(is_|has_|can_).*"   # Boolean columns must start with is_, has_, or can_
    - data_type: TIMESTAMP
      pattern: ".*_at$"               # Timestamps must end in _at
    - data_type: DATE
      pattern: ".*_date$"             # Dates must end in _date
    - data_type: STRING
      pattern: "^[a-z][a-z0-9_]*$"   # Strings must be snake_case
  severity: warn
```

### Advanced Model-Level Rules

#### Column Requirements

Enforce that models have specific columns present:

```yaml
model:
  has_columns:                    # Model must include these columns
    - id
    - created_at
    - updated_at
  severity: warn
```

For forbidden columns, use `not: true` with `has_columns`:

```yaml
model:
  not: true
  has_columns:
    - temp
    - debug
  severity: warn
```

#### Table Property Requirements

Enforce that models have specific table properties set:

```yaml
model:
  has_table_property:             # Table must have these properties
    retention_days: "90"
    owner: "data_team"
    encryption_enabled: "true"
  severity: error
```

The `has_table_property` check uses **partial matching**: extra properties are allowed. This means if your table has `{owner: "data_team", cost_center: "cc123"}` and the policy requires `{owner: "data_team"}`, the check passes because the required property is present with the correct value.

#### Quality Check Requirements

Enforce that models have quality checks configured:

```yaml
model:
  has_quality_check: true         # Model must have quality checks (SDP or DQX)
  severity: error
```

This checks that the model has a `quality` section defined (either SDP or DQX). It does not validate the content of the checks, just their presence.

## Severity Levels

| Severity | Behavior |
|----------|----------|
| `warn` | Violation is logged as a warning; loading continues normally |
| `error` | Violation is logged as an error; a `RuntimeError` is raised after all checks complete |

`model.severity` and `column.severity` are independent. A policy can warn on model violations while failing hard on column violations:

```yaml
model:
  require_description: true
  severity: warn           # Just warn for missing model descriptions
column:
  require_description: true
  severity: error          # Fail hard for missing column descriptions
```

## `applies_to` Path Patterns

The `applies_to` field uses glob patterns to match model file paths. Patterns are matched against the `origin_file_path` of models (e.g., `models/bronze/customers.yml`).

**Important guidelines:**
- Patterns must start with `models/` — this ensures they match the models directory
- Use forward slashes `/` (not backslashes)
- Glob wildcards (`*`, `?`, `**`) are supported

**Valid patterns:**
```yaml
applies_to: "models/bronze/*"       # All models in the bronze/ directory
applies_to: "models/*/events*"      # All events models in any layer
applies_to: "models/silver/**/fact_*"  # All fact models anywhere in silver/
applies_to: "models/**"             # All models recursively
```

**Invalid patterns (won't match):**
```yaml
applies_to: "bronze/*"               # Missing "models/" prefix
applies_to: "*/models/*"             # "models" not at the start
applies_to: "models"                 # No glob to match files
```

Kelp evaluates policies in order and applies **all matching** policies to each model.
Use `policy_config.fast_exit: true` (or `--fast-exit`) when you prefer faster
checks over complete violation reporting.
## Running Policy Checks

### Automatic (on `init`)

When `policy_config.enabled: true`, checks run every time `init()` is called:

```python
from kelp import init

ctx = init()  # Policy checks run here if enabled
```

### CLI

Run checks on demand with `kelp check-policies`:

```bash
# Run all policy checks
uv run kelp check-policies

# Run against a specific target
uv run kelp check-policies --target prod

# Only show violations at or above a severity level
uv run kelp check-policies --severity error

# Exit with code 1 when any warning is found (useful for strict CI gates)
uv run kelp check-policies --fail-on warn

# Stop at first violating policy per model (speed optimization)
uv run kelp check-policies --fast-exit
```

### Example Output

```
[POLICY WARN] Model 'kelp_catalog.kelp_bronze.bronze_events' is missing a description.
[POLICY WARN] Model 'kelp_catalog.kelp_bronze.bronze_events' is missing required tag 'owner'.
[POLICY ERROR] Column 'raw_payload' in model 'kelp_catalog.kelp_bronze.bronze_events' is missing a description.

✘ Policy check failed: 3 violation(s) found (1 error, 2 warnings).
```

## Multiple Policies Per File

A single file can hold multiple policies. Kelp merges all policy files in `policies_path`:

```yaml
kelp_policies:
  - name: bronze_standards
    applies_to: "models/bronze/*"
    model:
      require_description: true
      require_tags:
        - owner
        - domain
      severity: warn
    column:
      require_description: true
      severity: warn

  - name: silver_standards
    applies_to: "*/models/silver/*"
    model:
      require_description: true
      require_tags:
        - owner
        - domain
      require_constraints:
        - primary_key
      severity: error
    column:
      require_description: true
      require_tags:
        - data_classification
      severity: error

  - name: gold_standards
    applies_to: "models/gold/*"
    model:
      require_description: true
      require_any_tag: true
      require_constraints:
        - primary_key
      naming_pattern: "^gold_.*"
      severity: error
    column:
      require_description: true
      require_tags:
        - data_classification
        - owner
      naming_patterns_by_type:
        - data_type: BOOLEAN
          pattern: "^(is_|has_|can_).*"
        - data_type: TIMESTAMP
          pattern: ".*_at$"
      severity: error

  - name: global_fallback
    applies_to: "*"
    model:
      require_description: true
      severity: warn
```

## Full Example

### Project Layout

```
my_project/
├── kelp_project.yml
└── kelp_metadata/
    ├── models/
    │   ├── bronze/
    │   │   └── bronze_customers.yml
    │   ├── silver/
    │   │   └── silver_customers_cleaned.yml
    │   └── gold/
    │       └── gold_customer_summary.yml
    └── policies/
        └── data_standards.yml
```

### kelp_project.yml

```yaml
kelp_project:
  models_path: "./kelp_metadata/models"
  policies_path: "./kelp_metadata/policies"

  models:
    +catalog: ${ catalog }
    bronze:
      +schema: bronze
    silver:
      +schema: silver
    gold:
      +schema: gold

  policy_config:
    enabled: true
    fast_exit: false

vars:
  catalog: my_catalog
```

### kelp_metadata/policies/data_standards.yml

```yaml
kelp_policies:
  - name: bronze_layer
    applies_to: "models/bronze/*"
    model:
      require_description: true
      require_tags:
        - owner
        - domain
      severity: warn
    column:
      require_description: true
      severity: warn

  - name: silver_layer
    applies_to: "models/silver/*"
    model:
      require_description: true
      require_tags:
        - owner
        - domain
      require_constraints:
        - primary_key
      severity: error
    column:
      require_description: true
      require_tags:
        - data_classification
      severity: error

  - name: gold_layer
    applies_to: "models/gold/*"
    model:
      require_description: true
      require_any_tag: true
      require_constraints:
        - primary_key
      naming_pattern: "^gold_.*"
      severity: error
    column:
      require_description: true
      require_tags:
        - data_classification
      naming_patterns_by_type:
        - data_type: BOOLEAN
          pattern: "^(is_|has_|can_).*"
        - data_type: TIMESTAMP
          pattern: ".*_at$"
      severity: error
```

## Advanced Example: Comprehensive Policy with New Rules

This example demonstrates the new column, property, and quality check rules:

```yaml
kelp_policies:
  - name: silver_with_advanced_rules
    applies_to: "models/silver/*"
    model:
      require_description: true
      require_tags:
        - owner
        - domain
      # Require that all silver tables have identity and timestamp columns
      has_columns:
        - id
        - created_at
        - updated_at
      # Forbid temporary or debug columns
      not: true
      has_columns:
        - temp
        - debug
        - test_data
      # Enforce table properties for data governance
      has_table_property:
        owner: "data_platform"
        retention_days: "90"
        pii_sensitive: "false"
      # All silver tables must have quality checks
      has_quality_check: true
      require_constraints:
        - primary_key
      severity: error
    column:
      require_description: true
      require_tags:
        - data_classification
      naming_pattern: "^[a-z][a-z0-9_]*$"
      naming_patterns_by_type:
        - data_type: BOOLEAN
          pattern: "^(is_|has_|can_).*"
        - data_type: TIMESTAMP
          pattern: ".*_at$"
      severity: error
```

When a model fails these checks, Kelp reports violations like:

```
[POLICY ERROR] Model 'catalog.silver.transactions' is missing required column 'id'.
[POLICY ERROR] Model 'catalog.silver.transactions' must not have column 'temp'.
[POLICY ERROR] Model 'catalog.silver.transactions' is missing required table property 'owner'.
[POLICY ERROR] Model 'catalog.silver.transactions' must have quality checks configured.
```

## See Also

- [Project Configuration](project_config.md) — Setting `policies_path`, `policy_config.enabled`, and `policy_config.fast_exit`
- [CLI Reference](cli.md) — `kelp check-policies` options
