# Functions and ABAC Policies

This guide explains how to define and organize `kelp_functions` and `kelp_abacs` in your Kelp project.

## Configure Paths and Defaults

Add dedicated paths and inheritance blocks to `kelp_project.yml`.

```yaml
kelp_project:
  functions_path: "./kelp_metadata/functions"
  functions:
    +catalog: ${ function_catalog }
    +schema: ${ function_schema }

  abacs_path: "./kelp_metadata/abacs"
  abacs: {}

vars:
  function_catalog: security_catalog
  function_schema: security_schema
  data_catalog: analytics_catalog
  data_schema: analytics_schema
```

## Define Functions (`kelp_functions`)

Functions are parsed as metadata-first objects and rendered to SQL by Kelp.

### Inline function body

```yaml
kelp_functions:
  - name: normalize_email
    language: SQL
    description: Normalize customer email
    parameters:
      - name: email
        data_type: STRING
    returns_data_type: STRING
    body: lower(trim(email))
```

### External body via `body_path`

```yaml
kelp_functions:
  - name: mask_ssn
    language: SQL
    parameters:
      - name: ssn
        data_type: STRING
    returns_data_type: STRING
    body_path: ./kelp_metadata/functions/sql/mask_ssn.sql
```

`body_path` is resolved relative to the project root.

## Define ABAC Policies (`kelp_abacs`)

ABAC policies can reference functions by fully qualified UDF name.

```yaml
kelp_abacs:
  - name: mask_ssn_policy
    securable_type: TABLE
    securable_name: ${ data_catalog }.${ data_schema }.customers
    description: Mask SSN for analysts
    mode: COLUMN_MASK
    udf_name: ${ function_catalog }.${ function_schema }.mask_ssn
    target_column: ssn
    principals_to:
      - analysts
    principals_except:
      - admins
    for_tables_when: hasTag('contains_sensitive_data')
    match_columns:
      - condition: hasTagValue('pii', 'ssn')
        alias: ssn_col
    using_columns:
      - ssn_col
```

## Sync Order

`sync_catalog()` applies functions before other objects so ABAC policies can reference them safely.

You can also sync subsets:

```python
import kelp.catalog as kc

for query in kc.sync_functions():
    print(query)

for query in kc.sync_abac_policies():
    print(query)
```

See also: [Sync Metadata with Your Catalog](02_catalog.md)
