# ABAC Policies

This guide explains how to define and organize Attribute-Based Access Control (ABAC) policies in your Kelp project. ABAC policies implement row-level and column-level access control using Unity Catalog's fine-grained access control features.

!!! note
    ABAC policies reference [Functions](functions.md) to implement masking and filtering logic. Ensure your functions are defined and synced before creating ABAC policies.

## Configure Paths and Defaults

Add dedicated paths for ABAC policies to `kelp_project.yml`:

```yaml
kelp_project:
  abacs_path: "./kelp_metadata/abacs"
  abacs: {}

# Or with hierarchical configuration
kelp_project:
  abacs_path: "./kelp_metadata/abacs"
  abacs:
    +catalog: ${ security_catalog }
    +schema: ${ abac_schema }
```

## Define ABAC Policies

ABAC policies are defined in YAML files and reference functions (UDFs) for masking and filtering.

### Column Masking Policy

Mask sensitive column values for specific principals using a masking function:

```yaml
kelp_abacs:
  - name: mask_ssn_policy
    securable_type: TABLE
    securable_name: ${ data_catalog }.${ data_schema }.customers
    description: Mask SSN for non-admin analysts
    mode: COLUMN_MASK
    udf_name: ${ function_catalog }.${ function_schema }.mask_ssn
    target_column: ssn
    principals_to:
      - analysts
    principals_except:
      - admins
```

**Policy Components:**

- `name` - Unique policy identifier
- `securable_type` - `TABLE`, `SCHEMA`, or `CATALOG`
- `securable_name` - Fully qualified name of the target securable (with variable interpolation)
- `description` - Human-readable documentation
- `mode` - `COLUMN_MASK` for column-level masking or `ROW_FILTER` for row filtering
- `udf_name` - Fully qualified name of the masking function (must be synced beforehand)
- `target_column` - The column to mask (for `COLUMN_MASK` only)
- `principals_to` - List of principals this policy applies to
- `principals_except` - List of principals excluded from this policy

### Row Filtering Policy

Restrict row access based on a filter condition using a UDF:

```yaml
kelp_abacs:
  - name: filter_user_data
    securable_type: TABLE
    securable_name: ${ data_catalog }.${ data_schema }.user_activity
    description: Filter user activity by region for regional managers
    mode: ROW_FILTER
    udf_name: ${ function_catalog }.${ function_schema }.filter_by_region
    principals_to:
      - regional_managers
    for_tables_when: hasTag('contains_pii')
```

## Advanced ABAC Features

### Match Columns with Tag Conditions

Use `match_columns` to identify columns matching tag conditions and apply masking:

```yaml
kelp_abacs:
  - name: mask_tagged_pii
    securable_type: TABLE
    securable_name: ${ data_catalog }.${ data_schema }.customers
    description: Mask all columns tagged with 'pii' severity
    mode: COLUMN_MASK
    udf_name: ${ function_catalog }.${ function_schema }.mask_pii
    principals_to:
      - analysts
    match_columns:
      - condition: hasTagValue('pii', 'email')
        alias: email_col
      - condition: hasTagValue('pii', 'phone')
        alias: phone_col
    using_columns:
      - email_col
      - phone_col
```

**Components:**

- `match_columns` - List of tag-based conditions identifying target columns
  - `condition` - Expression using `hasTag()` or `hasTagValue()` functions
  - `alias` - Reference name for use in `using_columns`
- `using_columns` - Aliases from `match_columns` to apply masking to

### Conditional Policies with FOR TABLES WHEN

Apply policies only to tables matching specific tag conditions:

```yaml
kelp_abacs:
  - name: mask_sensitive_tables_only
    securable_type: TABLE
    securable_name: ${ data_catalog }.${ data_schema }.*
    description: Mask customer ID in all tables tagged as sensitive
    mode: COLUMN_MASK
    udf_name: ${ function_catalog }.${ function_schema }.mask_id
    target_column: customer_id
    principals_to:
      - analysts
    for_tables_when: hasTag('contains_sensitive_data')
```

The `for_tables_when` expression uses Unity Catalog's tag functions and is only applied to matching tables.

## Organizing ABAC Policies

Create a clear structure for managing policies:

```
kelp_metadata/abacs/
├── abacs.yml              # Core policies
├── sensitive/
│   └── pii_masking.yml    # PII-related policies
├── compliance/
│   └── gdpr_policies.yml  # Compliance-related policies
└── regional/
    └── regional_access.yml # Geographic access policies
```

## Complete ABAC Policy Example

Here's a comprehensive example with multiple policy types:

```yaml
kelp_abacs:
  # Column masking for PII
  - name: mask_customer_email
    securable_type: TABLE
    securable_name: analytics_prod.silver.customers
    description: Mask email addresses for non-admin users
    mode: COLUMN_MASK
    udf_name: security_catalog.security.mask_email
    target_column: email
    principals_to:
      - data_analysts
      - business_users
    principals_except:
      - admins
      - data_engineers

  # Row filtering by region
  - name: regional_sales_filter
    securable_type: TABLE
    securable_name: analytics_prod.silver.sales_transactions
    description: Restrict sales data by region for regional managers
    mode: ROW_FILTER
    udf_name: security_catalog.security.filter_by_user_region
    principals_to:
      - regional_managers

  # Conditional masking on sensitive tables
  - name: mask_on_sensitive_tables
    securable_type: TABLE
    securable_name: analytics_prod.silver.*
    description: Mask SSN in all sensitive tables
    mode: COLUMN_MASK
    udf_name: security_catalog.security.mask_ssn
    target_column: ssn
    principals_to:
      - data_analysts
    for_tables_when: hasTag('contains_pii')

  # Tag-based column masking
  - name: mask_all_pii_columns
    securable_type: TABLE
    securable_name: analytics_prod.silver.customers
    description: Mask all columns marked with PII tag
    mode: COLUMN_MASK
    udf_name: security_catalog.security.mask_pii
    principals_to:
      - analysts
    match_columns:
      - condition: hasTagValue('pii', 'email')
        alias: email_col
      - condition: hasTagValue('pii', 'phone')
        alias: phone_col
      - condition: hasTagValue('pii', 'ssn')
        alias: ssn_col
    using_columns:
      - email_col
      - phone_col
      - ssn_col
```

## Syncing ABAC Policies

ABAC policies must be synced to Unity Catalog after your functions are registered.

### Prerequisites

Ensure your functions are synced first:

```python
import kelp.catalog as kc

kc.init("kelp_project.yml", target="prod")

# Sync functions first
for query in kc.sync_functions():
    spark.sql(query)
```

### Sync All Policies

```python
for query in kc.sync_abac_policies():
    print(f"Executing: {query}")
    spark.sql(query)
```

### Sync Specific Policies

```python
for query in kc.sync_abac_policies(policy_names=["mask_ssn_policy", "regional_sales_filter"]):
    spark.sql(query)
```

### Automatic Syncing with Catalog Sync

`sync_catalog()` includes ABAC policies, but **does not** include functions by default. Sync functions first, then run `sync_catalog()` after tables and metric views exist:

```python
# Before pipeline runs
for query in kc.sync_functions():
  spark.sql(query)

# After pipeline runs
for query in kc.sync_catalog():
  spark.sql(query)
```

## Best Practices

1. **Sync functions before policies** - Always ensure functions exist in the catalog before creating ABAC policies that reference them.

2. **Use meaningful names** - Policy names should clearly indicate their purpose (e.g., `mask_customer_ssn`, `filter_by_region`).

3. **Document policies** - Provide clear descriptions explaining:
   - What data is being protected
   - Which principals can access it
   - Why the restriction exists

4. **Test tagson sample data** - Verify that `match_columns` and `for_tables_when` conditions correctly identify the intended columns/tables.

5. **Start with principals_except** - Use `principals_except` to exclude privileged roles (admins, data engineers) from masking policies.

6. **Use hierarchical organization** - Group related policies by business domain or function.

7. **Version your policies** - Track policy changes and test in non-prod environments first.

8. **Monitor access patterns** - Use Unity Catalog audit logs to verify policies are working as expected.

## Common Patterns

### Masking Social Security Numbers

```yaml
kelp_abacs:
  - name: mask_ssn
    securable_type: TABLE
    securable_name: ${ data_catalog }.${ data_schema }.customers
    mode: COLUMN_MASK
    udf_name: ${ function_catalog }.${ function_schema }.mask_ssn
    target_column: ssn
    principals_to:
      - analysts
    principals_except:
      - admins
```

Create the masking function:

```yaml
kelp_functions:
  - name: mask_ssn
    language: SQL
    parameters:
      - name: ssn
        data_type: STRING
    returns_data_type: STRING
    body: CONCAT(SUBSTRING(ssn, 1, 3), '-XX-', SUBSTRING(ssn, 8, 4))
```

### Geographic Data Access Control

```yaml
kelp_abacs:
  - name: regional_sales_access
    securable_type: TABLE
    securable_name: ${ data_catalog }.${ data_schema }.sales
    mode: ROW_FILTER
    udf_name: ${ function_catalog }.${ function_schema }.filter_by_user_region
    principals_to:
      - regional_managers
```

Create the filter function:

```yaml
kelp_functions:
  - name: filter_by_user_region
    language: SQL
    parameters:
      - name: region_column
        data_type: STRING
    returns_data_type: BOOLEAN
    body: |
      region_column = current_user_region()
```

### Multi-Level Sensitivity Classification

```yaml
kelp_abacs:
  # Mask for basic users
  - name: mask_highly_sensitive
    securable_type: TABLE
    securable_name: ${ data_catalog }.${ data_schema }.employees
    mode: COLUMN_MASK
    udf_name: ${ function_catalog }.${ function_schema }.mask_pii
    principals_to:
      - business_users
    match_columns:
      - condition: hasTagValue('sensitivity', 'high')
        alias: sensitive_col
    using_columns:
      - sensitive_col

  # Filter for mid-level users
  - name: filter_confidential
    securable_type: TABLE
    securable_name: ${ data_catalog }.${ data_schema }.employees
    mode: ROW_FILTER
    udf_name: ${ function_catalog }.${ function_schema }.filter_by_department
    principals_to:
      - department_managers
```

## See Also

- [Functions](functions.md) - Defining masking and filter functions
- [Transformations](transformations.md) - Building data pipelines
- [Sync Metadata with Your Catalog](catalog.md) - Registering policies in Unity Catalog
- [Databricks ABAC Documentation](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/)
