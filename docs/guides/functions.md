# Functions

This guide explains how to define and organize `kelp_functions` in your Kelp project. Functions are reusable SQL or Python code that can be registered in Unity Catalog and referenced by transformations and ABAC policies.

## Configure Paths and Defaults

Add dedicated paths and inheritance blocks to `kelp_project.yml` to manage function metadata and defaults:

```yaml
kelp_project:
  functions_path: "./kelp_metadata/functions"
  functions:
    +catalog: ${ function_catalog }
    +schema: ${ function_schema }

vars:
  function_catalog: security_catalog
  function_schema: security_schema
```

The `+` prefix applies the values as defaults to all functions in the `functions_path`. You can also create sub-paths with their own defaults:

```yaml
kelp_project:
  functions_path: "./kelp_metadata/functions"
  functions:
    +catalog: ${ function_catalog }
    +schema: ${ default_schema }
    abac:
      +schema: ${ abac_schema }

vars:
  function_catalog: security_catalog
  default_schema: public_functions
  abac_schema: abac_functions
```

This allows grouping functions by domain while maintaining consistent naming and organization.

## Define SQL Functions

SQL functions are lightweight, deterministic functions suitable for type conversions, calculations, and data transformations.

### Inline Function Body

Define the function body directly in the YAML metadata:

```yaml
kelp_functions:
  - name: normalize_email
    language: SQL
    description: Normalize customer email addresses
    parameters:
      - name: email
        data_type: STRING
    returns_data_type: STRING
    body: lower(trim(email))
```

### External Body via `body_path`

For complex functions, store the SQL in a separate file and reference it using `body_path`:

```yaml
kelp_functions:
  - name: standardize_phone
    language: SQL
    description: Standardize phone number format using regex
    parameters:
      - name: phone
        data_type: STRING
    returns_data_type: STRING
    body_path: ./kelp_metadata/functions/sql/standardize_phone.sql
```

The `body_path` is resolved relative to the project root.

**Example file** (`kelp_metadata/functions/sql/standardize_phone.sql`):

```sql
CASE
  WHEN phone IS NULL THEN NULL
  ELSE REGEXP_REPLACE(phone, '[^0-9]', '')
END
```

## Define Python Functions

Python functions allow more complex logic and can leverage external libraries (with appropriate dependencies).

### Python Function with Inline Body

```yaml
kelp_functions:
  - name: format_full_name
    language: PYTHON
    description: Format full name from first and last name
    parameters:
      - name: first_name
        data_type: STRING
      - name: last_name
        data_type: STRING
    returns_data_type: STRING
    body: |
      def format_full_name(first, last):
          if first is None:
              return last
          if last is None:
              return first
          return f"{first} {last}".strip()
```

### Python Function with External File

For better maintainability, store Python function bodies in separate files:

```yaml
kelp_functions:
  - name: classify_customer
    language: PYTHON
    description: Classify customers into segments based on spending
    parameters:
      - name: total_spent
        data_type: DECIMAL
      - name: transaction_count
        data_type: INT
    returns_data_type: STRING
    body_path: ./kelp_metadata/functions/python/classify_customer.py
```

**Example file** (`kelp_metadata/functions/python/classify_customer.py`):

```python
def classify_customer(spent, count):
    if spent is None or count is None:
        return "Unknown"
    
    avg_transaction = spent / count if count > 0 else 0
    
    if avg_transaction > 1000:
        return "Premium"
    elif count > 100:
        return "Frequent"
    elif spent > 5000:
        return "High Value"
    else:
        return "Regular"
```

### Python with Dependencies

For Python functions that require external packages, use the `environment` clause:

```yaml
kelp_functions:
  - name: calculate_hash
    language: PYTHON
    description: Calculate SHA-256 hash of input string
    parameters:
      - name: input_string
        data_type: STRING
    returns_data_type: STRING
    environment:
      dependencies:
        - hashlib  # Built-in, no version needed
      environment_version: null  # Use Databricks default
    body_path: ./kelp_metadata/functions/python/calculate_hash.py
```

**Example file** (`kelp_metadata/functions/python/calculate_hash.py`):

```python
import hashlib

def calculate_hash(value):
    if value is None:
        return None
    return hashlib.sha256(value.encode()).hexdigest()
```

## Table-Valued Functions

Define functions that return multiple columns using the `returns_table` clause:

```yaml
kelp_functions:
  - name: parse_json_payload
    language: SQL
    description: Parse nested JSON payment payload into columns
    parameters:
      - name: json_payload
        data_type: STRING
    returns_table:
      - name: payment_id
        data_type: STRING
      - name: amount
        data_type: DECIMAL
      - name: currency
        data_type: STRING
      - name: timestamp
        data_type: TIMESTAMP
    body: |
      SELECT
        json_payload:payment_id::STRING as payment_id,
        json_payload:amount::DECIMAL as amount,
        json_payload:currency::STRING as currency,
        json_payload:timestamp::TIMESTAMP as timestamp
```

## Function Properties

### Basic Properties

- `name` - Unique function identifier
- `language` - `SQL` or `PYTHON`
- `description` - Human-readable documentation
- `parameters` - List of input parameters with names and data types
- `returns_data_type` - For scalar functions, the return type
- `returns_table` - For table-valued functions, the output column schema
- `body` - Inline function body (string)
- `body_path` - Path to external function body file

### Advanced Properties

```yaml
kelp_functions:
  - name: my_deterministic_function
    language: SQL
    catalog: security_catalog      # Override inherited catalog
    schema: custom_schema           # Override inherited schema
    temporary: false                # Session-scoped function
    if_not_exists: false            # Fail if function exists
    or_replace: true                # Use CREATE OR REPLACE
    deterministic: true             # Function is deterministic
    data_access: "CONTAINS SQL"     # SQL data access level
    default_collation: "as_cast"    # Collation for comparisons
    parameters:
      - name: value
        data_type: STRING
        default_expression: "'default'"
        comment: "Input value with default"
    returns_data_type: STRING
    body: upper(trim(value))
```

## Organizing Functions

Create a clear directory structure for your functions:

```
kelp_metadata/functions/
├── functions.yml          # Core functions
├── sql/
│   ├── normalize_email.sql
│   ├── standardize_phone.sql
│   └── encode_pii.sql
├── python/
│   ├── classify_customer.py
│   ├── calculate_hash.py
│   └── segment_users.py
└── abac/                  # Functions for ABAC policies
    ├── abac_functions.yml
    └── sql/
        ├── mask_ssn.sql
        └── mask_ccn.sql
```

Keep different categories of functions organized by purpose and layer.

## Using Functions in Transformations

Reference functions by their fully qualified name using `kp.func()`:

```python
from pyspark.sql.functions import col
import kelp.pipelines as kp

@kp.table()
def customer_normalized():
    """Use Kelp functions in transformations."""
    df = spark.readStream.table(kp.ref("bronze_customers"))
    
    return df.select(
        col("id"),
        # Call function by FQN
        F.expr(f"{kp.func('normalize_email')}(email)").alias("normalized_email"),
        col("name")
    )
```

## Syncing Functions to Catalog

Functions must be synced to Unity Catalog before they can be used in ABAC policies or other metadata.

### Sync All Functions

```python
import kelp.catalog as kc

kc.init("kelp_project.yml", target="prod")

for query in kc.sync_functions():
    print(f"Executing: {query}")
    spark.sql(query)
```

### Sync Specific Functions

```python
for query in kc.sync_functions(function_names=["normalize_email", "standardize_phone"]):
    spark.sql(query)
```

### Syncing with Other Metadata

Functions are **not** included in `sync_catalog()` by default. Sync them explicitly before your pipeline runs:

```python
import kelp.catalog as kc

# Before pipeline runs
for query in kc.sync_functions():
  spark.sql(query)

# After pipeline runs (tables/metrics exist)
for query in kc.sync_catalog():
  spark.sql(query)
```

## Best Practices

1. **Use SQL for simple transformations** - SQL functions are lighter and faster than Python.

2. **Document your functions** - Provide clear descriptions and parameter comments.

3. **Keep functions focused** - Each function should do one thing well.

4. **Test with typical data** - Ensure your functions handle NULL values and edge cases.

5. **Use external files for complex logic** - Keep YAML readable by moving large function bodies to separate files.

6. **Organize hierarchically** - Use subdirectories and inheritance blocks to group related functions.

7. **Avoid hardcoding values** - Use parameters instead of hardcoded constants in function logic.

8. **Version your Python functions** - Pin dependency versions in the `environment.dependencies` list.

9. **Document parameters** - Add `comment` fields to parameters for clarity:

```yaml
kelp_functions:
  - name: mask_value
    language: SQL
    parameters:
      - name: value
        data_type: STRING
        comment: "Value to mask - will be truncated to 'XX****'"
      - name: mask_char
        data_type: STRING
        default_expression: "'X'"
        comment: "Character to use for masking (default 'X')"
    returns_data_type: STRING
    body: CONCAT(REPEAT(mask_char, 2), REPEAT('*', LENGTH(value) - 2))
```

## See Also

- [Transformations](transformations.md) - Using functions in transformations
- [ABAC Policies](abacs.md) - Using functions for row/column access control
- [Sync Metadata with Your Catalog](catalog.md) - Registering functions in Unity Catalog
