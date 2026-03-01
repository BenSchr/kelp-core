# DataFrame Transformations

This guide explains how to use Kelp's composable DataFrame transformation functions. These utilities help you apply schemas, normalize column names, add missing columns, and apply Unity Catalog functions to DataFrames in isolation from your pipeline decorators.

## Overview

Kelp provides two main transformation functions designed to work with PySpark's `DataFrame.transform()`:

- `apply_schema()` - Enforce a target schema on a DataFrame
- `apply_func()` - Apply a Unity Catalog function to a DataFrame column

These are lightweight, composable functions that can be chained together or used independently.

## Schema Transformations with `apply_schema()`

### Basic Usage

The `apply_schema()` function enforces a target schema on a DataFrame by casting columns, reordering them, adding missing ones, and dropping extras:

```python
from kelp.transformations import apply_schema
from pyspark.sql import SparkSession

spark = SparkSession.active()
df = spark.read.table("raw_customers")

# Look up schema from Kelp metadata
transformed = df.transform(apply_schema("customers"))
```

### Schema from Kelp Metadata

Reference a schema defined in your `kelp_metadata/models`:

```python
@kp.table()
def silver_customers():
    df = spark.readStream.table(kp.ref("bronze_customers"))
    
    # Apply schema from kelp_metadata/models - enforces columns, types, and order
    return df.transform(apply_schema("silver_customers"))
```

### Explicit DDL Schema

Provide the schema directly as a DDL string:

```python
df = raw_data.transform(
    apply_schema(schema="id INT, name STRING, email STRING, created_at TIMESTAMP")
)
```

### Schema from StructType

Use a PySpark `StructType` directly:

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
])

df = raw_data.transform(apply_schema(schema=schema))
```

### Advanced Options

Control how the transformation handles schema mismatches:

```python
df = raw_data.transform(
    apply_schema(
        "my_table",
        safe_cast=True,               # Use try_cast - incompatible values become NULL
        drop_extra_columns=True,      # Remove columns not in target schema
        add_missing_columns=True,     # Create missing columns with default value
        missing_column_default=None   # Use NULL for new columns
    )
)
```

**Options:**

- `safe_cast` - When `True`, uses `try_cast()` so incompatible values become `NULL` instead of raising errors. Defaults to `False`.
- `drop_extra_columns` - When `True` (default), removes columns present in DataFrame but absent from target schema.
- `add_missing_columns` - When `True` (default), creates columns defined in target schema but absent from DataFrame.
- `missing_column_default` - The literal value used for newly added columns (defaults to `NULL`).

## Function Application with `apply_func()`

### Basic Usage

Apply a Unity Catalog function to create a new derived column:

```python
from kelp.transformations import apply_func

df = spark.read.table("customers")

# Apply a function to normalize an email column
result = df.transform(
    apply_func(
        func_name="normalize_email",
        new_column="email_normalized",
        parameters="email"  # Column name to pass to function
    )
)
```

### Function with Single Parameter

Map a single DataFrame column to a function parameter:

```python
df.transform(
    apply_func(
        func_name="mask_ssn",
        new_column="ssn_masked",
        parameters="ssn"  # Pass 'ssn' column to the mask_ssn function
    )
)
```

### Function with Multiple Parameters

Use a dictionary to map function parameters to DataFrame columns:

```python
df.transform(
    apply_func(
        func_name="format_full_name",
        new_column="full_name",
        parameters={
            "first_name": "first_name",  # Function param -> DataFrame column
            "last_name": "last_name"
        }
    )
)
```

### Chaining Multiple Functions

Compose multiple function applications:

```python
result = df.transform(
    apply_func(
        func_name="normalize_customer_id",
        new_column="customer_id_normalized",
        parameters="customer_id"
    )
).transform(
    apply_func(
        func_name="classify_customer",
        new_column="customer_segment",
        parameters={
            "total_spent": "lifetime_value",
            "transaction_count": "num_transactions"
        }
    )
)
```

## Real-World Examples

### Complete ETL Pipeline

Combine schema enforcement and function application in a pipeline:

```python
from kelp.transformations import apply_schema, apply_func
import kelp.pipelines as kp

@kp.table()
def silver_customers():
    """Bronze to silver transformation with normalization and masking."""
    df = spark.readStream.table(kp.ref("bronze_customers"))
    
    return (
        df
        # Enforce target schema from metadata
        .transform(apply_schema("silver_customers"))
        # Normalize email
        .transform(
            apply_func(
                func_name="normalize_email",
                new_column="email_clean",
                parameters="email"
            )
        )
        # Standardize country values
        .transform(
            apply_func(
                func_name="standardize_country",
                new_column="country_standard",
                parameters="country"
            )
        )
    )
```

### Data Masking

Apply masking functions before writing sensitive data:

```python
from kelp.transformations import apply_func
from pyspark.sql.functions import col

df = spark.read.table("analytics_catalog.raw.customers")

masked_df = (
    df
    .transform(
        apply_func(
            func_name="mask_ssn",
            new_column="ssn_masked",
            parameters="ssn"
        )
    )
    .transform(
        apply_func(
            func_name="mask_email",
            new_column="email_masked",
            parameters="email"
        )
    )
    .select("customer_id", "ssn_masked", "email_masked", col("*").exclude("ssn", "email"))
)

masked_df.write.mode("overwrite").saveAsTable("analytics_catalog.processed.customers_masked")
```

### Schema Migration

Migrate data to a new schema while preserving data:

```python
from kelp.transformations import apply_schema

# Old DataFrame with different column names/types
old_df = spark.read.table("legacy.customers")

# New schema with better naming and types
migrated = old_df.transform(
    apply_schema(
        schema="""
            customer_id INT,
            customer_name STRING,
            customer_email STRING,
            registration_date TIMESTAMP
        """,
        safe_cast=True,          # Incompatible values become NULL
        add_missing_columns=True,
        missing_column_default=None
    )
)

migrated.write.mode("overwrite").saveAsTable("new_catalog.customers")
```

### Multi-Step Normalization

Chain multiple transformations for comprehensive data cleaning:

```python
result = (
    spark.read.table("bronze_customers")
    .transform(apply_schema("silver_customers"))
    # Normalize identifiers
    .transform(
        apply_func(
            func_name="normalize_customer_id",
            new_column="cust_id_norm",
            parameters="customer_id"
        )
    )
    # Format full name
    .transform(
        apply_func(
            func_name="format_full_name",
            new_column="full_name",
            parameters={"first_name": "first_name", "last_name": "last_name"}
        )
    )
    # Standardize location
    .transform(
        apply_func(
            func_name="standardize_country",
            new_column="country_standard",
            parameters="country"
        )
    )
)
```

## Common Patterns

### Safe Type Casting

Handle data type conversions safely without failing on bad data:

```python
df = raw_data.transform(
    apply_schema(
        "my_table",
        safe_cast=True,      # Incompatible values become NULL
        safe_cast=True       # Better error handling than strict casting
    )
)
```

### Adding Default Values

Auto-create missing columns with a specific default:

```python
df = raw_data.transform(
    apply_schema(
        "target_schema",
        add_missing_columns=True,
        missing_column_default="UNKNOWN"  # String default for new columns
    )
)
```

### Column Renaming via Functions

Use functions to rename and transform columns simultaneously:

```python
df = raw_data.select(
    col("old_customer_id").alias("customer_id"),
    col("old_email").alias("email")
).transform(
    apply_schema("customers")
)
```

## Best Practices

1. **Use Kelp metadata first** - Reference schemas from `kelp_metadata/models` when available for consistency.

2. **Chain transformations** - Use `DataFrame.transform()` for readability and composability.

3. **Enable safe casting** - Use `safe_cast=True` when handling unvalidated external data.

4. **Document function purposes** - Add comments explaining what each function does.

5. **Test with sample data** - Verify transformations work correctly before running on production data.

6. **Order transformations logically** - Apply schema first, then functions, to ensure columns exist before applying functions.

7. **Handle NULL values** - Functions should gracefully handle `NULL` inputs per your business logic.

## See Also

- [Functions](functions.md) - Defining Unity Catalog functions for use with transformations
- [Transformations in SDP](sdp.md) - Using transformations in Spark Declarative Pipelines
- [Sync Metadata with Your Catalog](catalog.md) - Registering functions in Unity Catalog
