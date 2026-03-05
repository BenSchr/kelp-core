---
name: kelp-migration
description: "**WORKFLOW SKILL** — Convert existing Spark and SDP (Spark Declarative Pipelines) code to kelp-core patterns. Use when: migrating legacy pipelines to kelp, refactoring hardcoded catalog/schema references, extracting metadata from code to YAML, converting manual expectations to kelp quality checks, or modernizing existing ETL workflows. Covers codebase analysis, metadata extraction, incremental migration, and backwards compatibility. NOT for: creating new pipelines from scratch (use kelp-etl-builder), or debugging existing kelp code (use default agent)."
---

# Kelp Migration

Systematically convert existing Spark and SDP code to kelp-core's metadata-driven approach.

## When to Use

- Migrating legacy Spark/SDP pipelines to kelp-core
- Refactoring hardcoded catalog/schema references
- Extracting schema definitions from code to YAML
- Converting inline expectations to metadata-driven quality checks
- Modernizing pipeline architecture with minimal disruption
- Creating metadata for undocumented pipelines

## Migration Strategy

```
ASSESS → PLAN → EXTRACT → CONVERT → VALIDATE → ITERATE

├─ ASSESS: Inventory existing code (tables, schemas, quality checks)
├─ PLAN: Prioritize by layer (bronze → silver → gold)
├─ EXTRACT: Generate YAML from code/catalog
├─ CONVERT: Refactor Python to kelp patterns
├─ VALIDATE: Test parity with original
└─ ITERATE: Migrate incrementally, one layer at a time
```

## Workflow Steps

### 1. Assess Current State

**Inventory the codebase:**
```bash
# Find all pipeline files
find . -name "*.py" -path "*/transformations/*" -o -path "*/pipelines/*"

# Count transformation patterns
grep -r "@dp.table\|@dp.streaming_table\|@dp.materialized_view" --include="*.py"

# Find hardcoded references
grep -r "catalog\." --include="*.py" | grep -v "spark.catalog"
```

**Document findings:**
- Total number of tables/views
- Layers identified (bronze/silver/gold or custom)
- Quality checks in code (expectations)
- Hardcoded catalog/schema references
- Custom transformation logic

### 2. Plan Migration Order

**Recommended Sequence:**

1. **Start with Bronze** (least dependencies)
2. **Then Silver** (depends on bronze)
3. **Finally Gold** (depends on silver)
4. **Critical paths first** (high-value, frequently used tables)

**Risk Assessment:**
- ✅ **Low Risk**: Simple tables, no dependencies, no quality checks
- ⚠️ **Medium Risk**: Tables with expectations, schema enforcement
- 🔴 **High Risk**: Complex joins, custom UDFs, production-critical

**Pilot Selection:**
Choose 2-3 tables that are:
- Self-contained (minimal dependencies)
- Representative of common patterns
- Non-critical (safe to test)

### 3. Extract Metadata from Existing Code

#### Pattern A: From Unity Catalog

If tables already exist in Unity Catalog:

```python
# Script: extract_catalog_metadata.py
from databricks.sdk import WorkspaceClient
import yaml

w = WorkspaceClient()

catalog_name = "your_catalog"
schema_name = "your_schema"

tables = w.tables.list(catalog_name=catalog_name, schema_name=schema_name)

for table in tables:
    # Get table details
    table_info = w.tables.get(full_name=f"{catalog_name}.{schema_name}.{table.name}")
    
    # Generate YAML
    model = {
        "name": table.name,
        "description": table_info.comment or "",
        "columns": [
            {
                "name": col.name,
                "data_type": col.type_text,
                "description": col.comment or ""
            }
            for col in table_info.columns
        ]
    }
    
    # Write to file
    with open(f"kelp_metadata/models/{schema_name}/{table.name}.yml", "w") as f:
        yaml.dump({"kelp_models": [model]}, f, sort_keys=False)
```

#### Pattern B: From Spark Schema in Code

Extract from existing PySpark code:

**Original Code:**
```python
@dp.table(
    name="silver_customers",
    comment="Customer data"
)
def silver_customers():
    df = spark.readStream.table("bronze.customers")
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("email", StringType(), True),
        StructField("full_name", StringType(), True)
    ])
    return df
```

**Extract to YAML:**
```yaml
kelp_models:
  - name: silver_customers
    description: "Customer data"
    columns:
      - name: user_id
        data_type: string
        description: ""
      - name: email
        data_type: string
        description: ""
      - name: full_name
        data_type: string
        description: ""
```

#### Pattern C: From SDP Expectations

**Original Code:**
```python
@dp.table(
    name="silver_customers",
    expect={
        "valid_email": "email IS NOT NULL AND email LIKE '%@%'",
        "user_id_not_null": "user_id IS NOT NULL"
    }
)
def silver_customers():
    return spark.readStream.table("bronze.customers")
```

**Extract to YAML:**
```yaml
kelp_models:
  - name: silver_customers
    description: "Customer data with validation"
    columns: [...]  # Add columns from catalog or schema
    quality:
      engine: sdp
      expect_all_or_fail:
        user_id_not_null: user_id IS NOT NULL
      expect_all_or_quarantine:
        valid_email: email IS NOT NULL AND email LIKE '%@%'
```

### 4. Convert Code Patterns

#### Migration A: Basic Table Decorator

**Before (Native SDP):**
```python
from pyspark import pipelines as dp

@dp.table(
    name="bronze.customers",
    comment="Raw customer data"
)
def bronze_customers():
    return spark.readStream.table("landing.customers")
```

**After (Kelp):**
```python
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from kelp import pipelines as kp

spark = SparkSession.active()

@kp.table()  # Metadata from YAML
def bronze_customers():
    """Raw customer data"""
    return spark.readStream.table(kp.ref("landing_customers"))
```

**YAML (kelp_metadata/models/bronze/bronze_customers.yml):**
```yaml
kelp_models:
  - name: bronze_customers
    description: "Raw customer data"
    columns:
      - name: user_id
        data_type: string
        description: "Customer identifier"
```

#### Migration B: Hardcoded References

**Before:**
```python
@dp.table(name="production.silver.customers")
def silver_customers():
    bronze_df = spark.readStream.table("production.bronze.customers")
    ref_df = spark.read.table("shared.reference.countries")
    return bronze_df.join(ref_df, ...)
```

**After:**
```python
@kp.table()
def silver_customers():
    """Join customers with reference data."""
    bronze_df = spark.readStream.table(kp.ref("bronze_customers"))
    ref_df = spark.read.table(kp.source("ref_countries"))
    return bronze_df.join(ref_df, ...)
```

**kelp_project.yml:**
```yaml
kelp_project:
  models:
    +catalog: production
    bronze:
      +schema: bronze
    silver:
      +schema: silver

vars:
  catalog: production  # Override per target
```

**sources/ref_countries.yml:**
```yaml
kelp_sources:
  - name: ref_countries
    type: table
    catalog: shared
    schema: reference
    table: countries
```

#### Migration C: Expectations to Quality Checks

**Before:**
```python
@dp.table(
    name="silver.customers_validated",
    expect={
        "pk_not_null": "user_id IS NOT NULL",
        "email_valid": "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'"
    }
)
def customers_validated():
    return spark.readStream.table("bronze.customers")
```

**After:**
```python
@kp.table()
def silver_customers_validated():
    """Validated customers with quarantine."""
    return spark.readStream.table(kp.ref("bronze_customers"))
```

**YAML:**
```yaml
kelp_models:
  - name: silver_customers_validated
    description: "Validated customers with quarantine"
    columns: [...]
    quality:
      engine: sdp
      expect_all_or_fail:
        pk_not_null: user_id IS NOT NULL
      expect_all_or_quarantine:
        email_valid: email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'
```

#### Migration D: Schema Enforcement

**Before:**
```python
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("email", StringType(), True)
])

@dp.table(name="silver.customers", schema=schema)
def silver_customers():
    return spark.readStream.table("bronze.customers")
```

**After:**
```python
@kp.table()
def silver_customers():
    """Schema enforced from metadata."""
    df = spark.readStream.table(kp.ref("bronze_customers"))
    
    # Optional: explicit schema enforcement
    from kelp.transformations import apply_schema
    return apply_schema(df, kp.schema("silver_customers"))
```

**YAML:**
```yaml
kelp_models:
  - name: silver_customers
    description: "Customers with enforced schema"
    columns:
      - name: user_id
        data_type: string
        description: "Customer ID"
      - name: email
        data_type: string
        description: "Customer email"
```

#### Migration E: Append Flow to Quarantine

**Before:**
```python
@dp.append_flow(
    target="silver.validated_orders",
    expect={
        "amount_positive": "amount > 0"
    }
)
def validated_orders():
    return spark.readStream.table("bronze.orders")
```

**After:**
```python
@dp.append_flow(target=kp.target("silver_validated_orders"))
def silver_validated_orders():
    """Orders with quarantine support."""
    return spark.readStream.table(kp.ref("bronze_orders"))
```

**YAML:**
```yaml
kelp_models:
  - name: silver_validated_orders
    description: "Validated orders with quarantine"
    columns: [...]
    quality:
      engine: sdp
      expect_all_or_quarantine:
        amount_positive: amount > 0
```

### 5. Validate Migration

**Pre-Migration Checklist:**
```bash
# 1. Backup original code
git checkout -b feature/kelp-migration
cp -r transformations transformations.backup

# 2. Install kelp-core
uv add kelp-core

# 3. Initialize project
kelp init .

# 4. Validate configuration
uv run kelp validate
```

**Post-Migration Validation:**

**A. Schema Parity:**
```python
# Compare schemas
original_table = spark.read.table("catalog.schema.original_table")
kelp_table = spark.read.table(kp.ref("migrated_table"))

assert original_table.schema == kelp_table.schema, "Schema mismatch!"
```

**B. Data Parity:**
```python
# Compare record counts
original_count = spark.read.table("catalog.schema.original").count()
kelp_count = spark.read.table(kp.ref("migrated_table")).count()

assert original_count == kelp_count, f"Count mismatch: {original_count} vs {kelp_count}"
```

**C. Quality Checks:**
```bash
# Check quarantine tables created
uv run python -c "
from kelp import init
ctx = init()
tables = ctx.list_tables()
for t in tables:
    if t.quality and 'expect_all_or_quarantine' in t.quality:
        print(f'{t.name} has quarantine enabled')
"
```

**D. Catalog Metadata:**
```bash
# Sync and verify
uv run kelp catalog sync
uv run kelp catalog diff  # Should show no changes after sync
```

### 6. Incremental Migration Pattern

**Approach: Parallel Run**

Keep both old and new pipelines running temporarily:

```python
# OLD: production/transformations/silver_customers.py
@dp.table(name="silver.customers")
def silver_customers():
    return spark.readStream.table("bronze.customers")

# NEW: production/transformations/silver_customers_kelp.py
@kp.table()  # Writes to silver.customers_kelp initially
def silver_customers_kelp():
    return spark.readStream.table(kp.ref("bronze_customers"))
```

**YAML (temp name):**
```yaml
kelp_models:
  - name: silver_customers_kelp  # Temporary name
    description: "Migrated customer data (testing)"
    # Same schema as silver_customers
```

**Validation Script:**
```python
# Compare outputs
old_df = spark.read.table("silver.customers")
new_df = spark.read.table("silver.customers_kelp")

# Schema check
assert old_df.schema == new_df.schema

# Data check
diff_count = old_df.exceptAll(new_df).count()
assert diff_count == 0, f"Found {diff_count} differing records"

print("✅ Migration validated successfully")
```

**Cutover:**
1. Validate parallel run for 1-2 weeks
2. Update downstream consumers to use kelp version
3. Rename `silver_customers_kelp` → `silver_customers` in YAML
4. Deprecate old pipeline
5. Monitor for 1 week before removing old code

### 7. Refactor Complex Transformations

**Pattern: Extract Business Logic**

**Before (Monolithic):**
```python
@dp.table(name="gold.customer_360")
def customer_360():
    customers = spark.read.table("silver.customers")
    orders = spark.read.table("silver.orders")
    events = spark.read.table("silver.events")
    
    # 50+ lines of transformation logic...
    result = customers.join(orders, ...).join(events, ...)
    # ... complex aggregations ...
    
    return result
```

**After (Modular with Kelp):**
```python
from kelp import pipelines as kp
from kelp.transformations import apply_function

@kp.table()
def gold_customer_360():
    """Comprehensive customer view."""
    customers = spark.read.table(kp.ref("silver_customers"))
    orders = spark.read.table(kp.ref("silver_orders"))
    events = spark.read.table(kp.ref("silver_events"))
    
    # Use composable transformations
    df = join_customer_data(customers, orders, events)
    df = calculate_customer_metrics(df)
    df = apply_function(df, kp.function("calculate_ltv"))
    
    return df

def join_customer_data(customers, orders, events):
    """Separate function for joining logic."""
    # Extraction logic here
    return result

def calculate_customer_metrics(df):
    """Separate function for metric calculations."""
    # Calculation logic here
    return df
```

**YAML (with reusable function):**
```yaml
# functions/customer_functions.yml
kelp_functions:
  - name: calculate_ltv
    description: "Calculate customer lifetime value"
    language: python
    source_file: "python/calculate_ltv.py"
```

## Migration Helpers

### Automated Extraction Script

```python
#!/usr/bin/env python3
"""
Extract metadata from existing SDP code to kelp YAML.
Usage: python extract_metadata.py <pipeline_file.py>
"""
import ast
import yaml
import sys
from pathlib import Path

def extract_table_metadata(python_file: str) -> dict:
    """Parse Python file and extract SDP decorator metadata."""
    with open(python_file) as f:
        tree = ast.parse(f.read())
    
    metadata = []
    
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            # Find @dp.table decorator
            for decorator in node.decorator_list:
                if (isinstance(decorator, ast.Call) and
                    hasattr(decorator.func, 'attr') and
                    decorator.func.attr == 'table'):
                    
                    # Extract parameters
                    table_info = {
                        "name": node.name,
                        "description": ast.get_docstring(node) or "",
                        "columns": []
                    }
                    
                    # Extract expectations
                    for keyword in decorator.keywords:
                        if keyword.arg == 'expect':
                            quality = {"engine": "sdp", "expect_all_or_fail": {}}
                            # Parse expect dict
                            # ... (implementation details)
                            table_info["quality"] = quality
                    
                    metadata.append(table_info)
    
    return {"kelp_models": metadata}

if __name__ == "__main__":
    file_path = sys.argv[1]
    metadata = extract_table_metadata(file_path)
    
    # Output YAML
    output_file = f"kelp_metadata/models/{Path(file_path).stem}.yml"
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, "w") as f:
        yaml.dump(metadata, f, sort_keys=False, default_flow_style=False)
    
    print(f"✅ Generated {output_file}")
```

### Bulk Find-Replace Patterns

```bash
# Replace hardcoded table references
find ./transformations -name "*.py" -type f -exec sed -i \
  's/spark\.readStream\.table("\([^"]*\)\.\([^"]*\)\.\([^"]*\)")/spark.readStream.table(kp.ref("\3"))/g' {} +

# Replace @dp.table with @kp.table
find ./transformations -name "*.py" -type f -exec sed -i \
  's/@dp\.table/@kp.table/g' {} +

# Add kelp import
find ./transformations -name "*.py" -type f -exec sed -i \
  '/from pyspark import pipelines as dp/a from kelp import pipelines as kp' {} +
```

## Backwards Compatibility

**Maintain Both Patterns During Migration:**

```python
# kelp_project.yml can coexist with native SDP
from pyspark import pipelines as dp
from kelp import pipelines as kp

# Old style still works
@dp.table(name="bronze.legacy_table")
def legacy_table():
    return spark.readStream.table("source.data")

# New style uses kelp
@kp.table()
def modern_table():
    return spark.readStream.table(kp.ref("legacy_table"))  # Can still reference legacy
```

**Gradual Adoption:**
- Start with net-new tables using kelp
- Migrate existing tables opportunistically during updates
- No need for big-bang migration

## Common Migration Challenges

### Challenge 1: Dynamic Table Names

**Before:**
```python
env = "prod"
table_name = f"{env}.silver.customers"

@dp.table(name=table_name)
def customers():
    return spark.readStream.table(f"{env}.bronze.customers")
```

**After:**
```python
# Use targets in kelp_project.yml
@kp.table()
def silver_customers():
    return spark.readStream.table(kp.ref("bronze_customers"))
```

**kelp_project.yml:**
```yaml
vars:
  catalog: prod

targets:
  dev:
    vars:
      catalog: dev
  prod:
    vars:
      catalog: prod

kelp_project:
  models:
    +catalog: ${catalog}
    bronze:
      +schema: bronze
    silver:
      +schema: silver
```

### Challenge 2: Complex Expectations

**Before (nested logic):**
```python
@dp.table(
    expect={
        "complex_rule": "(status = 'active' AND email IS NOT NULL) OR (status = 'inactive')"
    }
)
```

**After (split into simpler rules):**
```yaml
quality:
  engine: sdp
  expect_all_or_quarantine:
    active_has_email: "status != 'active' OR email IS NOT NULL"
    status_valid: "status IN ('active', 'inactive')"
```

### Challenge 3: Schema Evolution

**Original code modifies schema:**
```python
@dp.table
def silver_customers():
    df = spark.readStream.table("bronze.customers")
    return df.withColumn("processed_at", F.current_timestamp())
```

**Kelp approach:**
```yaml
# Add to YAML schema
columns:
  - name: processed_at
    data_type: timestamp
    description: "Processing timestamp"
```

```python
@kp.table()
def silver_customers():
    df = spark.readStream.table(kp.ref("bronze_customers"))
    return df.withColumn("processed_at", F.current_timestamp())
```

## Best Practices

1. **Migrate Layer by Layer**: Start with bronze, then silver, then gold
2. **Run in Parallel**: Keep old and new pipelines running during validation
3. **Automate What You Can**: Use extraction scripts for bulk metadata
4. **Test Thoroughly**: Schema parity, data parity, count checks
5. **Document Changes**: Update README with migration status
6. **Incremental Cutover**: One table at a time, not all at once
7. **Monitor Post-Migration**: Watch quarantine tables, check data quality

## Migration Checklist

**Pre-Migration:**
- [ ] Backup original code to separate branch
- [ ] Install kelp-core: `uv add kelp-core`
- [ ] Initialize project: `kelp init .`
- [ ] Create directory structure: `kelp_metadata/models/`, etc.

**Per Table:**
- [ ] Extract metadata to YAML
- [ ] Convert decorator to `@kp.table()`
- [ ] Replace hardcoded references with `kp.ref()`
- [ ] Convert expectations to quality checks
- [ ] Validate schema parity
- [ ] Validate data parity
- [ ] Run parallel for validation period

**Post-Migration:**
- [ ] Sync to catalog: `uv run kelp catalog sync`
- [ ] Update documentation
- [ ] Monitor quarantine tables
- [ ] Deprecate old code
- [ ] Remove old code after grace period

## Related Skills

- `kelp-etl-builder` - Building new pipelines from scratch
- `kelp-quality-validator` - Quality checks and validation
- `agent-customization` - Creating migration automation skills
