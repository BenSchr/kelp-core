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

**Inventory the codebase manually or with IDE search:**
- Count total pipeline files and table decorators (@dp.table, @dp.streaming_table, etc.)
- Identify layers (bronze/silver/gold or custom)
- Document inline quality checks (SDP expectations)
- Note hardcoded catalog/schema references
- Catalog existing custom transformation logic

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

If tables already exist in Unity Catalog, use the Databricks SDK to fetch table details and metadata:

1. Connect to your workspace using `WorkspaceClient()`
2. List tables in the catalog: `w.tables.list(catalog_name=..., schema_name=...)`
3. For each table, retrieve full schema information
4. Generate YAML files mapping table names to column definitions

**Key fields to extract:**
- `table.name` → YAML model name
- `table.comment` → description
- `table.columns[].name` → column name
- `table.columns[].type_text` → data_type
- `table.columns[].comment` → column description
- Primary and foreign key constraints (if defined)

#### Pattern B: From Spark Schema in Code

Extract schema definitions from existing PySpark code:

**Original Code (contains inline schema):**
```python
@dp.table(
    name="silver_customers",
    comment="Customer data"
)
def silver_customers():
    df = spark.readStream.table("bronze.customers")
    # Schema is either checked/enforced or inferred
    return df
```

**Extract column definitions to YAML:**
```yaml
kelp_models:
  - name: silver_customers
    description: "Customer data"
    columns:
      - name: user_id
        data_type: string
        description: "User identifier"
      - name: email
        data_type: string
        description: "User email address"
      - name: full_name
        data_type: string
        description: "User full name"
```

**How to find columns:**
- Read inline StructType definitions
- Query existing table schema: `SHOW COLUMNS FROM bronze.customers`
- Use Databricks table metadata viewer in UC browser

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
- Backup original code to a feature branch (`git checkout -b feature/kelp-migration`)
- Install kelp-core (instructions in README)
- Initialize project with `kelp init .`
- Create directory structure: `kelp_metadata/models/`

**Post-Migration Validation Approach:**

**A. Schema Parity:**
Compare original and migrated table schemas side-by-side. Use IDE or Databricks SQL to verify columns, data types, and nullability are identical.

**B. Data Parity:**
Query both original and migrated tables and verify row counts match. Sample data to ensure no unexpected transformations were introduced.

**C. Quality Checks:**
Verify quarantine tables (`*_validation`, `*_quarantine`) are created for tables with expectations. Monitor their contents.

**D. Catalog Metadata:**
- Run `uv run kelp validate` to check YAML configuration
- Use `uv run kelp generate-ddl` to preview generated DDL
- Verify tags, descriptions, and parameters match your requirements

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

**Validation Approach:**

1. Run both tables in parallel for 1-2 weeks
2. Query both original and migrated tables and compare schema (use `DESCRIBE TABLE`)
3. Verify row counts match
4. Sample data to ensure transformations are identical

**Cutover:**
1. Validation successful? Rename `silver_customers_kelp` → `silver_customers` in YAML
2. Update downstream consumers to use kelp version
3. Deprecate old pipeline
4. Monitor for 1 week before removing old code

### 7. Refactor Complex Transformations

**Pattern: Extract Business Logic**

For monolithic transformation tables with 50+ lines of logic:

**Strategy:**
1. Identify distinct transformation steps (joins, aggregations, enrichments)
2. Extract each into a separate reusable function with clear purpose
3. Use `kp.ref()` to reference input tables
4. Use `kp.function()` or `kp.params()` to inject shared logic

**Example structure:**
```python
@kp.table()
def gold_customer_360():
    """Comprehensive customer view."""
    # Use helper functions for distinct steps
    customers = spark.read.table(kp.ref("silver_customers"))
    orders = spark.read.table(kp.ref("silver_orders"))
    events = spark.read.table(kp.ref("silver_events"))
    
    df = join_customer_data(customers, orders, events)
    df = calculate_customer_metrics(df)
    
    return df

def join_customer_data(customers, orders, events):
    """Join step isolated."""
    return customers.join(orders, ...).join(events, ...)

def calculate_customer_metrics(df):
    """Metric calculation isolated."""
    return df.withColumn("ltv", ...)
```

**Key Benefits:**
- Easier to test and debug each transformation step
- Reusable functions can be shared across tables
- Schema changes can be isolated to metadata
- Quality checks defined in YAML, not in code

## Backwards Compatibility

**Maintain Both Patterns During Migration:**

Kelp-decorated tables coexist with native SDP tables. Old decorators still work:

```python
from pyspark import pipelines as dp
from kelp import pipelines as kp

# Old style (native SDP)
@dp.table(name="bronze.legacy_table")
def legacy_table():
    return spark.readStream.table("source.data")

# New style (kelp with metadata)
@kp.table()
def modern_table():
    return spark.readStream.table(kp.ref("legacy_table"))
```

Kelp tables can reference legacy tables via fully qualified names. No need for a big-bang migration.

**Adoption Strategy:**
- Start with net-new tables using kelp decorators and YAML metadata
- Migrate existing tables opportunistically during regular updates
- No breaking changes; parallel patterns coexist during transition

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
3. **Extract Systematically**: Use Databricks SDK or IDE search to locate all references
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
- [ ] Update documentation
- [ ] Monitor quarantine tables
- [ ] Deprecate old code
- [ ] Remove old code after grace period

## Related Skills

- `kelp-etl-builder` - Building new pipelines from scratch
- `kelp-quality-validator` - Quality checks and validation
- `agent-customization` - Creating migration automation skills
