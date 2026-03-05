---
name: kelp-etl-builder
description: "**WORKFLOW SKILL** — Build end-to-end Databricks Spark and SDP (Spark Declarative Pipelines) ETL jobs using kelp-core. Use when: creating new data pipelines, bronze/silver/gold transformations, adding tables with metadata, defining quality checks, implementing streaming tables, setting up sources, or working with Unity Catalog sync. Covers project setup, YAML metadata definition, Python transformations with kelp decorators, quality validation, and catalog management. NOT for: debugging existing pipelines (use default agent), modifying kelp-core internals, or Unity Catalog SDK operations."
---

# Kelp ETL Builder

Build production-ready Spark and SDP ETL pipelines with kelp-core's metadata-driven approach.

## When to Use

- Creating new bronze/silver/gold layer transformations
- Adding tables with schema definitions and metadata
- Implementing data quality checks and quarantine patterns
- Setting up sources (volumes, tables, streaming)
- Building SDP pipelines with minimal boilerplate
- Syncing metadata to Unity Catalog
- Converting legacy Spark/SDP code to kelp patterns

## Prerequisites Check

Before starting, verify the project structure:

1. **Project file exists**: Look for `kelp_project.yml` in workspace root or ETL source folder
2. **Metadata directories**: Check for `kelp_metadata/models/`, `sources/`, etc.
3. **kelp-core installed**: Run `uv run python -c "import kelp; print(kelp.__version__)"`

If missing, initialize:
```bash
uv add kelp-core
kelp init .
```

## Workflow Steps

### 1. Understand Requirements

Gather the following information from the user (use ask-questions tool if available):

- **Layer**: bronze, silver, gold, or custom
- **Table name**: Following naming conventions
- **Source**: Where data comes from (upstream table, file path, streaming source)
- **Transformation logic**: Business rules, filtering, joins, aggregations
- **Schema**: Column names, types, descriptions
- **Quality checks**: Validation rules (fail fast, quarantine, warn)
- **Catalog/Schema**: Target Unity Catalog location

### 2. Define Metadata (YAML)

Create or update YAML in `kelp_metadata/models/<layer>/<table_name>.yml`:

**Template Pattern:**
```yaml
kelp_models:
  - name: <table_name>
    description: "Clear business description"
    tags:
      domain: <business_domain>
      stage: <bronze|silver|gold>
    columns:
      - name: <column_name>
        data_type: <spark_type>
        description: "Column purpose"
        tags:
          pii: ""  # Optional: mark sensitive data
      - name: user_id
        data_type: string
        description: "Unique user identifier"
    quality:
      engine: sdp  # or dqx
      expect_all_or_fail:
        <check_name>: <sql_expression>
      expect_all_or_quarantine:  # Creates _validation and _quarantine tables
        <check_name>: <sql_expression>
```

**Quality Engine Selection:**
- `sdp`: Native Databricks expectations (recommend for simplicity)
- `dqx`: Advanced DQX library (use for complex validations)

**Key Configuration Inheritance:**
- Project-level defaults in `kelp_project.yml` under `models:` with `+catalog`, `+schema`
- Directory-level configs apply to all models in that folder

**Quality Pattern Examples:**
```yaml
quality:
  engine: sdp
  expect_all_or_fail:  # Pipeline fails if violated
    not_null: column_name IS NOT NULL
    valid_status: status IN ('active', 'inactive')
  expect_all_or_quarantine:  # Violations go to <table>_quarantine
    country_valid: country IN (SELECT code FROM ref_countries)
```

### 3. Create Transformation Code (Python)

Location: `src/<project>/transformations/<layer>/<table_name>.py`

**Import Pattern (ALWAYS use this):**
```python
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from kelp import pipelines as kp

spark = SparkSession.active()
```

**Table Decorator Patterns:**

**Pattern A: Streaming Table (Bronze/Silver)**
```python
@kp.table()  # Auto-injects: name, schema, quality, catalog, etc.
def <table_name>():
    """Docstring describing transformation."""
    return spark.readStream.table(kp.ref("upstream_table"))
```

**Pattern B: Materialized View (Gold/Aggregations)**
```python
@kp.materialized_view()
def <table_name>():
    """Business metric calculation."""
    return spark.read.table(kp.ref("source_table"))
```

**Pattern C: With Transformations**
```python
@kp.table()
def silver_customers_cleaned():
    df = spark.readStream.table(kp.ref("bronze_customers"))
    return (df
        .filter("_rescued_data IS NULL")
        .drop("_rescued_data")
        .withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))))
```

**Pattern D: Exclude Specific Parameters**
```python
@kp.table(exclude_params=["schema"])  # Don't pass schema to SDP
def <table_name>():
    return spark.readStream.table(kp.ref("source"))
```

**Pattern E: Low-Level API (No Decorator)**
```python
@dp.table(**kp.params("my_table"))
def my_table():
    return spark.readStream.table(kp.ref("source"))

# Or with exclusions:
@dp.table(**kp.params("my_table", exclude=["schema"]))
```

**Pattern F: Quarantine Target**
```python
@dp.append_flow(target=kp.target("my_table"))
def my_table_flow():
    """Use kp.target() instead of string for quarantine support."""
    df = spark.readStream.table(kp.ref("source"))
    return df
```

### 4. Source Configuration

For external data sources, create `kelp_metadata/sources/<source_name>.yml`:

**Volume/File Source:**
```yaml
kelp_sources:
  - name: landing_json
    type: volume
    path: /Volumes/${catalog}/${schema}/landing/json
    options:
      format: json
      cloudFiles.format: json
      cloudFiles.schemaLocation: /checkpoint/landing_json
```

**Table Reference:**
```yaml
kelp_sources:
  - name: reference_countries
    type: table
    catalog: shared_catalog
    schema: reference
    table: countries
```

**Use in Code:**
```python
# For table sources
fqn = kp.source("reference_countries")  # Returns "catalog.schema.table"

# For volume/file sources
path = kp.source("landing_json")
options = kp.source_options("landing_json")
df = spark.readStream.format("cloudFiles").options(**options).load(path)
```

### 5. Helper Functions Reference

**Core Functions (import via `from kelp import pipelines as kp`):**

- `kp.ref("table_name")` → Fully qualified name for reading: `catalog.schema.table`
- `kp.target("table_name")` → Target with quarantine support (use in `@dp.append_flow`)
- `kp.schema("table_name")` → DDL schema string from metadata
- `kp.params("table_name")` → All table parameters as dict for `@dp.table(**params)`
- `kp.source("source_name")` → Source path or fully qualified name
- `kp.source_options("source_name")` → Source options dict

**Initialization (usually not needed in SDP, auto-discovered):**
```python
from kelp import init, get_context

ctx = init()  # Auto-discover kelp_project.yml
ctx = init(target="prod")  # Load prod target variables
ctx = init(overwrite_vars={"catalog": "custom"})  # Override variables
```

### 6. Validation and Testing

**Local Validation:**
```bash
# Validate configuration
uv run kelp validate

# Check for errors
uv run kelp catalog diff  # Preview changes before sync
```

**Quality Check Verification:**
- Expectations in metadata → auto-applied by `@kp.table()` decorator
- Quarantine tables: `<table>_validation` and `<table>_quarantine` created automatically
- Check quarantine: `SELECT * FROM <catalog>.<schema>.<table>_quarantine LIMIT 10`

### 7. Catalog Sync

After pipeline runs successfully:

```bash
# Sync metadata to Unity Catalog (descriptions, tags, properties)
uv run kelp catalog sync

# Or with target
uv run kelp catalog sync --target prod
```

**What gets synced:**
- Table and column descriptions
- Tags (table-level and column-level)
- Custom properties
- ABAC policies (if defined)
- Function definitions

## Code Style Requirements

**From AGENTS.md (ALWAYS follow):**

- ✅ **Absolute imports**: `from kelp.config import init`
- ✅ **Type hints**: `def process(name: str) -> DataFrame:`
- ✅ **Builtin types**: `str | None` not `Optional[str]`
- ✅ **Google docstrings**: All public functions
- ✅ **Run via uv**: `uv run python script.py`
- ✅ **Naming**: `snake_case` functions, `PascalCase` classes
- ❌ **Never**: Relative imports, root logger, hardcoded paths

## Common Patterns

### Bronze Layer (Ingestion)
```python
@kp.table(exclude_params=["schema"])
def bronze_customers():
    """Raw ingestion from landing zone."""
    return spark.readStream.table("source_customers")
```

### Silver Layer (Cleaned)
```python
@kp.table()
def silver_customers_cleaned():
    """Cleaned and validated customer data."""
    df = spark.readStream.table(kp.ref("bronze_customers"))
    return df.filter("_rescued_data IS NULL").drop("_rescued_data")
```

### Gold Layer (Business Logic)
```python
@kp.materialized_view()
def gold_customer_summary():
    """Customer metrics with orders."""
    customers = spark.read.table(kp.ref("silver_customers_cleaned"))
    orders = spark.read.table(kp.ref("silver_orders_cleaned"))
    return customers.join(orders, "user_id").groupBy("user_id").agg(...)
```

### With Quality Quarantine
```yaml
# In YAML metadata
quality:
  engine: sdp
  expect_all_or_quarantine:
    valid_country: country IN ('US', 'CA', 'UK')
    email_format: email LIKE '%@%'
```

```python
@kp.table()
def silver_users_validated():
    """Validates user data with quarantine."""
    return spark.readStream.table(kp.ref("bronze_users"))
```

## Architecture Reference

**Key Files to Check:**
- `kelp_project.yml` - Project config, variables, targets
- `kelp_metadata/models/**/*.yml` - Table definitions
- `kelp_metadata/sources/**/*.yml` - External source configs
- `src/<project>/transformations/**/*.py` - Pipeline code

**Directory Structure:**
```
project_root/
├── kelp_project.yml           # Main config
├── src/<project>/
│   └── transformations/
│       ├── 0_sources/         # Source definitions
│       ├── 1_bronze/          # Raw ingestion
│       ├── 2_silver/          # Cleaned/validated
│       └── 3_gold/            # Business aggregations
└── kelp_metadata/
    ├── models/
    │   ├── bronze/
    │   ├── silver/
    │   └── gold/
    ├── sources/
    ├── functions/
    └── abacs/
```

## Troubleshooting

**Common Issues:**

1. **Table not found in catalog**
   - Check `kelp_project.yml` has correct `+catalog` and `+schema` under `models:`
   - Verify hierarchy: project → directory → table-level configs
   - Run `uv run kelp validate` to see resolved configuration

2. **Quality checks not applied**
   - Ensure `quality:` section exists in table YAML
   - Verify `engine: sdp` or `engine: dqx` is set
   - Check decorator usage: `@kp.table()` not `@dp.table`

3. **Import errors**
   - Always use: `from kelp import pipelines as kp`
   - Run via: `uv run python script.py` (not `python` directly)
   - Check kelp-core installed: `uv run python -c "import kelp"`

4. **Variables not interpolating**
   - Use `${ var_name }` syntax in YAML (Jinja2 with custom delimiters)
   - Define in `vars:` section of `kelp_project.yml`
   - Override with targets: `targets: prod: vars: catalog: prod_catalog`

## Example Workflow

**User Request:** "Create a silver layer table to clean bronze customer data with email validation"

**Agent Actions:**

1. **Create YAML** at `kelp_metadata/models/silver/silver_customers_clean.yml`:
```yaml
kelp_models:
  - name: silver_customers_clean
    description: "Cleaned customer data with validated emails"
    columns:
      - name: user_id
        data_type: string
        description: "Unique customer identifier"
      - name: email
        data_type: string
        description: "Validated customer email"
      - name: full_name
        data_type: string
        description: "Concatenated first and last name"
    quality:
      engine: sdp
      expect_all_or_fail:
        not_null_email: email IS NOT NULL
      expect_all_or_quarantine:
        email_format: email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'
```

2. **Create Python** at `src/project_etl/transformations/2_silver/silver_customers_clean.py`:
```python
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from kelp import pipelines as kp

spark = SparkSession.active()


@kp.table()
def silver_customers_clean():
    """Clean and validate customer data from bronze layer."""
    df = spark.readStream.table(kp.ref("bronze_customers"))
    
    return (df
        .filter("_rescued_data IS NULL")
        .drop("_rescued_data")
        .withColumn("full_name", 
                   F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))))
```

3. **Validate**:
```bash
uv run kelp validate
```

4. **Run Pipeline** (via Databricks Workflows or CLI)

5. **Sync to Catalog**:
```bash
uv run kelp catalog sync
```

## Related Documentation

- **Kelp Docs**: [https://benschr.github.io/kelp-core/](https://benschr.github.io/kelp-core/)
- **SDP Reference**: [https://docs.databricks.com/aws/en/ldp/developer/python-ref](https://docs.databricks.com/aws/en/ldp/developer/python-ref)
- **DQX Quality Checks**: [https://databrickslabs.github.io/dqx/docs/reference/quality_checks/](https://databrickslabs.github.io/dqx/docs/reference/quality_checks/)

## Related Skills

Consider creating complementary skills:
- `kelp-quality-validator` - Deep focus on DQX quality patterns
- `kelp-abac-builder` - Row/column-level access control
- `kelp-metric-views` - Business metric definitions
- `kelp-migration` - Convert legacy Spark/SDP to kelp patterns
