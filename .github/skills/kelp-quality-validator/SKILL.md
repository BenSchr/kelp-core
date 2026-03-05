---
name: kelp-quality-validator
description: "**WORKFLOW SKILL** — Design and implement data quality checks for kelp-core ETL pipelines. Use when: adding validation rules, implementing quarantine patterns, choosing between SDP expectations vs DQX checks, debugging quality failures, setting up data contracts, or creating validation layers. Covers quality check patterns, quarantine table setup, DQX integration, and validation troubleshooting. NOT for: creating tables without quality logic (use kelp-etl-builder), or general debugging (use default agent)."
---

# Kelp Quality Validator

Design robust data quality checks and validation strategies for kelp-core ETL pipelines.

## When to Use

- Adding or refining quality checks to existing tables
- Implementing quarantine patterns for bad records
- Choosing between SDP expectations and DQX checks
- Debugging quality check failures
- Setting up validation layers (e.g., silver_validated tables)
- Creating data quality contracts and SLAs
- Investigating quarantine tables

## Quality Engine Decision Tree

```
START: What validation complexity do you need?

├─ Simple SQL expressions (NOT NULL, IN list, regex)
│  └─> Use SDP expectations
│     - Native Databricks feature
│     - Minimal setup
│     - Good for 80% of cases
│
└─ Complex validations (cross-table checks, statistical outliers, custom functions)
   └─> Use DQX
      - Advanced validation library
      - Requires: from databricks.labs import dqx
      - Better error reporting
```

## Workflow Steps

### 1. Identify Validation Requirements

Gather from user (use ask-questions tool if available):

- **Critical validations**: Pipeline fails if violated (fail fast)
- **Warning validations**: Log but don't fail (monitoring)
- **Quarantine validations**: Separate bad records for review
- **Data types**: Primary keys, foreign keys, measures, dimensions
- **Business rules**: Domain-specific constraints

**Common Validation Types:**
- **Completeness**: NOT NULL checks, required field presence
- **Uniqueness**: Primary key constraints, deduplication
- **Validity**: Format checks (email, phone, date ranges)
- **Consistency**: Foreign key relationships, reference data
- **Accuracy**: Business logic rules, calculated fields
- **Timeliness**: Freshness checks, timestamp validation

### 2. Define Quality Checks in YAML

Location: `kelp_metadata/models/<layer>/<table>.yml`

#### Pattern A: SDP Expectations (Recommended Start)

**Fail Fast Pattern:**
```yaml
quality:
  engine: sdp
  expect_all_or_fail:  # Pipeline stops on violation
    primary_key_not_null: user_id IS NOT NULL
    valid_status: status IN ('active', 'inactive', 'pending')
    timestamp_recent: event_timestamp >= current_date() - INTERVAL 30 DAYS
    email_format: email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'
```

**Quarantine Pattern:**
```yaml
quality:
  engine: sdp
  expect_all_or_quarantine:  # Bad records → <table>_quarantine
    country_valid: country IN ('US', 'CA', 'UK', 'AU')
    amount_positive: amount > 0
    date_not_future: order_date <= current_date()
```

**Mixed Pattern:**
```yaml
quality:
  engine: sdp
  expect_all_or_fail:
    # Critical: must pass or pipeline fails
    user_id_not_null: user_id IS NOT NULL
    
  expect_all_or_quarantine:
    # Important but not blocking: isolate bad records
    email_format: email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'
    phone_valid: phone RLIKE '^\\+?[0-9]{10,15}$'
```

**Warning Pattern (SDP standard):**
```yaml
quality:
  engine: sdp
  expect_all:  # Log violations but don't fail
    preferred_format: preferred_contact IN ('email', 'phone', 'mail')
```

#### Pattern B: DQX Checks (Advanced)

**Installation Required:**
```bash
uv add databricks-labs-dqx
```

**Basic DQX Pattern:**
```yaml
quality:
  engine: dqx
  checks:
    - name: primary_key_unique
      type: uniqueness
      columns: [user_id]
      action: quarantine
      
    - name: email_completeness
      type: completeness
      columns: [email]
      threshold: 0.95  # 95% must have email
      action: fail
      
    - name: amount_outlier
      type: statistical
      columns: [amount]
      method: zscore
      threshold: 3
      action: warn
```

**DQX Cross-Table Validation:**
```yaml
quality:
  engine: dqx
  checks:
    - name: valid_customer_reference
      type: referential_integrity
      column: customer_id
      reference_table: silver_customers
      reference_column: user_id
      action: quarantine
```

**DQX Custom Function:**
```yaml
quality:
  engine: dqx
  checks:
    - name: business_rule_validation
      type: custom
      function: validate_customer_tier
      action: fail
```

### 3. Implement Quarantine Tables

When using `expect_all_or_quarantine`, kelp auto-creates:

**Main Table**: `catalog.schema.my_table`
- Contains valid records only

**Validation Table**: `catalog.schema.my_table_validation`
- All records with validation status columns
- Columns: `<original_columns>` + `_kelp_validation_failed` + `_kelp_validation_rule`

**Quarantine Table**: `catalog.schema.my_table_quarantine`
- Failed records only
- Same schema as validation table

**Query Quarantine Pattern:**
```sql
-- Check quarantine volume
SELECT COUNT(*), _kelp_validation_rule
FROM catalog.schema.my_table_quarantine
GROUP BY _kelp_validation_rule
ORDER BY COUNT(*) DESC;

-- Review specific failures
SELECT * 
FROM catalog.schema.my_table_quarantine
WHERE _kelp_validation_rule = 'email_format'
LIMIT 100;

-- Track quarantine trends
SELECT 
    date_trunc('day', _kelp_quarantine_timestamp) as date,
    _kelp_validation_rule,
    COUNT(*) as failure_count
FROM catalog.schema.my_table_quarantine
GROUP BY date, _kelp_validation_rule
ORDER BY date DESC;
```

### 4. Apply Quality Checks in Code

**Automatic Application (Recommended):**
```python
from kelp import pipelines as kp

@kp.table()  # Quality checks auto-applied from YAML
def silver_customers_validated():
    """Quality rules applied automatically."""
    return spark.readStream.table(kp.ref("bronze_customers"))
```

**Manual Control (Advanced):**
```python
@kp.table(exclude_params=["expectations"])  # Skip quality checks
def silver_customers_raw():
    """Skip validation for debugging."""
    return spark.readStream.table(kp.ref("bronze_customers"))
```

**Low-Level Quarantine Target:**
```python
from pyspark import pipelines as dp
from kelp import pipelines as kp

@dp.append_flow(target=kp.target("silver_customers"))  # Supports quarantine
def validate_customers():
    df = spark.readStream.table(kp.ref("bronze_customers"))
    # Transformations here
    return df
```

### 5. Validation Layer Pattern

For complex pipelines, create dedicated validation tables:

**bronze → bronze_validated → silver pattern:**

```yaml
# bronze_customers.yml - No quality checks
kelp_models:
  - name: bronze_customers
    description: "Raw ingestion, no validation"
    columns: [...]
    # No quality section

# bronze_customers_validated.yml - Aggressive validation
kelp_models:
  - name: bronze_customers_validated
    description: "Validated bronze with quarantine"
    columns: [...]
    quality:
      engine: sdp
      expect_all_or_fail:
        not_null_pk: user_id IS NOT NULL
      expect_all_or_quarantine:
        email_valid: email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'
        country_known: country != 'unknown'
```

```python
# 1_bronze/bronze_customers.py
@kp.table()
def bronze_customers():
    """Raw ingestion."""
    return spark.readStream.table("source_customers")

# 1_bronze/bronze_customers_validated.py
@kp.table()
def bronze_customers_validated():
    """Validation layer with quarantine."""
    return spark.readStream.table(kp.ref("bronze_customers"))

# 2_silver/silver_customers.py
@kp.table()
def silver_customers():
    """Clean transformation uses validated source."""
    df = spark.readStream.table(kp.ref("bronze_customers_validated"))
    return df.drop("_rescued_data")
```

### 6. DQX Integration (Advanced)

**Enable DQX in kelp_project.yml:**
```yaml
kelp_project:
  models:
    +catalog: ${catalog}
    +quality_engine: dqx  # Override default 'sdp'
```

**DQX Check Library:**
```yaml
# models/silver/silver_transactions.yml
kelp_models:
  - name: silver_transactions
    description: "Validated transactions with DQX"
    columns:
      - name: transaction_id
        data_type: string
      - name: amount
        data_type: decimal(10,2)
      - name: customer_id
        data_type: string
    quality:
      engine: dqx
      checks:
        # Uniqueness
        - name: unique_transaction_id
          type: uniqueness
          columns: [transaction_id]
          action: fail
          
        # Completeness
        - name: required_fields
          type: completeness
          columns: [transaction_id, amount, customer_id]
          threshold: 1.0  # 100% required
          action: fail
          
        # Validity
        - name: amount_positive
          type: validity
          columns: [amount]
          condition: "amount > 0"
          action: quarantine
          
        # Referential Integrity
        - name: valid_customer
          type: referential_integrity
          column: customer_id
          reference_table: silver_customers
          reference_column: user_id
          action: quarantine
          
        # Statistical Outlier
        - name: amount_outlier_detection
          type: statistical
          columns: [amount]
          method: zscore
          threshold: 3
          action: warn
          
        # Custom Business Logic
        - name: business_rule_check
          type: custom
          function: validate_transaction_limits
          action: fail
```

**Custom DQX Function:**
```python
# kelp_metadata/functions/python/validate_transaction_limits.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def validate_transaction_limits(df: DataFrame) -> DataFrame:
    """Custom business rule: transactions must respect daily limits."""
    return df.filter(
        (F.col("transaction_type") == "credit") | 
        (F.col("amount") <= 10000)
    )
```

### 7. Testing and Monitoring

**Pre-Deployment Testing:**
```bash
# Validate YAML syntax and structure
uv run kelp validate

# Preview catalog changes
uv run kelp catalog diff

# Test quality checks locally (if supported)
uv run python -m pytest tests/quality/
```

**Monitor Quarantine Tables:**
```python
# Create monitoring query
from pyspark.sql import functions as F

quarantine = spark.read.table("catalog.schema.my_table_quarantine")

# Summary metrics
quarantine.groupBy("_kelp_validation_rule").agg(
    F.count("*").alias("failure_count"),
    F.min("_kelp_quarantine_timestamp").alias("first_failure"),
    F.max("_kelp_quarantine_timestamp").alias("last_failure")
).show()

# Alert on threshold
failure_rate = (
    quarantine.count() / 
    spark.read.table("catalog.schema.my_table").count()
)
if failure_rate > 0.05:  # More than 5% failure
    print(f"ALERT: High failure rate: {failure_rate:.2%}")
```

**Quality Dashboard Query:**
```sql
-- Quality metrics over time
WITH validation_stats AS (
  SELECT 
    date_trunc('hour', _kelp_quarantine_timestamp) as hour,
    _kelp_validation_rule as rule,
    COUNT(*) as failures
  FROM catalog.schema.my_table_quarantine
  WHERE _kelp_quarantine_timestamp >= current_timestamp() - INTERVAL 7 DAYS
  GROUP BY hour, rule
)
SELECT * FROM validation_stats
ORDER BY hour DESC, failures DESC;
```

## Quality Check Templates

### Primary Key Validation
```yaml
quality:
  engine: sdp
  expect_all_or_fail:
    pk_not_null: ${pk_column} IS NOT NULL
    pk_unique: ${pk_column} IS NOT NULL  # Note: SDP doesn't enforce uniqueness directly
```

### Foreign Key Validation (DQX)
```yaml
quality:
  engine: dqx
  checks:
    - name: valid_foreign_key
      type: referential_integrity
      column: customer_id
      reference_table: dim_customers
      reference_column: customer_id
      action: quarantine
```

### Email Validation
```yaml
quality:
  engine: sdp
  expect_all_or_quarantine:
    email_format: email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'
    email_not_null: email IS NOT NULL
```

### Date Range Validation
```yaml
quality:
  engine: sdp
  expect_all_or_fail:
    date_not_null: event_date IS NOT NULL
  expect_all_or_quarantine:
    date_not_future: event_date <= current_date()
    date_not_too_old: event_date >= current_date() - INTERVAL 365 DAYS
```

### Numeric Range Validation
```yaml
quality:
  engine: sdp
  expect_all_or_quarantine:
    amount_positive: amount > 0
    amount_reasonable: amount <= 1000000
    quantity_valid: quantity BETWEEN 0 AND 1000
```

### Enum/Categorical Validation
```yaml
quality:
  engine: sdp
  expect_all_or_fail:
    status_valid: status IN ('active', 'inactive', 'pending', 'cancelled')
    country_code_valid: country_code IN (SELECT code FROM ref.iso_countries)
```

### Completeness Check (Required Fields)
```yaml
quality:
  engine: sdp
  expect_all_or_fail:
    required_fields: |
      user_id IS NOT NULL AND
      email IS NOT NULL AND
      created_at IS NOT NULL
```

## Troubleshooting

### Issue: Quality checks not applying

**Diagnosis:**
```bash
# Check YAML syntax
uv run kelp validate

# Verify quality section parsed
uv run python -c "
from kelp import init
ctx = init()
table = ctx.get_table('my_table')
print(table.quality)
"
```

**Solutions:**
- Ensure `quality:` section exists in table YAML
- Verify `engine: sdp` or `engine: dqx` is set
- Check decorator: `@kp.table()` not `@dp.table`
- Validate YAML indentation (use spaces, not tabs)

### Issue: Quarantine tables not created

**Diagnosis:**
- Check for `expect_all_or_quarantine` in YAML
- Verify pipeline ran successfully
- Look for validation/quarantine tables in catalog

**Solutions:**
```python
# Explicitly create quarantine target
from pyspark import pipelines as dp
from kelp import pipelines as kp

@dp.append_flow(target=kp.target("my_table"))
def my_table_flow():
    return spark.readStream.table(kp.ref("source"))
```

### Issue: Quality check too strict (pipeline always fails)

**Solutions:**
1. **Separate fail vs quarantine:**
```yaml
quality:
  engine: sdp
  expect_all_or_fail:
    critical_only: user_id IS NOT NULL
  expect_all_or_quarantine:
    nice_to_have: email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'
```

2. **Use validation layer pattern:**
```yaml
# bronze_raw.yml - no checks
# bronze_validated.yml - with checks
# silver.yml - reads from bronze_validated
```

3. **Temporarily disable for debugging:**
```python
@kp.table(exclude_params=["expectations"])
def my_table_debug():
    return spark.readStream.table(kp.ref("source"))
```

### Issue: DQX checks not running

**Diagnosis:**
```bash
# Check DQX installed
uv run python -c "import databricks.labs.dqx; print(dqx.__version__)"
```

**Solutions:**
- Install DQX: `uv add databricks-labs-dqx`
- Verify `engine: dqx` in YAML
- Check DQX documentation for check syntax

## Best Practices

1. **Start Simple**: Begin with SDP expectations, migrate to DQX only if needed
2. **Fail Fast on Critical**: Use `expect_all_or_fail` for blocking issues (null PKs)
3. **Quarantine for Business Rules**: Use `expect_all_or_quarantine` for validation that shouldn't block
4. **Layer Validation**: Create `_validated` tables between layers for complex checks
5. **Monitor Quarantine**: Set up alerts for quarantine table growth
6. **Document Rules**: Add clear descriptions in YAML for each quality check
7. **Test Incrementally**: Add one check at a time, test, then add more

## Example: Comprehensive Validation

**YAML:**
```yaml
kelp_models:
  - name: silver_orders_validated
    description: "Validated order data with comprehensive quality checks"
    columns:
      - name: order_id
        data_type: string
        description: "Unique order identifier"
      - name: customer_id
        data_type: string
        description: "Customer reference"
      - name: order_amount
        data_type: decimal(10,2)
        description: "Total order amount in USD"
      - name: order_date
        data_type: date
        description: "Order placement date"
      - name: status
        data_type: string
        description: "Order status"
    quality:
      engine: sdp
      expect_all_or_fail:
        # Critical: must pass
        primary_key_not_null: order_id IS NOT NULL
        required_fields: |
          order_id IS NOT NULL AND
          customer_id IS NOT NULL AND
          order_amount IS NOT NULL AND
          order_date IS NOT NULL
      expect_all_or_quarantine:
        # Important: isolate bad records
        amount_positive: order_amount > 0
        amount_reasonable: order_amount <= 100000
        date_not_future: order_date <= current_date()
        date_recent: order_date >= current_date() - INTERVAL 730 DAYS
        status_valid: status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')
      expect_all:
        # Warning only: monitor but don't block
        preferred_status: status IN ('confirmed', 'shipped', 'delivered')
```

**Python:**
```python
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from kelp import pipelines as kp

spark = SparkSession.active()

@kp.table()
def silver_orders_validated():
    """
    Validated orders with comprehensive quality checks.
    Failed records are quarantined for review.
    """
    return spark.readStream.table(kp.ref("bronze_orders"))
```

## Related Documentation

- **DQX Docs**: [https://databrickslabs.github.io/dqx/](https://databrickslabs.github.io/dqx/)
- **SDP Expectations**: [https://docs.databricks.com/aws/en/ldp/developer/python-ref](https://docs.databricks.com/aws/en/ldp/developer/python-ref)
- **Kelp Quality Guide**: docs/guides/sdp.md

## Related Skills

- `kelp-etl-builder` - Core ETL pipeline creation
- `kelp-abac-builder` - Access control and data masking
- `kelp-migration` - Convert legacy validation logic
