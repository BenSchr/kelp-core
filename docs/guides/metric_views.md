# Metric Views

This guide explains how to define and organize metric views in your Kelp project. Metric views in Databricks provide a consistent way to define business metrics and KPIs that can be used across analytics, dashboards, and reporting tools.

## What Are Metric Views?

Metric views in Databricks are semantic layer objects that define:

- **Dimensions** - Grouping attributes (e.g., customer, region, time period)
- **Metrics** - Aggregated measures (e.g., revenue, customer count, conversion rate)
- **Source tables** - Underlying tables containing measure and dimension data

They provide a single source of truth for business metrics across your organization, enabling consistent reporting and analytics.

## Configure Paths and Defaults

Add dedicated paths for metric views to `kelp_project.yml`:

```yaml
kelp_project:
  metrics_path: "./kelp_metadata/metrics"
  metric_views:
    +catalog: ${ catalog }
    +schema: ${ metric_schema }
    +tags:
      kelp_managed: ""
      layer: analytics

vars:
  catalog: analytics_prod
  metric_schema: metrics
```

The `+` prefix applies defaults to all metric views. You can create nested hierarchies for different metric domains:

```yaml
kelp_project:
  metrics_path: "./kelp_metadata/metrics"
  metric_views:
    +catalog: ${ catalog }
    +schema: ${ metric_schema }
    customer:
      +tags:
        domain: customer
    product:
      +tags:
        domain: product
    finance:
      +tags:
        domain: finance
```

## Define Metric Views

Metric views are defined using Databricks' metric view specification format.

### Basic Structure

```yaml
kelp_metric_views:
  - name: customer_revenue_metrics
    catalog: ${ catalog }
    schema: ${ metric_schema }
    description: Customer-level revenue metrics by period
    
    definition:
      measures:
        - name: total_revenue
          description: Total revenue generated
          expr: SUM(amount)
        - name: transaction_count
          description: Number of transactions
          expr: COUNT(*)
        - name: avg_transaction_value
          description: Average transaction value
          expr: AVG(amount)
      
      dimensions:
        - name: customer_id
          expr: customer_id
        - name: order_date
          expr: order_date
        - name: region
          expr: region
      
      source_table: ${ catalog }.gold.customer_orders
      
    tags:
      domain: customer
      sla: high
```

**Key Components:**

- `name` - Metric view identifier
- `catalog` - Unity Catalog name
- `schema` - Schema name
- `description` - Documentation
- `definition` - Metric view specification
  - `measures` - Aggregated metrics (with name, description, SQL expression)
  - `dimensions` - Grouping attributes (with name and SQL expression)
  - `source_table` - Underlying table (fully qualified)
- `tags` - Metadata tags

### Revenue Metrics Example

```yaml
kelp_metric_views:
  - name: monthly_revenue
    catalog: analytics
    schema: metrics
    description: Monthly revenue across all products
    
    definition:
      measures:
        - name: gross_revenue
          description: Total revenue before discounts
          expr: SUM(gross_amount)
        - name: net_revenue
          description: Revenue after discounts and returns
          expr: SUM(net_amount)
        - name: discount_amount
          description: Total discounts given
          expr: SUM(gross_amount - net_amount)
        - name: order_count
          description: Number of orders
          expr: COUNT(DISTINCT order_id)
      
      dimensions:
        - name: year_month
          expr: TO_DATE(order_date, 'YYYY-MM')
        - name: product_category
          expr: product_category
        - name: region
          expr: customer_region
      
      source_table: analytics.gold.orders_with_customers
    
    tags:
      domain: financial
      frequency: monthly
```

### Customer Cohort Metrics

```yaml
kelp_metric_views:
  - name: customer_cohort_metrics
    catalog: ${ catalog }
    schema: ${ metric_schema }
    description: Customer metrics grouped by acquisition cohort
    
    definition:
      measures:
        - name: customer_count
          expr: COUNT(DISTINCT customer_id)
        - name: total_ltv
          description: Total lifetime value
          expr: SUM(lifetime_value)
        - name: avg_ltv
          description: Average lifetime value per customer
          expr: AVG(lifetime_value)
        - name: churn_rate
          description: Percentage of churned customers
          expr: SUM(CASE WHEN churned THEN 1 ELSE 0 END) / COUNT(*) * 100
      
      dimensions:
        - name: acquisition_cohort
          description: Customer acquisition month-year
          expr: DATE_TRUNC('MONTH', acquisition_date)
        - name: country
          expr: country
        - name: plan_type
          expr: subscription_plan
      
      source_table: ${ catalog }.gold.customer_cohorts
    
    tags:
      domain: customer
      sla: critical
```

## Organizing Metric Views

Create a clear structure for managing metric views:

```
kelp_metadata/metrics/
├── customer_metrics.yml      # Customer-related metrics
├── product_metrics.yml       # Product/SKU metrics
├── financial_metrics.yml     # Revenue and profitability
├── operational_metrics.yml   # KPIs and performance metrics
└── by_domain/
    ├── customer/
    │   └── cohort_metrics.yml
    ├── product/
    │   └── sales_metrics.yml
    └── financial/
        └── bookings_metrics.yml
```

Group related metrics by business domain for maintainability.

## Using Metric Views in Analysis

### SQL Queries

Query metric views using standard SQL with aggregation:

```sql
SELECT 
  order_date,
  region,
  SUM(total_revenue) as total_revenue,
  SUM(transaction_count) as total_transactions,
  AVG(avg_transaction_value) as avg_transaction
FROM analytics_prod.metrics.customer_revenue_metrics
GROUP BY order_date, region
ORDER BY order_date DESC, total_revenue DESC;
```

### Databricks SQL

Use metric views in Databricks SQL dashboards for reporting:

```sql
SELECT 
  acquisition_cohort,
  plan_type,
  customer_count,
  total_ltv,
  churn_rate
FROM analytics_prod.metrics.customer_cohort_metrics
WHERE acquisition_cohort >= DATE_TRUNC('YEAR', CURRENT_DATE())
ORDER BY total_ltv DESC;
```

### Python/PySpark

Access metric views from PySpark code:

```python
import kelp.pipelines as kp
from pyspark.sql import SparkSession

spark = SparkSession.active()

# Read metric view as DataFrame
df = spark.read.table("analytics_prod.metrics.customer_revenue_metrics")

# Use metric view in transformations
monthly_summary = (
    spark.read.table(kp.ref("customer_revenue_metrics"))
    .groupBy("order_date", "region")
    .agg({
        "total_revenue": "sum",
        "transaction_count": "sum",
        "avg_transaction_value": "avg"
    })
    .orderBy("order_date")
)

monthly_summary.display()
```

## Syncing Metric Views to Catalog

Metric views must be synced to Unity Catalog after they're defined in your metadata.

### Sync All Metric Views

```python
import kelp.catalog as kc

kc.init("kelp_project.yml", target="prod")

for query in kc.sync_metric_views():
    print(f"Executing: {query}")
    spark.sql(query)
```

### Sync Specific Metric Views

```python
for query in kc.sync_metric_views(view_names=["customer_revenue_metrics", "monthly_revenue"]):
    spark.sql(query)
```

### Automatic Syncing with Catalog

When using `sync_catalog()`, metric views are synced after tables (which they depend on) but before ABAC policies:

```python
for query in kc.sync_catalog():
    spark.sql(query)
# Order: tables → metric views → ABAC policies
```

## Versioning and Evolution

### Adding New Measures

Add new measures without breaking existing queries:

```yaml
definition:
  measures:
    # Existing measures
    - name: total_revenue
      expr: SUM(amount)
    
    # New measure for reporting
    - name: revenue_growth_pct
      description: Period-over-period revenue growth percentage
      expr: (SUM(amount) - LAG(SUM(amount)) OVER (ORDER BY date)) / LAG(SUM(amount)) OVER (ORDER BY date) * 100
```

### Renaming Dimensions

Use aliases to support dimension renames without breaking downstream usage:

```yaml
definition:
  dimensions:
    - name: order_month           # New name
      expr: DATE_TRUNC('MONTH', order_date)
    
    - name: order_date_month     # Old name (deprecated)
      expr: DATE_TRUNC('MONTH', order_date)
```

### Deprecating Metrics

Document deprecated metrics in descriptions:

```yaml
definition:
  measures:
    - name: old_metric
      description: "DEPRECATED: Use new_metric instead"
      expr: SUM(legacy_column)
    
    - name: new_metric
      description: "Improved calculation using updated data"
      expr: SUM(correct_column)
```

## Best Practices

1. **Document clearly** - Add descriptions explaining what each metric and dimension represents.

2. **Use consistent naming** - Follow naming conventions for measures and dimensions (e.g., `snake_case`).

3. **Define at the right layer** - Base metric views on gold/aggregated tables, not raw data.

4. **Version your metrics** - Track metric definition changes in git for auditability.

5. **Test expressions** - Validate metric calculations against known values before deployment.

6. **Organize by domain** - Group related metrics by business area (customer, product, financial).

7. **Use appropriate tags** - Tag metric views with domain, SLA, and frequency information:

```yaml
tags:
  domain: customer
  sla: critical
  frequency: daily
  owner: analytics-team
```

8. **Handle NULL values** - Consider NULL handling in measure expressions:

```yaml
measures:
  - name: safe_avg
    expr: AVG(CASE WHEN value IS NOT NULL THEN value ELSE NULL END)
```

9. **Cache expensive queries** - Use materialized metric views for complex calculations.

10. **Monitor performance** - Track metric view query performance and optimize source tables as needed.

## Common Patterns

### Funnel Metrics

Track progression through multi-stage processes:

```yaml
definition:
  measures:
    - name: visits
      expr: COUNT(DISTINCT session_id)
    - name: signups
      expr: COUNT(DISTINCT CASE WHEN event = 'signup' THEN user_id END)
    - name: first_purchase
      expr: COUNT(DISTINCT CASE WHEN event = 'first_purchase' THEN user_id END)
    - name: signup_conversion
      expr: COUNT(DISTINCT CASE WHEN event = 'signup' THEN user_id END) / COUNT(DISTINCT session_id) * 100
    - name: ftp_conversion
      expr: COUNT(DISTINCT CASE WHEN event = 'first_purchase' THEN user_id END) / COUNT(DISTINCT CASE WHEN event = 'signup' THEN user_id END) * 100
```

### Running Totals

Cumulative metrics over time:

```yaml
measures:
  - name: cumulative_revenue
    expr: SUM(revenue) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  - name: mau
    description: Monthly Active Users
    expr: COUNT(DISTINCT user_id)
```

### Year-over-Year Comparisons

```yaml
measures:
  - name: revenue
    expr: SUM(amount)
  - name: revenue_prior_year
    expr: SUM(CASE WHEN YEAR(order_date) = YEAR(CURRENT_DATE()) - 1 THEN amount ELSE 0 END)
  - name: yoy_growth
    expr: (SUM(amount) - SUM(CASE WHEN YEAR(order_date) = YEAR(CURRENT_DATE()) - 1 THEN amount ELSE 0 END)) / SUM(CASE WHEN YEAR(order_date) = YEAR(CURRENT_DATE()) - 1 THEN amount ELSE 0 END) * 100
```

## See Also

- [Project Configuration](project_config.md) - Configuring metric paths and hierarchies
- [Sync Metadata with Your Catalog](catalog.md) - Syncing metric views to Unity Catalog
- [Spark Declarative Pipelines](sdp.md) - Using metric views in SDP
- [Databricks Metric Views Documentation](https://docs.databricks.com/en/metric-views/create/sql)
