# Gold Orders Metrics (Databricks SDP materialized view)

This document describes the metrics and how they are derived from the
`gold_orders_customers` Databricks SDP materialized view.

Materialization
- The gold transformation `gold_orders_customers` is implemented as a
  Databricks SDP materialized view (configured via `dp.materialized_view`).
- The materialized view should point to the Delta table backing
  `gold_orders_customers` so SDP can manage refreshes and incremental
  computation.

Core row-level columns used
- `order_id`, `order_state`, `product`, `quantity`, `price`, `total_price`,
  `store`, `user_id`, `customer_name`

Metrics definitions
- total_revenue: SUM(total_price)
  - SQL: SELECT SUM(total_price) AS total_revenue FROM gold_orders_customers

- order_count: COUNT(DISTINCT order_id)
  - SQL: SELECT COUNT(DISTINCT order_id) AS order_count FROM gold_orders_customers

- avg_order_value: AVG(total_price)
  - SQL: SELECT AVG(total_price) AS avg_order_value FROM gold_orders_customers

- orders_by_state: counts grouped by `order_state`
  - SQL: SELECT order_state, COUNT(*) AS orders FROM gold_orders_customers GROUP BY order_state

Quality and expectations
- Ensure `price >= 0` and `quantity >= 0` at materialization time. SDP quality
  checks can be applied per the `gold_orders_customers` model metadata.

Notes
- For time-series metrics (e.g., revenue by day/week), augment the gold view
  with an order timestamp column and compute windowed aggregates in the
  metric layer or as separate materialized aggregates.
