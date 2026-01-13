-- Gold Customer Metrics: Customer-level aggregations
-- RFM-style metrics and customer lifetime analytics

CREATE OR REFRESH MATERIALIZED VIEW gold_customer_metrics
CLUSTER BY (customer_segment, region)
AS
SELECT
  o.customer_id,
  o.customer_name,
  o.customer_segment,
  o.customer_age_group,
  o.region,

  -- Order metrics
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(o.total) AS total_spend,
  ROUND(AVG(o.total), 2) AS avg_order_value,
  SUM(o.num_items) AS total_items_purchased,

  -- Recency
  MIN(o.order_date) AS first_order_date,
  MAX(o.order_date) AS last_order_date,
  DATEDIFF(MAX(o.order_date), MIN(o.order_date)) AS customer_tenure_days,

  -- Order status breakdown
  SUM(CASE WHEN o.status = 'completed' THEN 1 ELSE 0 END) AS completed_orders,
  SUM(CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
  SUM(CASE WHEN o.status = 'refunded' THEN 1 ELSE 0 END) AS refunded_orders,

  -- Payment preferences
  MODE(o.payment_method) AS preferred_payment_method,

  -- Discounts
  SUM(o.discount) AS total_discounts_received,
  ROUND(AVG(o.discount_pct), 2) AS avg_discount_pct,

  -- Customer value tier
  CASE
    WHEN SUM(o.total) >= 5000 THEN 'High Value'
    WHEN SUM(o.total) >= 1000 THEN 'Medium Value'
    ELSE 'Low Value'
  END AS value_tier

FROM silver_orders o
GROUP BY
  o.customer_id,
  o.customer_name,
  o.customer_segment,
  o.customer_age_group,
  o.region;
