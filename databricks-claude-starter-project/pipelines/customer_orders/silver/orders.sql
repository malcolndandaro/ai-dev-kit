-- Silver Orders: Cleansed and enriched order data
-- Joins with customers for denormalization and applies quality constraints

CREATE OR REFRESH MATERIALIZED VIEW silver_orders (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_total EXPECT (total > 0 OR status = 'cancelled') ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (status IN ('completed', 'shipped', 'processing', 'pending', 'cancelled', 'refunded')) ON VIOLATION DROP ROW
)
CLUSTER BY (order_date, customer_segment)
AS
SELECT
  o.order_id,
  o.customer_id,
  c.full_name AS customer_name,
  c.segment AS customer_segment,
  c.age_group AS customer_age_group,
  o.order_date,
  YEAR(o.order_date) AS order_year,
  MONTH(o.order_date) AS order_month,
  QUARTER(o.order_date) AS order_quarter,
  DAYOFWEEK(o.order_date) AS order_day_of_week,
  CASE WHEN DAYOFWEEK(o.order_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
  o.status,
  CASE
    WHEN o.status IN ('completed', 'shipped') THEN 'Fulfilled'
    WHEN o.status IN ('processing', 'pending') THEN 'In Progress'
    ELSE 'Cancelled/Refunded'
  END AS status_category,
  o.num_items,
  o.subtotal,
  o.discount,
  o.shipping,
  o.total,
  ROUND(o.discount / NULLIF(o.subtotal, 0) * 100, 2) AS discount_pct,
  o.payment_method,
  o.region,
  o._ingested_at,
  current_timestamp() AS _processed_at
FROM bronze_orders o
LEFT JOIN silver_customers c ON o.customer_id = c.customer_id;
