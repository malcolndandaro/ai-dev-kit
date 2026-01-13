-- Gold Segment Summary: Business segment performance
-- Executive-level KPIs by customer segment and region

CREATE OR REFRESH MATERIALIZED VIEW gold_segment_summary
CLUSTER BY (customer_segment)
AS
SELECT
  customer_segment,
  region,

  -- Customer counts
  COUNT(DISTINCT customer_id) AS customer_count,

  -- Revenue metrics
  SUM(total) AS total_revenue,
  ROUND(AVG(total), 2) AS avg_order_value,
  SUM(num_items) AS total_items,

  -- Order metrics
  COUNT(DISTINCT order_id) AS order_count,
  ROUND(COUNT(DISTINCT order_id) * 1.0 / COUNT(DISTINCT customer_id), 2) AS orders_per_customer,

  -- Status breakdown
  ROUND(SUM(CASE WHEN status_category = 'Fulfilled' THEN total ELSE 0 END), 2) AS fulfilled_revenue,
  ROUND(SUM(CASE WHEN status_category = 'Cancelled/Refunded' THEN total ELSE 0 END), 2) AS lost_revenue,

  -- Payment mix
  ROUND(SUM(CASE WHEN payment_method = 'credit_card' THEN total ELSE 0 END) * 100.0 / SUM(total), 2) AS credit_card_pct,
  ROUND(SUM(CASE WHEN payment_method = 'paypal' THEN total ELSE 0 END) * 100.0 / SUM(total), 2) AS paypal_pct,

  -- Discount analysis
  SUM(discount) AS total_discounts,
  ROUND(SUM(discount) * 100.0 / NULLIF(SUM(subtotal), 0), 2) AS discount_rate

FROM silver_orders
GROUP BY customer_segment, region;
