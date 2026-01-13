-- Gold Daily Sales: Daily sales aggregations
-- Business-ready metrics for dashboards and reporting

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_sales
CLUSTER BY (order_date)
AS
SELECT
  order_date,
  order_year,
  order_month,
  order_quarter,
  day_type,
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT customer_id) AS unique_customers,
  SUM(num_items) AS total_items_sold,
  SUM(subtotal) AS gross_sales,
  SUM(discount) AS total_discounts,
  SUM(shipping) AS total_shipping,
  SUM(total) AS net_sales,
  ROUND(AVG(total), 2) AS avg_order_value,
  ROUND(AVG(num_items), 2) AS avg_items_per_order,
  SUM(CASE WHEN status_category = 'Fulfilled' THEN 1 ELSE 0 END) AS fulfilled_orders,
  SUM(CASE WHEN status_category = 'Cancelled/Refunded' THEN 1 ELSE 0 END) AS cancelled_orders,
  ROUND(SUM(CASE WHEN status_category = 'Fulfilled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fulfillment_rate
FROM silver_orders
GROUP BY order_date, order_year, order_month, order_quarter, day_type;
