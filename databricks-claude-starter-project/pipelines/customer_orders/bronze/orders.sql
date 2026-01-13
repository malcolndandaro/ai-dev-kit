-- Bronze Orders: Raw order data ingestion
-- Source: In-memory generated data (for demo, we'll create from values)

CREATE OR REFRESH MATERIALIZED VIEW bronze_orders
CLUSTER BY (order_date, region)
AS
SELECT
  order_id,
  customer_id,
  CAST(order_date AS DATE) AS order_date,
  status,
  num_items,
  CAST(subtotal AS DECIMAL(10,2)) AS subtotal,
  CAST(discount AS DECIMAL(10,2)) AS discount,
  CAST(shipping AS DECIMAL(10,2)) AS shipping,
  CAST(total AS DECIMAL(10,2)) AS total,
  payment_method,
  region,
  current_timestamp() AS _ingested_at
FROM (
  VALUES
    ('ORD-000001', 'CUST-00001', '2025-01-23', 'completed', 2, 163.19, 29.79, 8.80, 142.20, 'credit_card', 'North America'),
    ('ORD-000002', 'CUST-00006', '2026-01-03', 'completed', 20, 991.12, 171.97, 113.23, 932.38, 'paypal', 'North America'),
    ('ORD-000003', 'CUST-00003', '2025-11-12', 'completed', 2, 174.87, 0.00, 19.33, 194.20, 'credit_card', 'Asia Pacific'),
    ('ORD-000004', 'CUST-00007', '2025-12-12', 'shipped', 1, 54.73, 0.00, 6.69, 61.42, 'debit_card', 'Europe'),
    ('ORD-000005', 'CUST-00004', '2025-04-24', 'completed', 11, 413.13, 0.00, 54.36, 467.49, 'credit_card', 'North America'),
    ('ORD-000006', 'CUST-00002', '2025-08-15', 'completed', 3, 89.50, 10.00, 12.50, 92.00, 'debit_card', 'North America'),
    ('ORD-000007', 'CUST-00010', '2025-09-20', 'completed', 50, 2500.00, 250.00, 150.00, 2400.00, 'bank_transfer', 'North America'),
    ('ORD-000008', 'CUST-00008', '2025-03-10', 'cancelled', 5, 320.00, 0.00, 0.00, 320.00, 'credit_card', 'Europe'),
    ('ORD-000009', 'CUST-00009', '2025-07-05', 'refunded', 2, 75.00, 0.00, 8.00, 83.00, 'paypal', 'Latin America'),
    ('ORD-000010', 'CUST-00005', '2025-10-18', 'processing', 4, 210.00, 21.00, 25.00, 214.00, 'credit_card', 'Asia Pacific'),
    ('ORD-000011', 'CUST-00006', '2025-06-01', 'completed', 15, 1800.00, 180.00, 100.00, 1720.00, 'credit_card', 'North America'),
    ('ORD-000012', 'CUST-00001', '2025-05-15', 'completed', 8, 450.00, 45.00, 35.00, 440.00, 'debit_card', 'North America'),
    ('ORD-000013', 'CUST-00010', '2025-11-25', 'shipped', 30, 3200.00, 0.00, 200.00, 3400.00, 'bank_transfer', 'North America'),
    ('ORD-000014', 'CUST-00007', '2025-02-14', 'completed', 1, 45.00, 5.00, 5.00, 45.00, 'credit_card', 'Europe'),
    ('ORD-000015', 'CUST-00004', '2025-12-01', 'pending', 6, 550.00, 0.00, 40.00, 590.00, 'paypal', 'North America')
) AS t(order_id, customer_id, order_date, status, num_items, subtotal, discount, shipping, total, payment_method, region);
