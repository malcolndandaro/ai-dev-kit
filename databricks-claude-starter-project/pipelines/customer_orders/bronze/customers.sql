-- Bronze Customers: Raw customer data ingestion
-- Source: In-memory generated data (for demo, we'll create from values)

CREATE OR REFRESH MATERIALIZED VIEW bronze_customers
CLUSTER BY (region, segment)
AS
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  phone,
  age,
  segment,
  region,
  status,
  lifetime_value,
  CAST(created_at AS DATE) AS created_at,
  current_timestamp() AS _ingested_at
FROM (
  VALUES
    ('CUST-00001', 'Linda', 'Smith', 'linda.smith759@proton.me', '+1-758-189-7912', 36, 'Small Business', 'North America', 'active', 1552.62, '2022-07-29'),
    ('CUST-00002', 'John', 'Smith', 'john.smith666@proton.me', '+1-918-658-7873', 59, 'Individual', 'North America', 'active', 249.17, '2023-02-12'),
    ('CUST-00003', 'Joseph', 'Anderson', 'joseph.anderson433@proton.me', '+1-548-384-3547', 34, 'Individual', 'Asia Pacific', 'active', 123.06, '2022-11-23'),
    ('CUST-00004', 'Susan', 'Walker', 'susan.walker619@outlook.com', '+1-470-926-1711', 48, 'Small Business', 'North America', 'active', 3312.47, '2024-01-06'),
    ('CUST-00005', 'Lucas', 'Moore', 'lucas.moore634@proton.me', '+1-570-691-4150', 45, 'Individual', 'Asia Pacific', 'active', 101.94, '2023-08-24'),
    ('CUST-00006', 'Michael', 'Johnson', 'michael.johnson123@gmail.com', '+1-555-123-4567', 42, 'Enterprise', 'North America', 'active', 45000.00, '2022-03-15'),
    ('CUST-00007', 'Sarah', 'Williams', 'sarah.williams456@yahoo.com', '+1-555-234-5678', 31, 'Individual', 'Europe', 'active', 89.50, '2023-06-20'),
    ('CUST-00008', 'David', 'Brown', 'david.brown789@hotmail.com', '+1-555-345-6789', 55, 'Small Business', 'Europe', 'inactive', 2100.00, '2022-09-10'),
    ('CUST-00009', 'Emma', 'Davis', 'emma.davis321@gmail.com', '+1-555-456-7890', 28, 'Individual', 'Latin America', 'active', 175.25, '2024-02-28'),
    ('CUST-00010', 'James', 'Garcia', 'james.garcia654@outlook.com', '+1-555-567-8901', 63, 'Enterprise', 'North America', 'active', 78000.00, '2021-11-05')
) AS t(customer_id, first_name, last_name, email, phone, age, segment, region, status, lifetime_value, created_at);
