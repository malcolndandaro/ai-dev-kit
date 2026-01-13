-- Silver Customers: Cleansed and validated customer data
-- Applies data quality constraints and standardization

CREATE OR REFRESH MATERIALIZED VIEW silver_customers (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email EXPECT (email IS NOT NULL AND email LIKE '%@%') ON VIOLATION DROP ROW,
  CONSTRAINT valid_age EXPECT (age >= 18 AND age <= 120) ON VIOLATION DROP ROW,
  CONSTRAINT valid_segment EXPECT (segment IN ('Individual', 'Small Business', 'Enterprise')) ON VIOLATION DROP ROW
)
CLUSTER BY (segment, region)
AS
SELECT
  customer_id,
  INITCAP(first_name) AS first_name,
  INITCAP(last_name) AS last_name,
  CONCAT(INITCAP(first_name), ' ', INITCAP(last_name)) AS full_name,
  LOWER(email) AS email,
  phone,
  age,
  CASE
    WHEN age < 25 THEN '18-24'
    WHEN age < 35 THEN '25-34'
    WHEN age < 45 THEN '35-44'
    WHEN age < 55 THEN '45-54'
    WHEN age < 65 THEN '55-64'
    ELSE '65+'
  END AS age_group,
  segment,
  region,
  status,
  lifetime_value,
  created_at,
  _ingested_at,
  current_timestamp() AS _processed_at
FROM bronze_customers;
