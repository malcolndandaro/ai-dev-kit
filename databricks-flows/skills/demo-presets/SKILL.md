---
name: demo-presets
description: "Industry-specific data warehouse presets defining table schemas, relationships, row counts, and gold layer metrics. Supports retail, healthcare, financial, and IoT verticals. Use when building end-to-end data warehouse demos with the e2e-data-warehouse flow."
---

# Demo Presets — Industry Data Warehouse Templates

Pre-defined table schemas, relationships, and metrics for common industry verticals. Each preset provides everything needed to generate synthetic data, build a medallion architecture, and create dashboards.

> **NEVER default catalog or schema** — always ask the user.

## Quick Reference

| Preset | Tables | Key Entity | Primary Metric |
|--------|--------|------------|----------------|
| **Retail** | 7 tables | Orders | Revenue |
| **Healthcare** | 7 tables | Encounters | Patient Volume |
| **Financial** | 6 tables | Transactions | Transaction Volume |
| **IoT** | 6 tables | Readings | Sensor Throughput |

---

## Retail Preset

An e-commerce / retail scenario with customers, products, orders, and inventory.

### Tables

| Table | Column | Type | Description |
|-------|--------|------|-------------|
| **customers** | customer_id | STRING | PK. Format: `CUST-XXXXX` |
| | name | STRING | Full name (Faker) |
| | email | STRING | Email address (Faker) |
| | tier | STRING | `Free` (60%), `Pro` (30%), `Enterprise` (10%) |
| | region | STRING | `North`, `South`, `East`, `West` (25% each) |
| | created_date | DATE | Account creation date (last 2 years) |
| **products** | product_id | STRING | PK. Format: `PROD-XXXXX` |
| | name | STRING | Product name (Faker) |
| | category_id | STRING | FK to categories.category_id |
| | price | DECIMAL(10,2) | Unit price, log-normal distribution |
| | cost | DECIMAL(10,2) | Unit cost (~40-70% of price) |
| **categories** | category_id | STRING | PK. Format: `CAT-XXX` |
| | name | STRING | Category name (Electronics, Clothing, Home, Sports, Books) |
| | department | STRING | Department grouping |
| **orders** | order_id | STRING | PK. Format: `ORD-XXXXXXXX` |
| | customer_id | STRING | FK to customers.customer_id |
| | store_id | STRING | FK to stores.store_id |
| | order_date | DATE | Order date (last 6 months) |
| | status | STRING | `delivered` (65%), `shipped` (15%), `processing` (10%), `pending` (5%), `cancelled` (5%) |
| | total_amount | DECIMAL(10,2) | Sum of order_items |
| **order_items** | item_id | STRING | PK. Format: `ITEM-XXXXXXXX` |
| | order_id | STRING | FK to orders.order_id |
| | product_id | STRING | FK to products.product_id |
| | quantity | INT | 1-10, weighted toward 1-3 |
| | unit_price | DECIMAL(10,2) | Price at time of sale |
| | line_total | DECIMAL(10,2) | quantity * unit_price |
| **stores** | store_id | STRING | PK. Format: `STORE-XXX` |
| | name | STRING | Store name |
| | region | STRING | `North`, `South`, `East`, `West` |
| | store_type | STRING | `physical` (70%), `online` (30%) |
| | opened_date | DATE | Store opening date |
| **inventory** | inventory_id | STRING | PK. Format: `INV-XXXXXXXX` |
| | store_id | STRING | FK to stores.store_id |
| | product_id | STRING | FK to products.product_id |
| | quantity_on_hand | INT | Current stock level (0-500) |
| | reorder_point | INT | Min stock before reorder (10-50) |
| | last_restocked | DATE | Last restock date |

### Relationships

```
categories ──< products ──< order_items >── orders >── customers
                   │                          │
                   └──< inventory >── stores ──┘
```

### Suggested Row Counts

| Table | Rows | Notes |
|-------|------|-------|
| customers | 10,000 | Enterprise customers generate 5x more orders |
| products | 500 | Across 5 categories |
| categories | 5 | Electronics, Clothing, Home, Sports, Books |
| orders | 50,000 | ~5 orders/customer avg, last 6 months |
| order_items | 120,000 | ~2.4 items/order avg |
| stores | 25 | Mix of physical and online |
| inventory | 12,500 | 25 stores x 500 products |

### Gold Layer Metrics

| Metric Table | Description | Key Measures |
|-------------|-------------|-------------|
| `fact_daily_sales` | Daily sales aggregated by region and tier | order_count, total_revenue, unique_customers, avg_order_value |
| `fact_product_performance` | Product-level performance metrics | units_sold, total_revenue, avg_unit_price, return_rate |
| `dim_customers` | Customer dimension with tier and region | customer_id, name, tier, region, created_date |
| `dim_products` | Product dimension with category info | product_id, name, category, price, cost, margin_pct |
| `dim_stores` | Store dimension | store_id, name, region, store_type |
| `summary_kpis` | Single-row KPI summary | total_revenue, total_orders, unique_customers, avg_order_value |

### Dashboard KPIs

- **Total Revenue** — SUM(total_amount) from fact_daily_sales
- **Total Orders** — COUNT(order_id) from fact_daily_sales
- **Unique Customers** — COUNT(DISTINCT customer_id)
- **Avg Order Value** — total_revenue / total_orders
- **Revenue by Region** — Bar chart from fact_daily_sales grouped by region
- **Revenue Trend** — Line chart of daily total_revenue over time
- **Top Products** — Bar chart from fact_product_performance (top 10)
- **Orders by Status** — Pie chart from orders grouped by status

---

## Healthcare Preset

A hospital / healthcare scenario with patients, encounters, diagnoses, and providers.

### Tables

| Table | Column | Type | Description |
|-------|--------|------|-------------|
| **patients** | patient_id | STRING | PK. Format: `PAT-XXXXXXXX` |
| | name | STRING | Full name (Faker) |
| | date_of_birth | DATE | DOB (age 0-95, weighted toward 30-70) |
| | gender | STRING | `M` (48%), `F` (50%), `Other` (2%) |
| | blood_type | STRING | Realistic distribution (O+ 37%, A+ 36%, B+ 8%, etc.) |
| | insurance_type | STRING | `Private` (55%), `Medicare` (25%), `Medicaid` (15%), `Self-pay` (5%) |
| | zip_code | STRING | 5-digit ZIP (Faker) |
| **providers** | provider_id | STRING | PK. Format: `PROV-XXXXX` |
| | name | STRING | Provider name (Faker) |
| | specialty | STRING | Cardiology, Orthopedics, General, Pediatrics, Neurology, etc. |
| | department_id | STRING | FK to departments.department_id |
| | npi_number | STRING | 10-digit NPI (Faker) |
| **departments** | department_id | STRING | PK. Format: `DEPT-XXX` |
| | name | STRING | Emergency, Cardiology, Orthopedics, General Medicine, Pediatrics, Neurology |
| | floor | INT | Hospital floor (1-5) |
| | bed_count | INT | Beds available (10-50) |
| **encounters** | encounter_id | STRING | PK. Format: `ENC-XXXXXXXX` |
| | patient_id | STRING | FK to patients.patient_id |
| | provider_id | STRING | FK to providers.provider_id |
| | department_id | STRING | FK to departments.department_id |
| | encounter_date | DATE | Visit date (last 6 months) |
| | encounter_type | STRING | `outpatient` (60%), `inpatient` (25%), `emergency` (15%) |
| | discharge_date | DATE | Null for outpatient; 1-14 days after encounter_date for inpatient |
| | total_charges | DECIMAL(10,2) | Log-normal, varies by type |
| **diagnoses** | diagnosis_id | STRING | PK. Format: `DX-XXXXXXXX` |
| | encounter_id | STRING | FK to encounters.encounter_id |
| | icd10_code | STRING | ICD-10 code (sampled from common codes) |
| | description | STRING | Diagnosis description |
| | is_primary | BOOLEAN | True for first diagnosis per encounter |
| **procedures** | procedure_id | STRING | PK. Format: `PX-XXXXXXXX` |
| | encounter_id | STRING | FK to encounters.encounter_id |
| | cpt_code | STRING | CPT code (sampled from common codes) |
| | description | STRING | Procedure description |
| | charge_amount | DECIMAL(10,2) | Procedure cost, log-normal |
| **medications** | medication_id | STRING | PK. Format: `MED-XXXXXXXX` |
| | encounter_id | STRING | FK to encounters.encounter_id |
| | drug_name | STRING | Common drug names (sampled list) |
| | dosage | STRING | e.g., "500mg", "10mg" |
| | frequency | STRING | `daily`, `twice daily`, `as needed`, `weekly` |
| | prescribed_date | DATE | Same as encounter_date |

### Relationships

```
departments ──< providers ──< encounters >── patients
                                  │
                    ┌─────────────┼─────────────┐
                    │             │             │
                diagnoses    procedures    medications
```

### Suggested Row Counts

| Table | Rows | Notes |
|-------|------|-------|
| patients | 15,000 | Mix of ages and insurance types |
| providers | 200 | Across 6 departments |
| departments | 6 | Emergency, Cardiology, Orthopedics, General Medicine, Pediatrics, Neurology |
| encounters | 60,000 | ~4 encounters/patient avg |
| diagnoses | 80,000 | ~1.3 diagnoses/encounter avg |
| procedures | 45,000 | ~0.75 procedures/encounter avg |
| medications | 70,000 | ~1.2 medications/encounter avg |

### Gold Layer Metrics

| Metric Table | Description | Key Measures |
|-------------|-------------|-------------|
| `fact_daily_encounters` | Daily encounter volume by department and type | encounter_count, total_charges, avg_length_of_stay, unique_patients |
| `fact_diagnosis_summary` | Diagnosis frequency and costs | diagnosis_count, avg_charges, top_icd10_codes |
| `dim_patients` | Patient dimension | patient_id, name, age_group, gender, insurance_type |
| `dim_providers` | Provider dimension with specialty | provider_id, name, specialty, department |
| `dim_departments` | Department dimension | department_id, name, floor, bed_count |
| `summary_kpis` | Single-row KPI summary | total_encounters, total_charges, unique_patients, avg_charges_per_encounter |

### Dashboard KPIs

- **Total Encounters** — COUNT from fact_daily_encounters
- **Total Charges** — SUM(total_charges)
- **Unique Patients** — COUNT(DISTINCT patient_id)
- **Avg Charges/Encounter** — total_charges / total_encounters
- **Encounters by Department** — Bar chart grouped by department
- **Encounter Trend** — Line chart of daily encounter volume
- **Top Diagnoses** — Bar chart of top 10 ICD-10 codes
- **Encounter Type Mix** — Pie chart (outpatient/inpatient/emergency)

---

## Financial Preset

A banking / fintech scenario with accounts, transactions, and fraud detection.

### Tables

| Table | Column | Type | Description |
|-------|--------|------|-------------|
| **customers** | customer_id | STRING | PK. Format: `CUST-XXXXXXXX` |
| | name | STRING | Full name (Faker) |
| | email | STRING | Email (Faker) |
| | customer_since | DATE | Account opening date (last 5 years) |
| | risk_score | INT | 1-100, normal distribution centered at 30 |
| | segment | STRING | `retail` (70%), `premium` (20%), `private` (10%) |
| **accounts** | account_id | STRING | PK. Format: `ACCT-XXXXXXXX` |
| | customer_id | STRING | FK to customers.customer_id |
| | account_type | STRING | `checking` (50%), `savings` (30%), `credit` (20%) |
| | balance | DECIMAL(12,2) | Current balance, log-normal by segment |
| | currency | STRING | `USD` (85%), `EUR` (10%), `GBP` (5%) |
| | opened_date | DATE | Account opening date |
| | status | STRING | `active` (90%), `dormant` (8%), `closed` (2%) |
| **transactions** | transaction_id | STRING | PK. Format: `TXN-XXXXXXXX` |
| | account_id | STRING | FK to accounts.account_id |
| | merchant_id | STRING | FK to merchants.merchant_id |
| | transaction_date | TIMESTAMP | Transaction timestamp (last 6 months) |
| | amount | DECIMAL(12,2) | Transaction amount, log-normal |
| | transaction_type | STRING | `purchase` (60%), `transfer` (20%), `withdrawal` (10%), `deposit` (10%) |
| | channel | STRING | `online` (45%), `mobile` (30%), `in_store` (20%), `atm` (5%) |
| **merchants** | merchant_id | STRING | PK. Format: `MERCH-XXXXX` |
| | name | STRING | Merchant name (Faker company) |
| | category_id | STRING | FK to categories.category_id |
| | city | STRING | City (Faker) |
| | state | STRING | US state code |
| **categories** | category_id | STRING | PK. Format: `MCAT-XXX` |
| | name | STRING | Groceries, Dining, Travel, Shopping, Utilities, Entertainment, Healthcare |
| | risk_weight | DECIMAL(3,2) | Category risk factor (0.1 - 1.0) |
| **fraud_flags** | flag_id | STRING | PK. Format: `FLAG-XXXXXXXX` |
| | transaction_id | STRING | FK to transactions.transaction_id |
| | flag_type | STRING | `velocity`, `amount_anomaly`, `geo_anomaly`, `merchant_risk` |
| | confidence_score | DECIMAL(3,2) | 0.50 - 1.00 |
| | is_confirmed_fraud | BOOLEAN | True for ~15% of flagged transactions |
| | reviewed_date | DATE | Review date (1-7 days after transaction) |

### Relationships

```
customers ──< accounts ──< transactions >── merchants >── categories
                                │
                           fraud_flags
```

### Suggested Row Counts

| Table | Rows | Notes |
|-------|------|-------|
| customers | 8,000 | Mix of segments |
| accounts | 12,000 | ~1.5 accounts/customer avg |
| transactions | 200,000 | ~17 transactions/account avg over 6 months |
| merchants | 1,000 | Across 7 categories |
| categories | 7 | Groceries, Dining, Travel, Shopping, Utilities, Entertainment, Healthcare |
| fraud_flags | 3,000 | ~1.5% of transactions flagged |

### Gold Layer Metrics

| Metric Table | Description | Key Measures |
|-------------|-------------|-------------|
| `fact_daily_transactions` | Daily transaction volume by channel and type | txn_count, total_amount, avg_amount, unique_accounts |
| `fact_fraud_summary` | Fraud detection metrics by flag type | flags_count, confirmed_fraud_count, false_positive_rate, avg_confidence |
| `dim_customers` | Customer dimension with segment and risk | customer_id, name, segment, risk_score, customer_since |
| `dim_merchants` | Merchant dimension with category | merchant_id, name, category, city, state |
| `dim_accounts` | Account dimension | account_id, customer_id, account_type, balance, status |
| `summary_kpis` | Single-row KPI summary | total_transactions, total_volume, fraud_rate, avg_transaction_amount |

### Dashboard KPIs

- **Total Transactions** — COUNT from fact_daily_transactions
- **Total Volume** — SUM(total_amount)
- **Fraud Rate** — confirmed_fraud_count / total_transactions as percentage
- **Avg Transaction** — total_volume / total_transactions
- **Transactions by Channel** — Bar chart grouped by channel
- **Transaction Trend** — Line chart of daily volume
- **Fraud by Type** — Bar chart from fact_fraud_summary
- **Category Spend** — Pie chart of transaction amounts by merchant category

---

## IoT Preset

A sensor / device monitoring scenario with devices, readings, alerts, and maintenance.

### Tables

| Table | Column | Type | Description |
|-------|--------|------|-------------|
| **devices** | device_id | STRING | PK. Format: `DEV-XXXXXXXX` |
| | device_type_id | STRING | FK to device_types.device_type_id |
| | location_id | STRING | FK to locations.location_id |
| | firmware_version | STRING | Semantic version (e.g., "2.4.1") |
| | install_date | DATE | Installation date (last 2 years) |
| | status | STRING | `active` (85%), `maintenance` (10%), `offline` (5%) |
| **device_types** | device_type_id | STRING | PK. Format: `TYPE-XXX` |
| | name | STRING | Temperature Sensor, Pressure Sensor, Vibration Sensor, Humidity Sensor, Flow Meter |
| | unit | STRING | Measurement unit (C, PSI, mm/s, %, L/min) |
| | min_value | DECIMAL(10,2) | Normal range minimum |
| | max_value | DECIMAL(10,2) | Normal range maximum |
| | critical_threshold | DECIMAL(10,2) | Alert threshold |
| **locations** | location_id | STRING | PK. Format: `LOC-XXX` |
| | name | STRING | Location name (Plant A, Plant B, Warehouse 1, etc.) |
| | zone | STRING | `production` (40%), `storage` (30%), `utility` (20%), `office` (10%) |
| | latitude | DECIMAL(9,6) | GPS latitude |
| | longitude | DECIMAL(9,6) | GPS longitude |
| **readings** | reading_id | STRING | PK. Format: `RD-XXXXXXXX` |
| | device_id | STRING | FK to devices.device_id |
| | reading_timestamp | TIMESTAMP | Sensor reading time (last 30 days, every 5-15 min) |
| | value | DECIMAL(10,2) | Sensor reading value |
| | quality_score | DECIMAL(3,2) | Data quality (0.90-1.00 normal, < 0.80 suspect) |
| **alerts** | alert_id | STRING | PK. Format: `ALT-XXXXXXXX` |
| | device_id | STRING | FK to devices.device_id |
| | reading_id | STRING | FK to readings.reading_id |
| | alert_timestamp | TIMESTAMP | When alert was triggered |
| | severity | STRING | `critical` (10%), `warning` (30%), `info` (60%) |
| | alert_type | STRING | `threshold_breach`, `anomaly_detected`, `device_offline`, `quality_degraded` |
| | acknowledged | BOOLEAN | True for 80% of alerts |
| | resolved_timestamp | TIMESTAMP | Null if unresolved; 1-48 hours after alert |
| **maintenance_logs** | log_id | STRING | PK. Format: `ML-XXXXXXXX` |
| | device_id | STRING | FK to devices.device_id |
| | maintenance_date | DATE | When maintenance was performed |
| | maintenance_type | STRING | `preventive` (60%), `corrective` (30%), `emergency` (10%) |
| | duration_hours | DECIMAL(4,1) | Duration (0.5-24 hours) |
| | cost | DECIMAL(10,2) | Maintenance cost, log-normal |
| | technician | STRING | Technician name (Faker) |

### Relationships

```
device_types ──< devices ──< readings ──< alerts
                    │
    locations ──────┘
                    │
              maintenance_logs
```

### Suggested Row Counts

| Table | Rows | Notes |
|-------|------|-------|
| devices | 500 | Across 5 device types and 10 locations |
| device_types | 5 | Temperature, Pressure, Vibration, Humidity, Flow |
| locations | 10 | Plants, warehouses, utility areas |
| readings | 500,000 | ~1,000 readings/device over 30 days |
| alerts | 5,000 | ~1% of readings trigger alerts |
| maintenance_logs | 2,000 | ~4 maintenance events/device avg |

### Gold Layer Metrics

| Metric Table | Description | Key Measures |
|-------------|-------------|-------------|
| `fact_hourly_readings` | Hourly aggregated sensor data by device type and location | avg_value, min_value, max_value, reading_count, anomaly_count |
| `fact_alert_summary` | Alert metrics by severity and type | alert_count, avg_resolution_hours, acknowledgment_rate, unresolved_count |
| `fact_maintenance_costs` | Maintenance cost analysis | total_cost, avg_duration, maintenance_count, cost_per_device |
| `dim_devices` | Device dimension with type and location | device_id, device_type, location, firmware_version, status |
| `dim_locations` | Location dimension | location_id, name, zone, coordinates |
| `summary_kpis` | Single-row KPI summary | total_readings, active_devices, alert_rate, avg_uptime_pct |

### Dashboard KPIs

- **Active Devices** — COUNT where status = 'active'
- **Total Readings** — COUNT from fact_hourly_readings (last 24h)
- **Alert Rate** — alerts / readings as percentage
- **Avg Uptime** — percentage of time devices are active
- **Readings by Device Type** — Line chart of hourly avg values per type
- **Alert Trend** — Line chart of daily alert count by severity
- **Alerts by Type** — Bar chart from fact_alert_summary
- **Maintenance Cost by Location** — Bar chart from fact_maintenance_costs

---

## Usage

When building an e2e-data-warehouse demo:

1. Load the preset for the selected industry
2. Use table definitions for synthetic data generation (Step 1)
3. Use relationships to ensure referential integrity
4. Use suggested row counts for realistic demo scale
5. Use gold layer metrics to build fact and dimension tables
6. Use dashboard KPIs to design the AI/BI dashboard

**Example**:
```
/e2e-data-warehouse retail my_catalog.demo_warehouse
```

This loads the retail preset and builds:
- 7 raw tables with synthetic data
- 7 bronze streaming tables
- 7 silver validated tables
- 6 gold tables (3 dims + 2 facts + 1 summary)
- 1 SDP pipeline
- 1 AI/BI dashboard with 8 widgets
