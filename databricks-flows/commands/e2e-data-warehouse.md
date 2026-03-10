---
name: e2e-data-warehouse
description: "Build a complete end-to-end data warehouse demo on Databricks: synthetic data generation, bronze/silver/gold medallion layers, SDP pipelines, and AI/BI dashboard. Use when user mentions 'data warehouse demo', 'end-to-end warehouse', 'medallion demo', 'e2e data warehouse', or 'full lakehouse demo'."
disable-model-invocation: true
argument-hint: "[industry: retail|healthcare|financial|iot] [catalog.schema]"
---

# End-to-End Data Warehouse Flow

Build a complete data warehouse demo on Databricks in one orchestrated workflow. This flow composes multiple skills to go from zero to a fully operational medallion architecture with dashboards.

> **NEVER default catalog or schema** — always ask the user. If not provided in arguments, prompt before proceeding.

## Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `industry` | Yes | One of: `retail`, `healthcare`, `financial`, `iot` |
| `catalog.schema` | Yes | Unity Catalog target (e.g., `my_catalog.warehouse_demo`) |

If the user omits arguments, ask:
1. "Which industry vertical? (retail, healthcare, financial, iot)"
2. "Which catalog.schema should I use for this demo?"

## Orchestration Steps

Execute these steps **sequentially**. Report what was created after each step before proceeding to the next. If any step fails, stop and troubleshoot before continuing.

---

### Step 1: Discover or Generate Data

**Goal**: Ensure raw data exists for the chosen industry vertical.

1. Ask the user: "Do you have existing data, or should I generate synthetic demo data?"
2. **If existing data**: Use `get_table_details` to discover tables in the user's catalog/schema. Map them to the industry preset from the `demo-presets` skill.
3. **If generating**: Load the industry preset from `databricks-flows/skills/demo-presets/SKILL.md` for the selected vertical. Follow patterns from the `databricks-synthetic-data-gen` skill:
   - Use Spark + Faker + Pandas UDFs (strongly recommended)
   - Use serverless compute
   - Generate master tables first, then related tables with valid foreign keys
   - Write raw data as Parquet files to a UC Volume: `/Volumes/{catalog}/{schema}/raw_data/`
   - Create the schema and volume if they don't exist

**MCP Tools**: `execute_sql` (CREATE SCHEMA/VOLUME IF NOT EXISTS), `get_table_details` (discover existing data)

**Report**: List all tables generated/discovered with row counts and output location.

---

### Step 2: Bronze Layer (Raw Ingestion)

**Goal**: Create streaming tables that ingest raw data files.

1. For each raw data source, create a bronze streaming table using `read_files()` or Auto Loader patterns
2. Add ingestion metadata columns: `_ingested_at` (timestamp), `_source_file` (file path)
3. Use `CLUSTER BY` on the primary date column (not PARTITION BY)
4. Name tables with `bronze_` prefix (e.g., `bronze_customers`, `bronze_orders`)

**SQL Pattern** (preferred):
```sql
CREATE OR REFRESH STREAMING TABLE bronze_orders
CLUSTER BY (order_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/{catalog}/{schema}/raw_data/orders/',
  format => 'parquet'
);
```

**Report**: List all bronze tables created with column counts.

---

### Step 3: Silver Layer (Cleaned and Validated)

**Goal**: Clean, validate, and deduplicate bronze data.

1. Create silver streaming tables or materialized views from bronze tables
2. Apply data quality rules:
   - Filter out nulls on primary keys
   - Cast columns to correct types
   - Standardize string formats (trim, lower/upper as appropriate)
   - Add data quality expectation comments
3. Deduplicate where appropriate
4. Name tables with `silver_` prefix (e.g., `silver_customers`, `silver_orders`)

**SQL Pattern**:
```sql
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
)
CLUSTER BY (order_date)
AS
SELECT
  order_id,
  customer_id,
  ROUND(amount, 2) AS amount,
  order_date,
  status
FROM STREAM(bronze_orders);
```

**Report**: List all silver tables with quality constraints applied.

---

### Step 4: Gold Layer (Business-Ready)

**Goal**: Create aggregated, denormalized tables following a star schema pattern.

1. Create dimension tables as materialized views (`dim_` prefix):
   - Dimension tables contain descriptive attributes
   - Include surrogate keys where appropriate
2. Create fact tables as materialized views (`fact_` prefix):
   - Fact tables contain measures and foreign keys to dimensions
   - Pre-aggregate where useful for dashboard performance
3. Create summary/metric tables for KPI dashboards
4. Use the industry preset's gold layer metrics as a guide

**SQL Pattern**:
```sql
CREATE OR REFRESH MATERIALIZED VIEW dim_customers
AS
SELECT
  customer_id,
  name,
  email,
  tier,
  region,
  created_date
FROM silver_customers;

CREATE OR REFRESH MATERIALIZED VIEW fact_daily_sales
AS
SELECT
  o.order_date,
  c.region,
  c.tier,
  COUNT(DISTINCT o.order_id) AS order_count,
  SUM(o.amount) AS total_revenue,
  COUNT(DISTINCT o.customer_id) AS unique_customers
FROM silver_orders o
JOIN silver_customers c ON o.customer_id = c.customer_id
GROUP BY o.order_date, c.region, c.tier;
```

**Report**: List all gold tables (dims and facts) with descriptions.

---

### Step 5: SDP Pipeline

**Goal**: Wire bronze, silver, and gold layers into a Spark Declarative Pipeline.

Follow patterns from the `databricks-spark-declarative-pipelines` skill:

1. **Ask the user**: "Do you prefer SQL or Python for the pipeline?"
2. Create pipeline source files organized by layer:
   - `bronze/` — ingestion streaming tables
   - `silver/` — cleaning streaming tables with expectations
   - `gold/` — aggregation materialized views
3. Use `manage_pipelines` MCP tool to create a serverless SDP pipeline:
   - Set `target_catalog` and `target_schema` from user's input
   - Use serverless compute (default)
   - Set `development_mode: true` for the demo
4. Run the pipeline and wait for completion
5. Verify tables are populated using `get_table_details`

**MCP Tools**: `manage_pipelines` (create + run pipeline), `get_table_details` (verify), `execute_sql` (spot-check row counts)

**Report**: Pipeline ID, run status, and table row counts after completion.

---

### Step 6: AI/BI Dashboard

**Goal**: Create a Lakeview dashboard from gold layer tables.

Follow patterns from the `databricks-aibi-dashboards` skill **strictly**:

1. Use `get_table_details` to inspect gold table schemas
2. Design dashboard datasets using fully-qualified table names (`catalog.schema.table`)
3. **Test ALL SQL queries via `execute_sql` before building the dashboard JSON** — this is mandatory
4. Build the dashboard with:
   - Title and subtitle text widgets
   - KPI counters from summary metrics (3 across, width=2 each)
   - Time series line charts from fact tables
   - Bar charts for categorical breakdowns
   - A detail table for drill-down
5. Use `get_best_warehouse` to get a warehouse ID
6. Deploy via `create_or_update_dashboard`

**MCP Tools**: `get_table_details`, `execute_sql`, `get_best_warehouse`, `create_or_update_dashboard`

**Report**: Dashboard URL and widget summary.

---

### Step 7: Optional DABs Deployment

**Goal**: Package the entire demo as a Databricks Asset Bundle for reproducible deployment.

1. Ask the user: "Would you like to package this as a Databricks Asset Bundle for deployment?"
2. If yes, follow patterns from the `databricks-asset-bundles` skill:
   - Create `databricks.yml` with dev/prod targets
   - Create pipeline resource definitions under `resources/`
   - Place transformation files under `src/`
   - Include job definitions for scheduled refreshes
3. Validate with `databricks bundle validate`

**MCP Tools**: `manage_jobs` (create scheduled refresh job)

**Report**: Bundle structure, deployment targets, and validation status.

---

## Completion Summary

After all steps, present a summary table:

```
Demo Warehouse Summary
======================
Industry:    {industry}
Location:    {catalog}.{schema}

Resources Created:
| Layer    | Tables                        | Rows    |
|----------|-------------------------------|---------|
| Raw      | {list raw files/tables}       | {count} |
| Bronze   | {list bronze tables}          | {count} |
| Silver   | {list silver tables}          | {count} |
| Gold     | {list gold dims + facts}      | {count} |

Pipeline:    {pipeline_name} (ID: {pipeline_id})
Dashboard:   {dashboard_url}
Bundle:      {bundle_path or "Not requested"}
```

## Error Handling

- If a step fails, **stop and report the error** with the specific MCP tool output
- Suggest a fix and ask the user before retrying
- Common issues:
  - **Permission denied on catalog**: Ask user to verify catalog permissions
  - **Warehouse not available**: Use `get_best_warehouse` to find an alternative
  - **Pipeline timeout**: Increase timeout or check pipeline events with `manage_pipelines`
  - **Dashboard query fails**: Re-test with `execute_sql` and fix column references

## Related Skills

- **databricks-synthetic-data-gen** — Data generation patterns and Faker UDFs
- **databricks-spark-declarative-pipelines** — SDP pipeline creation and medallion architecture
- **databricks-aibi-dashboards** — Dashboard JSON structure and validation workflow
- **databricks-asset-bundles** — DABs for multi-environment deployment
- **databricks-unity-catalog** — Catalog/schema/volume management
