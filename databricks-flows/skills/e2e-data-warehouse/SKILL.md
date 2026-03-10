---
name: e2e-data-warehouse
description: "Build a complete end-to-end data warehouse demo on Databricks with synthetic data, medallion architecture, pipelines, and dashboards."
context: fork
agent: general-purpose
allowed-tools:
  - Bash
  - Read
  - Write
  - Edit
  - Grep
  - Glob
  - mcp__databricks__execute_sql
  - mcp__databricks__get_table_details
  - mcp__databricks__get_best_warehouse
  - mcp__databricks__manage_pipelines
  - mcp__databricks__create_or_update_dashboard
  - mcp__databricks__manage_jobs
disable-model-invocation: true
---

# End-to-End Data Warehouse Demo — $ARGUMENTS

You are an autonomous warehouse builder agent. Build a complete data warehouse demo on Databricks following the steps below. Execute ALL steps sequentially without asking the user questions (except for catalog.schema if not provided).

## Your Target

Parse `$ARGUMENTS` as `[industry] [catalog.schema]`:
- `industry`: One of `retail`, `healthcare`, `financial`, `iot` (default: `retail`)
- `catalog.schema`: Unity Catalog target (e.g., `my_catalog.warehouse_demo`)

**If catalog.schema is missing, respond with:** "Usage: /e2e-data-warehouse <industry> <catalog.schema>" and stop. NEVER default or guess the catalog.

## Step 1: Setup Infrastructure

Run via `execute_sql`:
```sql
CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};
CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_data;
```

Report: Confirm schema and volume exist.

## Step 2: Generate Synthetic Data

**Invoke: `/databricks-synthetic-data-gen`**

Load the industry preset from the `demo-presets` skill. Tell the datagen skill:

> Generate synthetic data for a {industry} data warehouse demo in `{catalog}.{schema}`.
> Write raw data as Parquet to `/Volumes/{catalog}/{schema}/raw_data/{table_name}/`.
> Use Spark + Faker + Pandas UDFs on serverless compute.
> Generate master tables first, then child tables with valid foreign keys.
> NEVER use .cache() or .persist() on serverless.

Use these table definitions from the `demo-presets` skill for the selected industry vertical. Generate all tables with the suggested row counts.

Report: List all tables generated with row counts and file paths.

## Step 3: Create Medallion Pipeline

**Invoke: `/databricks-spark-declarative-pipelines`**

Create a Spark Declarative Pipeline with:

**Bronze** (`bronze_` prefix): Streaming tables using `read_files()` from `/Volumes/{catalog}/{schema}/raw_data/{table}/`. Add `_ingested_at` and `_source_file` metadata columns. Use `CLUSTER BY` on date column.

**Silver** (`silver_` prefix): Streaming tables with data quality expectations:
```sql
CONSTRAINT valid_pk EXPECT ({pk_col} IS NOT NULL) ON VIOLATION DROP ROW
```
Cast types, trim strings, deduplicate.

**Gold** (`dim_` and `fact_` prefixes): Materialized views following star schema. Use the gold layer metrics from the industry preset.

Use SQL. Serverless compute. Development mode. Create via `manage_pipelines` MCP tool.

Report: Pipeline ID, run status, row counts for all layers.

## Step 4: Create AI/BI Dashboard

**Invoke: `/databricks-aibi-dashboards`**

Build a Lakeview dashboard from gold tables. Follow the MANDATORY validation workflow:

1. Get gold table schemas via `get_table_details`
2. Write SQL queries for each dataset
3. **TEST EVERY QUERY via `execute_sql`** — do NOT skip this
4. Build dashboard JSON with:
   - KPI counters (from dashboard KPIs in the industry preset)
   - Time series line chart
   - Breakdown bar charts
   - Detail table
5. Get warehouse via `get_best_warehouse`
6. Deploy via `create_or_update_dashboard`

Report: Dashboard URL and widget summary.

## Step 5: Summary

After all steps complete, output:

```
════════════════════════════════════════════════════════════
  DATA WAREHOUSE DEMO — {industry}
  {catalog}.{schema}
════════════════════════════════════════════════════════════

RESOURCES CREATED
─────────────────
| Layer    | Tables                   | Rows    |
|----------|--------------------------|---------|
| Raw      | {list}                   | {count} |
| Bronze   | {list}                   | {count} |
| Silver   | {list}                   | {count} |
| Gold     | {list dims + facts}      | {count} |

Pipeline:  {pipeline_name} (ID: {pipeline_id})
Dashboard: {dashboard_url}
════════════════════════════════════════════════════════════
```

## Error Handling

- If a step fails, **stop and report the error** with the MCP tool output
- Do NOT proceed past a failed step
- Common fixes:
  - Permission denied → user must grant catalog access
  - No warehouse → use `get_best_warehouse`
  - Pipeline timeout → check events via `manage_pipelines`
  - Dashboard query fails → re-test with `execute_sql`, fix column names

## Skills Referenced

| Step | Skill |
|------|-------|
| 2 | `/databricks-synthetic-data-gen` |
| 3 | `/databricks-spark-declarative-pipelines` |
| 4 | `/databricks-aibi-dashboards` |
