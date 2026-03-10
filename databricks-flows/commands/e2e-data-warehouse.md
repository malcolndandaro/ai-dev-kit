---
name: e2e-data-warehouse
description: "Build a complete end-to-end data warehouse demo on Databricks: synthetic data generation, bronze/silver/gold medallion layers, SDP pipelines, and AI/BI dashboard. Use when user mentions 'data warehouse demo', 'end-to-end warehouse', 'medallion demo', 'e2e data warehouse', or 'full lakehouse demo'."
disable-model-invocation: true
argument-hint: "[industry: retail|healthcare|financial|iot] [catalog.schema]"
---

# End-to-End Data Warehouse Flow

Build a complete data warehouse demo on Databricks in one orchestrated workflow. Each step explicitly invokes an existing skill via its slash command.

> **NEVER default catalog or schema** — always ask the user. If not provided in arguments, prompt before proceeding.

## Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `industry` | Yes | One of: `retail`, `healthcare`, `financial`, `iot` |
| `catalog.schema` | Yes | Unity Catalog target (e.g., `my_catalog.warehouse_demo`) |

If the user omits arguments, ask:
1. "Which industry vertical? (retail, healthcare, financial, iot)"
2. "Which catalog.schema should I use for this demo?"

## CRITICAL: Execution Rules

1. Execute steps **sequentially** — do NOT skip or parallelize steps
2. **Report what was created** after each step before moving to the next
3. **Stop on failure** — do not proceed past a failed step
4. At each step, **invoke the specified skill** using its `/slash-command` — the skill contains the detailed instructions and patterns

---

## Step 1: Setup Infrastructure

Before invoking any skill, create the target schema and volume:

```sql
-- Run these via execute_sql MCP tool
CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};
CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_data;
```

**Report**: Confirm schema and volume exist.

---

## Step 2: Generate Synthetic Data

**Invoke**: `/databricks-synthetic-data-gen`

Load the industry preset from the `demo-presets` skill for the selected vertical. Then tell the datagen skill:

> "Generate synthetic data for a {industry} data warehouse demo in `{catalog}.{schema}`. Use the following table definitions from the demo preset: {paste the preset tables here}. Write raw data as Parquet to `/Volumes/{catalog}/{schema}/raw_data/{table_name}/`. Use Spark + Faker + Pandas UDFs on serverless compute. Generate master tables first, then child tables with valid foreign keys."

Key parameters to pass:
- **catalog.schema**: `{catalog}.{schema}`
- **Tables**: From the industry preset in `demo-presets` skill
- **Row counts**: From the preset's suggested row counts
- **Output**: Parquet files in `/Volumes/{catalog}/{schema}/raw_data/`

**Report**: List all tables generated with row counts and file paths.

---

## Step 3: Create Bronze → Silver → Gold Pipeline

**Invoke**: `/databricks-spark-declarative-pipelines`

Tell the SDP skill:

> "Create a Spark Declarative Pipeline for a {industry} medallion architecture in `{catalog}.{schema}`. Raw data is in `/Volumes/{catalog}/{schema}/raw_data/`. Create:
> - **Bronze streaming tables** (`bronze_` prefix) using `read_files()` with `_ingested_at` and `_source_file` metadata columns, CLUSTER BY on date column
> - **Silver streaming tables** (`silver_` prefix) with data quality expectations (CONSTRAINT EXPECT for non-null PKs, valid amounts/values), deduplication, type casting
> - **Gold materialized views**: dimension tables (`dim_` prefix) and fact tables (`fact_` prefix) following star schema pattern with these metrics: {paste gold layer metrics from preset}
> Use SQL. Use serverless compute. Deploy in development mode."

The SDP skill will handle:
- Pipeline initialization via `databricks pipelines init` or manual creation
- Source file organization by layer (`bronze/`, `silver/`, `gold/`)
- Pipeline creation and execution via `manage_pipelines` MCP tool
- Data quality expectations

**Report**: Pipeline ID, run status, and table row counts for all layers.

---

## Step 4: Create AI/BI Dashboard

**Invoke**: `/databricks-aibi-dashboards`

Tell the dashboard skill:

> "Create an AI/BI dashboard for a {industry} data warehouse in `{catalog}.{schema}`. Use these gold layer tables: {list the dim_ and fact_ tables created in Step 3}. Build:
> - **Page 1 - Overview**: KPI counters ({paste dashboard KPIs from preset}), revenue/volume trend line chart, breakdown bar chart
> - **Page 2 - Details**: Categorical breakdown bar charts, detail drill-down table
> Follow the mandatory validation workflow: get table schemas → write queries → TEST ALL QUERIES via execute_sql → build dashboard JSON → deploy."

The dashboard skill will handle:
- Schema inspection via `get_table_details`
- SQL query writing and **mandatory testing** via `execute_sql`
- Dashboard JSON construction with correct widget versions
- Deployment via `create_or_update_dashboard`

**Report**: Dashboard URL and widget summary.

---

## Step 5: Optional — Package as Asset Bundle

Ask the user: "Would you like to package this as a Databricks Asset Bundle for repeatable deployment?"

If yes, **invoke**: `/databricks-asset-bundles`

Tell the DABs skill:

> "Package the {industry} data warehouse demo in `{catalog}.{schema}` as a Databricks Asset Bundle. Include:
> - The SDP pipeline (ID: {pipeline_id from Step 3})
> - Pipeline source files organized by layer
> - A scheduled job for pipeline refresh
> - Dev/prod deployment targets
> Validate with `databricks bundle validate`."

**Report**: Bundle structure, deployment targets, and validation status.

---

## Step 6: Optional — Schedule Refresh Job

Ask the user: "Would you like to create a scheduled job to refresh the pipeline?"

If yes, **invoke**: `/databricks-jobs`

Tell the jobs skill:

> "Create a Databricks job to refresh the SDP pipeline `{pipeline_name}` (ID: {pipeline_id}) on a schedule. Use:
> - Pipeline task type targeting the existing pipeline
> - Daily schedule at 6am UTC (or ask user preference)
> - Email notification on failure
> - Serverless compute"

**Report**: Job ID, schedule, and next run time.

---

## Completion Summary

After all steps, present:

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
Job:         {job_id or "Not requested"}
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

## Skills Invoked by This Flow

| Step | Skill | Slash Command |
|------|-------|---------------|
| 2 | Synthetic Data Generation | `/databricks-synthetic-data-gen` |
| 3 | Spark Declarative Pipelines | `/databricks-spark-declarative-pipelines` |
| 4 | AI/BI Dashboards | `/databricks-aibi-dashboards` |
| 5 | Asset Bundles | `/databricks-asset-bundles` |
| 6 | Jobs | `/databricks-jobs` |
