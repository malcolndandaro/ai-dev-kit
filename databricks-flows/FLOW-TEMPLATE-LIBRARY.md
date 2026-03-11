# Flow Template Library

Reference material for building user-defined flows in the Databricks AI Dev Kit.

Each flow is a `SKILL.md` file with YAML frontmatter that spawns an autonomous agent. This document covers the canonical template categories, anatomy, composability patterns, reusable building blocks, and anti-patterns to avoid.

---

## What a Flow Is

A flow is a SKILL.md file that:

1. Declares `context: fork` — it runs as an isolated subagent, not inline in the main conversation.
2. Declares an `agent:` — the agent definition file that governs tool access and persona.
3. Contains step-by-step instructions for autonomous execution against `$ARGUMENTS`.
4. **Never asks the user questions** after launch (except for missing required arguments, which are validated upfront).
5. Outputs a structured report in a fixed format.

```yaml
---
name: aidevkit:flow:<your-flow-name>
description: "One sentence describing what this flow does and its output."
context: fork
agent: aidevkit:flow:<your-flow-name>-agent
allowed-tools:
  - Bash
  - mcp__databricks__execute_sql
  - mcp__databricks__get_best_warehouse
disable-model-invocation: true
---
```

The `allowed-tools` list is the **permission boundary** for the forked agent. Only list tools the flow genuinely needs.

---

## Template Categories

### Category 1: Data Quality

**Purpose**: Detect problems in data before they reach consumers. These flows are read-only and produce scored reports with fix guidance.

---

#### 1a. Null & Completeness Auditor

**Complexity**: Simple
**Description**: Scans a schema for null rates, constant columns, and columns that should be non-nullable based on naming conventions (`_id`, `_key`, `_date` suffixes).

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`
- `mcp__databricks__get_table_details`

**Input pattern**: `<catalog.schema> [null_threshold_pct]`
- `catalog.schema` — required
- `null_threshold_pct` — optional, default 5. Columns above this threshold are flagged.

**Output pattern**: Markdown report with a per-table null heatmap and a ranked list of highest-null columns across the schema.

**Core queries**:
```sql
-- Null rate per column (generate dynamically per table)
SELECT
  '{col}' AS column_name,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_count,
  ROUND(100.0 * SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_pct
FROM {catalog}.{schema}.{table}
```

**Rules**:
- NEVER run INSERT, UPDATE, DELETE, CREATE, DROP, or ALTER
- Read-only; skip tables on error and log

---

#### 1b. Schema Drift Detector

**Complexity**: Medium
**Description**: Compares the current table schema against a previously saved snapshot (stored as a JSON file in a Unity Catalog Volume). Reports added, removed, and type-changed columns. Ideal for catching breaking changes in upstream sources.

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`
- `mcp__databricks__get_table_details`
- `Read` (to load snapshot file)
- `Write` (to save new snapshot)
- `Bash` (for JSON comparison)

**Input pattern**: `<catalog.schema> [snapshot_volume_path]`
- `catalog.schema` — required
- `snapshot_volume_path` — optional, defaults to `/Volumes/{catalog}/{schema}/metadata/schema_snapshots/`

**Output pattern**: Diff report showing added/removed/changed columns per table. Optionally updates the snapshot.

**Core queries**:
```sql
-- Current schema
DESCRIBE TABLE {catalog}.{schema}.{table}

-- Fetch column metadata from information_schema
SELECT column_name, data_type, is_nullable
FROM {catalog}.information_schema.columns
WHERE table_schema = '{schema}' AND table_name = '{table}'
ORDER BY ordinal_position
```

**Rules**:
- Ask user before overwriting the snapshot (unless `--update-snapshot` flag is passed in arguments)
- NEVER modify table schemas

---

#### 1c. Data Freshness Monitor

**Complexity**: Simple
**Description**: Checks when each table in a schema was last updated. Flags tables that have not received new data within a configurable threshold. Uses `DESCRIBE HISTORY` for Delta tables and `_metadata.file_modification_time` for external tables.

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`
- `mcp__databricks__get_table_details`

**Input pattern**: `<catalog.schema> [stale_hours]`
- `catalog.schema` — required
- `stale_hours` — optional, default 24. Tables not updated within this window are flagged STALE.

**Output pattern**: Table with last-modified timestamp, hours since update, and FRESH/STALE/UNKNOWN status per table.

**Core queries**:
```sql
-- Delta table last modification
DESCRIBE HISTORY {catalog}.{schema}.{table} LIMIT 1

-- Row-level freshness via timestamp column (when available)
SELECT MAX({timestamp_col}) AS latest_record
FROM {catalog}.{schema}.{table}
```

---

### Category 2: Pipeline Operations

**Purpose**: Automate operational tasks on pipelines and jobs — backfills, replays, status checks. These flows may be read-write; they must confirm destructive actions before proceeding.

---

#### 2a. Pipeline Health Check

**Complexity**: Simple
**Description**: Inspects all Spark Declarative Pipelines (or Delta Live Tables pipelines) in a workspace and reports their status, last run time, error counts, and data quality expectation pass rates.

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`
- `Bash` (for Databricks CLI: `databricks pipelines list`, `databricks pipelines get`)

**Input pattern**: `[pipeline_name_filter]`
- `pipeline_name_filter` — optional substring filter on pipeline name

**Output pattern**: Status table with columns: Pipeline Name, State, Last Run, Duration, Rows Processed, Expectation Pass Rate, Failed Expectations.

**Core queries**:
```sql
-- Pipeline events from system tables
SELECT pipeline_id, event_type, message, timestamp
FROM system.lakeflow.pipeline_events
WHERE pipeline_id = '{pipeline_id}'
ORDER BY timestamp DESC
LIMIT 100
```

**Rules**:
- NEVER start, stop, or modify pipelines
- Read-only status inspection only

---

#### 2b. Backfill Planner

**Complexity**: Medium
**Description**: Given a table and a date range, identifies missing partitions or date gaps in the data. Generates a set of ready-to-run `INSERT OVERWRITE` or pipeline trigger commands to fill the gaps. Does NOT execute the backfill itself — it outputs the commands for human review.

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`
- `mcp__databricks__get_table_details`
- `Write` (to write the backfill script to a local file)

**Input pattern**: `<catalog.schema.table> <start_date> <end_date> [date_column]`
- `catalog.schema.table` — required, fully qualified
- `start_date` / `end_date` — required, format `YYYY-MM-DD`
- `date_column` — optional, auto-detected from column names if not provided

**Output pattern**: Markdown report with the gap analysis table and a generated SQL backfill script saved to a local `.sql` file.

**Core queries**:
```sql
-- Detect date gaps
WITH expected_dates AS (
  SELECT explode(sequence(
    DATE('{start_date}'), DATE('{end_date}'), INTERVAL 1 DAY
  )) AS expected_date
),
actual_dates AS (
  SELECT DISTINCT DATE({date_col}) AS actual_date
  FROM {catalog}.{schema}.{table}
  WHERE {date_col} BETWEEN '{start_date}' AND '{end_date}'
)
SELECT e.expected_date AS missing_date
FROM expected_dates e
LEFT JOIN actual_dates a ON e.expected_date = a.actual_date
WHERE a.actual_date IS NULL
ORDER BY missing_date
```

**Rules**:
- Output commands only — NEVER execute INSERT OVERWRITE, MERGE, or pipeline runs
- Warn the user if the gap spans more than 90 days (large backfill risk)

---

#### 2c. Job Run Monitor

**Complexity**: Simple
**Description**: Fetches the last N runs for all Databricks Jobs in a workspace (or filtered by name). Surfaces failures, long-running jobs, and jobs that have not run recently.

**Required tools**:
- `Bash` (for Databricks CLI: `databricks jobs list`, `databricks runs list`)
- `mcp__databricks__execute_sql` (for system table queries)

**Input pattern**: `[job_name_filter] [lookback_days]`
- `job_name_filter` — optional substring
- `lookback_days` — optional, default 7

**Output pattern**: Report with job name, last run status, last run duration, success rate over the lookback window, and alert if the job has not run in >24 hours beyond its schedule.

**Core queries**:
```sql
-- Job runs from system tables
SELECT job_id, run_id, run_name, state_life_cycle_state, state_result_state,
       start_time, end_time,
       TIMESTAMPDIFF(MINUTE, start_time, end_time) AS duration_mins
FROM system.lakeflow.job_run_timeline
WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL {lookback_days} DAYS
ORDER BY start_time DESC
```

---

### Category 3: Cost Analysis

**Purpose**: Surface spending, storage bloat, and inefficiency signals from Unity Catalog system tables. Always read-only.

---

#### 3a. Warehouse Cost Analyzer

**Complexity**: Medium
**Description**: Queries system tables for SQL warehouse usage over a configurable time window. Breaks down DBU consumption by warehouse, user, and query type. Identifies the top 10 most expensive queries.

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`

**Input pattern**: `[lookback_days] [warehouse_name_filter]`
- `lookback_days` — optional, default 30
- `warehouse_name_filter` — optional

**Output pattern**: Report with total DBU spend, top warehouses by cost, top users by cost, top queries by execution time and DBU, and recommendations (idle auto-stop, right-sizing).

**Core queries**:
```sql
-- Warehouse usage
SELECT warehouse_id, warehouse_name,
       SUM(billing_originator_dbus) AS total_dbus,
       COUNT(*) AS query_count,
       AVG(execution_duration_ms) / 1000 AS avg_duration_secs
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - {lookback_days}
  AND billing_origin_product = 'INTERACTIVE'
GROUP BY 1, 2
ORDER BY total_dbus DESC
```

```sql
-- Top expensive queries
SELECT query_id, user_name, warehouse_id,
       execution_duration_ms / 1000 AS duration_secs,
       total_task_duration_ms / 1000 AS total_task_secs,
       statement_text
FROM system.query.history
WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL {lookback_days} DAYS
ORDER BY execution_duration_ms DESC
LIMIT 20
```

---

#### 3b. Storage Cost Auditor

**Complexity**: Medium
**Description**: Scans all tables in a catalog (or schema) and reports storage size, number of files, estimated file fragmentation waste, and tables that have not been VACUUMed recently. Produces a prioritized list of storage reclamation actions.

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`

**Input pattern**: `<catalog[.schema]> [size_threshold_gb]`
- `catalog` or `catalog.schema` — required
- `size_threshold_gb` — optional, default 1. Only report on tables above this size.

**Output pattern**: Table with name, size, file count, avg file size, last VACUUM, and estimated reclaimable space. Total savings estimate at the bottom.

**Core queries**:
```sql
-- Storage metrics per table
SELECT table_catalog, table_schema, table_name,
       total_size_bytes / 1073741824.0 AS size_gb,
       num_files,
       total_size_bytes / NULLIF(num_files, 0) / 1048576.0 AS avg_file_size_mb
FROM system.information_schema.table_storage_metrics
WHERE table_catalog = '{catalog}'
ORDER BY total_size_bytes DESC
```

---

#### 3c. Query Optimization Advisor

**Complexity**: Complex
**Description**: Analyzes the query history for a warehouse and identifies patterns that indicate optimization opportunities: full table scans on large tables, missing ZORDER/clustering on heavily-filtered columns, cross-join patterns, and repeated identical queries that could be cached.

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`

**Input pattern**: `<warehouse_id_or_name> [lookback_days]`

**Output pattern**: Ranked list of optimization opportunities with estimated savings, affected queries, and specific SQL or table property recommendations.

---

### Category 4: Security and Governance

**Purpose**: Audit permissions, detect sensitive data, and surface lineage gaps. Always read-only.

---

#### 4a. Permission Auditor

**Complexity**: Medium
**Description**: For a given catalog or schema, lists all grants and identifies: overly-broad `ALL PRIVILEGES` grants, direct grants to users (should use groups), public grants, and service principals with write access to production schemas.

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`

**Input pattern**: `<catalog[.schema]>`

**Output pattern**: Report with a grant inventory table and a flagged-issues section sorted by severity (CRITICAL: public write access → HIGH: ALL PRIVILEGES to non-admin users → MEDIUM: direct user grants).

**Core queries**:
```sql
-- Catalog grants
SHOW GRANTS ON CATALOG {catalog}

-- Schema grants
SHOW GRANTS ON SCHEMA {catalog}.{schema}

-- Table grants
SHOW GRANTS ON TABLE {catalog}.{schema}.{table}
```

---

#### 4b. PII Column Scanner

**Complexity**: Medium
**Description**: Scans column names and sample data across a schema to identify likely PII columns (names, emails, SSNs, phone numbers, addresses, DOBs) using pattern matching on column names and regex sampling on data values. Does NOT read full tables — only samples 100 rows per column.

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`
- `mcp__databricks__get_table_details`

**Input pattern**: `<catalog.schema>`

**Output pattern**: PII risk matrix per table — column name, detected PII type, confidence (HIGH/MEDIUM/LOW based on name match + data sample), current masking policy (if any), and recommended Unity Catalog tag.

**Core queries**:
```sql
-- Sample column values for regex validation
SELECT {col} FROM {catalog}.{schema}.{table}
WHERE {col} IS NOT NULL
LIMIT 100

-- Check existing column masks
SELECT table_name, column_name, mask_name
FROM {catalog}.information_schema.column_masks
WHERE table_schema = '{schema}'
```

**Rules**:
- NEVER print more than 3 sample values per column in the report (mask the rest)
- Log a warning if full PII values appear in query results

---

#### 4c. Lineage Gap Reporter

**Complexity**: Complex
**Description**: Uses Unity Catalog system tables to trace the lineage of gold/reporting tables back to their sources. Identifies tables with no recorded lineage (potentially untracked), broken lineage chains, and tables consumed by downstream processes but not updated recently.

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`

**Input pattern**: `<catalog.schema> [max_depth]`
- `max_depth` — optional, default 5 hops

**Output pattern**: Lineage graph summary (text-based), list of lineage gaps with affected downstream consumers, and tables with no upstream lineage registered.

**Core queries**:
```sql
-- Entity lineage from system tables
SELECT source_table_full_name, target_table_full_name, event_time
FROM system.access.table_lineage
WHERE target_table_full_name LIKE '{catalog}.{schema}.%'
ORDER BY event_time DESC
```

---

### Category 5: Reporting

**Purpose**: Generate periodic summaries, update data catalog metadata, and validate SLAs. May include lightweight writes to metadata tables if explicitly scoped.

---

#### 5a. Daily Metrics Digest

**Complexity**: Simple
**Description**: Runs a set of user-defined SQL queries (loaded from a config file) against gold layer tables and assembles the results into a formatted daily digest. Designed to be scheduled as a Databricks Job.

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`
- `Read` (to load query config file)

**Input pattern**: `<config_file_path> [date]`
- `config_file_path` — path to a JSON/YAML file defining the metrics queries
- `date` — optional, defaults to yesterday (`CURRENT_DATE - 1`)

**Output pattern**: Formatted markdown digest with metric name, current value, previous value, and delta (with trend arrow). Optionally saved to a Volume.

**Config file format**:
```json
{
  "metrics": [
    {
      "name": "Daily Revenue",
      "query": "SELECT SUM(total_amount) FROM {catalog}.{schema}.fact_daily_sales WHERE sale_date = '{date}'",
      "format": "currency"
    }
  ]
}
```

---

#### 5b. SLA Monitor

**Complexity**: Medium
**Description**: Checks that critical tables have been updated within their defined SLA windows and that pipeline runs completed within their expected time budgets. Produces a RAG (Red/Amber/Green) status dashboard.

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`
- `Bash` (for Databricks CLI job run queries)
- `Read` (to load SLA definition file)

**Input pattern**: `<sla_config_path>`

**Output pattern**: RAG status table per SLA item with breach details, trend (how many consecutive days breached), and escalation recommendation.

**SLA config format**:
```json
{
  "slas": [
    {
      "name": "Orders table freshness",
      "type": "table_freshness",
      "table": "catalog.schema.orders",
      "max_age_hours": 4,
      "timestamp_column": "updated_at"
    },
    {
      "name": "ETL pipeline completion",
      "type": "job_duration",
      "job_name": "daily_etl",
      "max_duration_minutes": 30
    }
  ]
}
```

---

#### 5c. Data Catalog Enricher

**Complexity**: Medium
**Description**: Uses an LLM (via Databricks Foundation Models) to generate table and column comments for undocumented tables, based on column names, data types, and sample values. Outputs the `ALTER TABLE` and `ALTER TABLE CHANGE COLUMN` statements for human review — does NOT apply them automatically.

**Required tools**:
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`
- `mcp__databricks__get_table_details`
- `Write` (to save generated SQL script)

**Input pattern**: `<catalog.schema> [--dry-run]`

**Output pattern**: SQL script file with all `ALTER TABLE` comment statements, plus a preview in the console. User runs the script manually.

**Rules**:
- NEVER execute ALTER TABLE statements directly — output only
- Always show a sample of generated comments for user review before finalizing the script

---

### Category 6: DevOps and Deployment

**Purpose**: Validate deployments, compare environments, and surface infrastructure drift. These flows interact with the Databricks CLI and workspace APIs.

---

#### 6a. Deployment Validator

**Complexity**: Medium
**Description**: After deploying a Databricks Asset Bundle (DAB), verifies that all expected resources exist and are in healthy state. Checks jobs, pipelines, serving endpoints, and tables against a manifest.

**Required tools**:
- `Bash` (for `databricks jobs list`, `databricks pipelines list`, `databricks bundle validate`)
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`
- `Read` (to read `databricks.yml` bundle manifest)

**Input pattern**: `<bundle_path> [target_environment]`
- `bundle_path` — path to the DAB directory
- `target_environment` — optional, e.g., `prod`, `staging`

**Output pattern**: Checklist report — each expected resource listed with FOUND/MISSING/DEGRADED status, plus a deployment health score.

---

#### 6b. Environment Comparator

**Complexity**: Complex
**Description**: Compares two Databricks environments (e.g., staging vs. production) across three dimensions: schema structure (tables, columns, types), job/pipeline definitions, and serving endpoint configurations. Outputs a diff report.

**Required tools**:
- `Bash` (for Databricks CLI calls to both environments via `--profile`)
- `mcp__databricks__execute_sql`
- `mcp__databricks__get_best_warehouse`

**Input pattern**: `<source_catalog.schema> <target_catalog.schema> [--profile-source <name>] [--profile-target <name>]`

**Output pattern**: Three-section diff: schema diff, job config diff, endpoint diff. Highlights breaking changes in red (columns removed, types changed, required job configs missing).

**Rules**:
- NEVER modify either environment — read-only comparison
- Never use `--profile` flags unless provided in arguments

---

#### 6c. Bundle Drift Detector

**Complexity**: Medium
**Description**: Compares the deployed state of a Databricks workspace against what is defined in a local DAB `databricks.yml`. Surfaces configuration drift — jobs that have been modified in the UI after deployment, pipelines with manually changed cluster sizes, etc.

**Required tools**:
- `Bash` (for `databricks bundle validate`, `databricks jobs get`, `databricks pipelines get`)
- `Read` (to parse `databricks.yml` and resource files)

**Input pattern**: `<bundle_path> [target_environment]`

**Output pattern**: Drift report listing each resource, its deployed vs. defined state, and whether the drift is safe (additive) or breaking (removed config, changed type).

---

## Template Anatomy Reference

For any flow, the SKILL.md should always follow this structure:

```
1. YAML frontmatter       — name, description, context, agent, allowed-tools
2. Title line             — "# Flow Name — $ARGUMENTS"
3. Role declaration       — "You are an autonomous X agent."
4. Argument parsing       — Explicit parse rules for $ARGUMENTS with usage error
5. Numbered steps         — Step 1 through N, each with concrete tool calls or queries
6. Output format          — EXACT fixed-width format block
7. Rules section          — Hard constraints (NEVER, max N tables, error handling)
```

### Frontmatter Fields

| Field | Required | Notes |
|-------|----------|-------|
| `name` | Yes | Must follow `aidevkit:flow:<slug>` convention |
| `description` | Yes | One sentence; shown in `/help` lists |
| `context` | Yes | Always `fork` for flows |
| `agent` | Yes | References a companion agent `.md` in `agents/` |
| `allowed-tools` | Yes | Minimum required tools only |
| `disable-model-invocation` | Recommended | Set `true` to prevent recursive LLM calls |

---

## Composability Patterns

### Pattern A: Sequential Composition (Flow A calls Flow B)

Use this when Flow A produces output that Flow B consumes as input. In practice, the SKILL.md invokes a subsequent skill using the `Skill` tool or by embedding the subsequent flow's instructions directly as a sub-step.

Example: `e2e-data-warehouse` calls `/databricks-synthetic-data-gen`, then `/databricks-spark-declarative-pipelines`, then `/databricks-aibi-dashboards` in sequence.

```
## Step 3: Create Pipeline

**Invoke: `/databricks-spark-declarative-pipelines`**

Pass: catalog={catalog}, schema={schema}, tables=[...from step 2...]
```

**When to use**: Each step has a clear output artifact that the next step consumes. Steps cannot be parallelized because they have data dependencies.

**Constraint**: Sequential composition increases total latency. If steps are independent, use parallel fan-out instead.

---

### Pattern B: Parallel Fan-Out (Profile N items simultaneously)

Use this when the same operation must run against multiple independent targets. Structure the step as a loop instruction and tell the agent to run iterations as parallel sub-tasks where possible.

Example: A schema-wide scan that profiles 20 tables can issue all queries in rapid succession without waiting for each to complete before starting the next.

```
## Step 3: Profile Each Table

For EACH table in parallel:
- Run query 3a (null stats)
- Run query 3b (numeric stats)

Collect all results before proceeding to Step 4.
```

**When to use**: The same operation applies to multiple independent items (tables, jobs, pipelines). The agent issues all queries and collects results.

**Constraint**: MCP tool calls are inherently sequential in the Claude agent loop. "Parallel" here means minimizing blocking waits, not true parallelism. For true parallel execution, spawn multiple subagents via a parent orchestrator flow.

---

### Pattern C: Conditional Branching (If issue found, run deeper analysis)

Use this when a quick surface scan determines whether a deeper, more expensive analysis is warranted.

```
## Step 2: Quick Scan

Run the lightweight check. If any table scores below threshold:
→ Proceed to Step 3 (Deep Analysis)

If all tables pass:
→ Skip to Step 4 (Output Summary)
```

**When to use**: The deeper analysis is expensive (many queries, large tables) and only warranted when problems are detected. The quick scan acts as a gate.

**Example flow structure**:
1. Freshness check (fast — one query per table)
2. **Branch**: If stale tables found → run detailed row count analysis and gap detection
3. Output report

---

### Pattern D: Multi-Flow Orchestration via a Supervisor Flow

For workflows where 3+ independent flows must each run and their results must be synthesized, create a supervisor flow that spawns each sub-flow as a background task and waits for all results.

This requires the supervisor to use the `Agent` tool to spawn subagents, which means it does NOT use `context: fork` or `disable-model-invocation: true`.

```yaml
---
name: aidevkit:flow:daily-health-check
description: "Run all health checks in parallel: data quality, pipeline status, cost, and SLA monitoring."
# No 'context: fork' — this is an orchestrator
---
```

**When to use**: Multiple independent audits must run simultaneously and the results need to be combined into a single executive summary. Typically this is a "morning health check" type flow.

---

## Common Building Blocks

These are reusable patterns to copy into new flows. Each is a prose instruction block (not code) that the agent interprets.

---

### Building Block 1: Get Warehouse and Execute SQL

Use at the start of any flow that needs SQL execution:

```
## Step 1: Get Warehouse

Call `get_best_warehouse` to obtain a running SQL warehouse.
If no warehouse is available, stop and output:
"No running SQL warehouse found. Start a warehouse in the Databricks UI and re-run this flow."

Store the warehouse_id for all subsequent `execute_sql` calls.
```

---

### Building Block 2: Iterate Over Tables in a Schema

Use when a flow must process all tables in a schema:

```
## Step 2: List Tables

Execute:
```sql
SHOW TABLES IN {catalog}.{schema}
```

Extract the list of table names. If the result is empty, stop and report:
"No tables found in {catalog}.{schema}."

If more than 20 tables are found, process the first 20 alphabetically and append a note:
"Note: Schema contains {total} tables. Only the first 20 were analyzed."

For EACH table, proceed with the analysis steps below. On any query error,
log: "[ERROR] {table}: {error_message}" and continue to the next table.
```

---

### Building Block 3: Generate Markdown Report with Sections

Use to produce consistently-formatted output:

```
## Step N: Output Report

Output the following EXACT format. Do not deviate from the structure.

```
════════════════════════════════════════════════════
  {REPORT TITLE}
  {catalog}.{schema}
  Generated: {CURRENT_DATE} | Tables: {count}
════════════════════════════════════════════════════

SUMMARY
───────
{1-3 sentence executive summary}

{SECTION 1 TITLE}
─────────────────
{content}

{SECTION 2 TITLE}
─────────────────
{content}

NEXT STEPS
──────────
- {recommended action}  →  /aidevkit:flow:{related-flow} or /{skill}
════════════════════════════════════════════════════
```
```

Always end the report with a NEXT STEPS section pointing to related flows or skills.

---

### Building Block 4: Compare Before/After

Use when a flow must detect change between two states (schema drift, environment comparison, etc.):

```
## Step N: Compare States

Load the baseline state from {source}.
Collect the current state via {query or API call}.

For each item in the baseline:
- If present in current with same type/value → UNCHANGED
- If present in current with different type/value → CHANGED (record old → new)
- If absent from current → REMOVED

For each item in current not in baseline:
- ADDED

Sort output: REMOVED first, then CHANGED, then ADDED.
```

---

### Building Block 5: Threshold-Based Alerting

Use when a metric must be compared against a threshold and flagged accordingly:

```
Apply the following classification to each metric value:

CRITICAL  — value exceeds {critical_threshold}
WARNING   — value exceeds {warning_threshold} but not {critical_threshold}
OK        — value is at or below {warning_threshold}

In the output report:
- Use ❌ for CRITICAL items
- Use ⚠️ for WARNING items
- Use ✅ for OK items

Always list CRITICAL items first, then WARNING, then OK.
Do not suppress OK items — show the full list.
```

---

## Anti-Patterns

### Anti-Pattern 1: Modifying Data Without Confirmation

**Problem**: A flow that runs `INSERT OVERWRITE`, `MERGE`, `DELETE`, or `OPTIMIZE` without user review can cause data loss or corruption in production.

**Rule**: Flows that modify data MUST:
1. Output the full list of actions to be taken as SQL statements.
2. Require the user to pass an explicit `--execute` flag in `$ARGUMENTS` to confirm.
3. Default to dry-run mode (output only, no execution).

**Correct pattern**:
```
If `--execute` is NOT in $ARGUMENTS:
→ Output all SQL commands that would be run. Stop. Do NOT execute.

If `--execute` IS in $ARGUMENTS:
→ Execute the commands one at a time. Log each result.
```

---

### Anti-Pattern 2: Flows That Are Too Broad

**Problem**: A flow that tries to do data quality + cost analysis + permission audit + SLA monitoring in a single run will time out, produce incoherent output, and be impossible to debug.

**Rule**: Each flow should do exactly one thing. If you find yourself writing "and also check..." — split it into a second flow. Use a supervisor flow to orchestrate multiple targeted flows.

**Signal that a flow is too broad**:
- The output report has more than 5 major sections
- The flow queries more than 5 different system tables
- The step count exceeds 8

---

### Anti-Pattern 3: Flows Without Upfront Argument Validation

**Problem**: A flow that proceeds several expensive steps before discovering that the user provided a nonexistent catalog will waste time and produce confusing errors.

**Rule**: Always validate `$ARGUMENTS` as the first operation. If required arguments are missing or malformed, output the usage string and stop immediately — before making any MCP tool calls.

**Correct pattern**:
```
**If $ARGUMENTS is empty or does not match <catalog.schema>:**
Respond with: "Usage: /aidevkit:flow:<name> <catalog.schema> [options]"
Then stop. Do not proceed.
```

---

### Anti-Pattern 4: Hardcoding Catalog and Schema Names

**Problem**: A flow with `SELECT * FROM my_demo_catalog.sales.orders` is useless to any other user.

**Rule**: All catalog and schema references MUST come from `$ARGUMENTS`. Never hardcode a catalog, schema, table name, warehouse ID, cluster ID, or file path in the SKILL.md instructions.

**Correct pattern**:
```
Parse $ARGUMENTS to extract {catalog} and {schema}.
Use {catalog}.{schema}.{table} in every SQL query.
```

---

### Anti-Pattern 5: Skipping Error Handling

**Problem**: A flow that fails silently when one table throws an error and continues to the next will produce a partial report with no indication of what was missed.

**Rule**: Every iteration over a collection (tables, jobs, columns) must include:
1. A try/catch equivalent: "On error, log `[ERROR] {item}: {message}` and continue."
2. An error summary in the final report: "N items failed — see error log above."

---

### Anti-Pattern 6: Flows That Read Sensitive Data Verbosely

**Problem**: A PII scanner or permission auditor that prints full email addresses, SSNs, or secret values in its output report creates a security risk in the conversation history.

**Rule**: Any flow that samples sensitive columns must:
1. Show at most 3 example values per column, truncated to 20 characters.
2. Replace remaining characters with `***`.
3. Add a note in the report: "Full values redacted. Review in Databricks UI."

---

## Quick Reference: Flow Naming Convention

| Pattern | Example |
|---------|---------|
| `aidevkit:flow:<verb>-<noun>` | `aidevkit:flow:audit-permissions` |
| `aidevkit:flow:<noun>-<descriptor>` | `aidevkit:flow:schema-drift-detector` |
| `aidevkit:flow:<domain>-<action>` | `aidevkit:flow:cost-analyzer` |

The companion agent must be named `aidevkit:flow:<slug>-agent` and placed in `databricks-flows/agents/<slug>-agent.md`.

---

## Flow Complexity Guide

| Level | Step count | Tables touched | System tables used | Typical runtime |
|-------|-----------|----------------|-------------------|-----------------|
| Simple | 3-4 | 1-5 | 0-1 | < 30 seconds |
| Medium | 5-7 | 5-20 | 1-3 | 30s - 3 minutes |
| Complex | 8+ | 20+ | 3+ | 3-10 minutes |

For complex flows, always provide a `--quick` flag option in the argument parser that runs a reduced subset of checks.
