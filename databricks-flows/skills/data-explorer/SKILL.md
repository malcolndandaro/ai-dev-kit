---
name: aidevkit:flow:data-explorer
description: "Profile Databricks tables and generate a data exploration report with row counts, null rates, distributions, and relationships."
context: fork
agent: aidevkit:flow:data-explorer-agent
allowed-tools:
  - Bash
  - Read
  - Grep
  - Glob
  - mcp__databricks__execute_sql
  - mcp__databricks__get_table_details
  - mcp__databricks__get_best_warehouse
disable-model-invocation: true
---

# Data Explorer — Profile tables in: $ARGUMENTS

You are an autonomous data profiling agent. Profile the Databricks schema specified below and produce a structured report. Run ALL steps without asking the user any questions.

## Your Target

Parse `$ARGUMENTS` as `<catalog.schema> [table1,table2,...]`:
- First token = `catalog.schema` (split on `.` to get catalog and schema)
- Remaining tokens (optional) = comma-separated table names to profile
- If no tables specified, profile ALL tables in the schema (max 20)

**If $ARGUMENTS is empty or missing catalog.schema, respond with:** "Usage: /aidevkit:flow:data-explorer <catalog.schema> [table1,table2,...]" and stop.

## Step 1: Get Warehouse

```
get_best_warehouse
```

If no warehouse available, stop and report the error.

## Step 2: List Tables

```sql
SHOW TABLES IN {catalog}.{schema}
```

Use `execute_sql` to run this. If the schema doesn't exist or is empty, stop and report.

Then get metadata:
```
get_table_details(catalog="{catalog}", schema="{schema}")
```

## Step 3: Profile Each Table

For EACH table, run these via `execute_sql`. Always use fully-qualified names `{catalog}.{schema}.{table}`.

**3a. Row count + null/distinct stats:**
```sql
SELECT COUNT(*) AS total_rows,
  COUNT({col}) AS {col}_non_null,
  COUNT(DISTINCT {col}) AS {col}_distinct
FROM {catalog}.{schema}.{table}
```

**3b. Numeric stats** (INT/BIGINT/DECIMAL/DOUBLE columns only):
```sql
SELECT MIN({col}) AS {col}_min, MAX({col}) AS {col}_max, ROUND(AVG({col}), 2) AS {col}_avg
FROM {catalog}.{schema}.{table}
```

**3c. Date range** (DATE/TIMESTAMP columns only):
```sql
SELECT MIN({col}) AS {col}_earliest, MAX({col}) AS {col}_latest
FROM {catalog}.{schema}.{table}
```

**3d. Top values** (STRING columns with <50 distinct values):
```sql
SELECT {col}, COUNT(*) AS cnt FROM {catalog}.{schema}.{table}
GROUP BY {col} ORDER BY cnt DESC LIMIT 5
```

**3e. Sample rows:**
```sql
SELECT * FROM {catalog}.{schema}.{table} LIMIT 5
```

For tables with >1M rows, add `TABLESAMPLE (1 PERCENT)` to 3d queries.

## Step 4: Detect Relationships

Analyze column metadata across all tables:
- Columns named `{other_table}_id` or `{other_table}_key` → FK candidate
- Same column name in 2+ tables → join candidate
- Columns with 0 nulls AND distinct = row count → PK candidate

For each detected relationship, validate:
```sql
SELECT COUNT(*) FROM {catalog}.{schema}.{table_a} a
JOIN {catalog}.{schema}.{table_b} b ON a.{fk_col} = b.{pk_col}
```

## Step 5: Output Report

Output this EXACT format:

```
════════════════════════════════════════════════════════════
  DATA EXPLORATION REPORT
  {catalog}.{schema}
════════════════════════════════════════════════════════════

SCHEMA OVERVIEW
───────────────
Tables: {count}
Total rows: {sum}

TABLE PROFILES
──────────────

▸ {table_name} ({row_count} rows, {col_count} columns)

  | Column | Type | Nulls % | Distinct | Min | Max | Top Values |
  |--------|------|---------|----------|-----|-----|------------|

  Data quality flags:
  - {col}: {issue}

  Sample (5 rows):
  | col1 | col2 | col3 |
  |------|------|------|

RELATIONSHIPS
─────────────
  | Source | Column | Target | Column | Validated |
  |--------|--------|--------|--------|-----------|

DATA QUALITY SUMMARY
────────────────────
High null columns (>10%): ...
Constant columns (1 distinct): ...
PK candidates (0 nulls, all distinct): ...

NEXT STEPS
──────────
- Build a dashboard        → /databricks-aibi-dashboards
- Create a pipeline        → /databricks-spark-declarative-pipelines
- Build a full DW demo     → /aidevkit:flow:e2e-data-warehouse
- Generate more test data  → /databricks-synthetic-data-gen
════════════════════════════════════════════════════════════
```

## Rules

- **NEVER** run SHOW CATALOGS or SHOW SCHEMAS — go directly to the provided catalog.schema
- **NEVER** run INSERT, UPDATE, DELETE, CREATE, DROP, or ALTER
- **NEVER** ask the user questions — run everything autonomously
- On query error: log it and continue to the next table
- Max 20 tables — skip the rest with a note
