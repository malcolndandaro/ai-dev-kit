---
name: data-explorer
description: "Profile and explore Databricks tables. Generates a structured data profiling report with row counts, null rates, distributions, relationships, and data quality flags."
disable-model-invocation: true
argument-hint: "<catalog.schema> [table1,table2,...]"
context: fork
agent: data-explorer-agent
---

# Data Explorer Flow

Profile tables in a Databricks schema and produce a structured exploration report.

> **Arguments are required.** The user MUST provide `catalog.schema`. If not provided, stop and ask for it — do not guess or default.

## Inputs

| Argument | Required | Example |
|----------|----------|---------|
| `catalog.schema` | **Yes** | `my_catalog.sales_data` |
| `table1,table2,...` | No — profiles ALL tables in schema if omitted | `orders,customers,products` |

## Execution

Run these steps exactly. Do NOT ask the user for input between steps — execute the full profiling pipeline autonomously and present the final report.

### Step 1: Get Warehouse

```
get_best_warehouse
```

If no warehouse available, stop and tell the user.

### Step 2: Validate Schema Exists

```sql
-- Run via execute_sql
SHOW TABLES IN {catalog}.{schema}
```

If schema doesn't exist or is empty, stop and report the error.

### Step 3: Get Table Metadata

```
get_table_details(catalog="{catalog}", schema="{schema}")
```

If user specified tables, filter to only those. Otherwise, profile ALL tables in the schema (up to 20 tables max — if more than 20, profile the first 20 and note the rest were skipped).

### Step 4: Profile Each Table

For EACH table, run these queries via `execute_sql`. Combine into as few queries as possible.

**4a. Row count + column nulls + distinct counts** (one query per table):

```sql
SELECT
  COUNT(*) AS total_rows,
  {for each column:}
  COUNT({col}) AS {col}_non_null,
  COUNT(DISTINCT {col}) AS {col}_distinct,
  {end for}
FROM {catalog}.{schema}.{table}
```

**4b. Numeric stats** (one query per table, only for INT/BIGINT/DECIMAL/DOUBLE/FLOAT columns):

```sql
SELECT
  {for each numeric column:}
  MIN({col}) AS {col}_min,
  MAX({col}) AS {col}_max,
  ROUND(AVG({col}), 2) AS {col}_avg,
  {end for}
FROM {catalog}.{schema}.{table}
```

**4c. Date range** (one query per table, only for DATE/TIMESTAMP columns):

```sql
SELECT
  {for each date column:}
  MIN({col}) AS {col}_earliest,
  MAX({col}) AS {col}_latest,
  DATEDIFF(DAY, MIN({col}), MAX({col})) AS {col}_span_days,
  {end for}
FROM {catalog}.{schema}.{table}
```

**4d. Top values for categorical columns** (only for STRING columns with <50 distinct values):

```sql
SELECT {col}, COUNT(*) AS cnt
FROM {catalog}.{schema}.{table}
GROUP BY {col}
ORDER BY cnt DESC
LIMIT 5
```

**4e. Sample rows** (5 rows per table for quick inspection):

```sql
SELECT * FROM {catalog}.{schema}.{table} LIMIT 5
```

### Step 5: Detect Relationships

Analyze column metadata across all profiled tables:

1. **FK pattern**: Column named `{other_table}_id` or `{other_table}_key` → maps to `{other_table}.id` or `{other_table}.{other_table}_id`
2. **Shared columns**: Same column name in 2+ tables (e.g., `customer_id` in `orders` and `returns`)
3. **PK candidates**: Columns with 0 nulls AND distinct count = row count

For each detected relationship, validate with a count query:
```sql
SELECT COUNT(*) AS match_count
FROM {catalog}.{schema}.{table_a} a
JOIN {catalog}.{schema}.{table_b} b ON a.{fk_col} = b.{pk_col}
```

### Step 6: Generate Report

Output this EXACT format — do not deviate:

```
════════════════════════════════════════════════════════════
  DATA EXPLORATION REPORT
  {catalog}.{schema}
  Generated: {timestamp}
════════════════════════════════════════════════════════════

SCHEMA OVERVIEW
───────────────
Tables: {count}
Total rows: {sum across all tables}

TABLE PROFILES
──────────────

▸ {table_name} ({row_count} rows, {col_count} columns)

  | Column | Type | Nulls % | Distinct | Min | Max | Top Values |
  |--------|------|---------|----------|-----|-----|------------|
  | {col}  | {type} | {pct} | {n}    | {v} | {v} | {val (pct)} |

  ⚠ Data quality flags:
  - {col}: {issue description}

  Sample (5 rows):
  | col1 | col2 | col3 | ... |
  |------|------|------|-----|

{repeat for each table}

RELATIONSHIPS
─────────────

  | Source | Column | Target | Column | Type | Validated |
  |--------|--------|--------|--------|------|-----------|
  | {tbl}  | {col}  | {tbl}  | {col}  | FK   | ✓ {n} matches |

DATA QUALITY SUMMARY
────────────────────
⚠ High null columns (>10%):
  - {catalog}.{schema}.{table}.{col}: {pct}% null

⚠ Constant columns (1 distinct value):
  - {catalog}.{schema}.{table}.{col}: always "{value}"

⚠ Potential PKs (0 nulls, all distinct):
  - {catalog}.{schema}.{table}.{col}

NEXT STEPS
──────────
Based on this data, you can:
• Build a dashboard        → /databricks-aibi-dashboards
• Create a pipeline        → /databricks-spark-declarative-pipelines
• Build a full DW demo     → /e2e-data-warehouse
• Generate more test data  → /databricks-synthetic-data-gen
• Query with natural lang  → /databricks-genie
════════════════════════════════════════════════════════════
```

## Rules

- **Run autonomously** — do NOT pause to ask the user between steps
- **Use only MCP tools**: `execute_sql`, `get_table_details`, `get_best_warehouse`
- **Always use fully-qualified names**: `catalog.schema.table`
- **Large tables (>1M rows)**: Add `TABLESAMPLE (1 PERCENT)` to distribution queries in Step 4d
- **Max 20 tables** — skip the rest with a note
- **Errors**: If a query fails on one table, log the error and continue with the next table
- **No data modification** — never run INSERT, UPDATE, DELETE, CREATE, DROP, or ALTER
