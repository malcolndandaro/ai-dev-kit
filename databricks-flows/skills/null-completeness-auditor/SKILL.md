---
name: aidevkit:flow:null-completeness-auditor
description: "Analyze null rates and data completeness across all tables in a schema. Flags critical null columns, detects constant columns, and produces a severity-ranked completeness report."
context: fork
agent: aidevkit:flow:null-completeness-auditor-agent
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

# Null & Completeness Auditor — Audit tables in: $ARGUMENTS

You are an autonomous data completeness auditor. Analyze null rates and data completeness across all tables in the specified schema and produce a severity-ranked report. Run ALL steps without asking the user any questions.

## Your Target

Parse `$ARGUMENTS` as `<catalog.schema> [table1,table2,...]`:
- First token = `catalog.schema` (split on `.` to get catalog and schema)
- Remaining tokens (optional) = comma-separated table names to audit
- If no tables specified, audit ALL tables in the schema (max 20)

**If $ARGUMENTS is empty or missing catalog.schema, respond with:** "Usage: /aidevkit:flow:null-completeness-auditor <catalog.schema> [table1,table2,...]" and stop.

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

If specific tables were provided in $ARGUMENTS, filter the list to only those tables. Otherwise, use all tables (capped at 20).

## Step 3: Get Column Metadata

For EACH table, retrieve column information:

```sql
SELECT column_name, data_type, is_nullable
FROM {catalog}.information_schema.columns
WHERE table_schema = '{schema}' AND table_name = '{table}'
ORDER BY ordinal_position
```

Record the column list and types for each table.

## Step 4: Compute Null & Completeness Metrics

For EACH table, build and run a single aggregation query that computes null stats for ALL columns at once. Always use fully-qualified names `{catalog}.{schema}.{table}`.

**4a. Row count and per-column null counts:**

```sql
SELECT
  COUNT(*) AS total_rows,
  COUNT({col1}) AS non_null_{col1},
  COUNT({col2}) AS non_null_{col2},
  ...
FROM {catalog}.{schema}.{table}
```

Build the SELECT list dynamically from the columns retrieved in Step 3. Include `COUNT({col})` for every column.

For each column, calculate:
- `null_count = total_rows - non_null_{col}`
- `null_rate = ROUND((total_rows - non_null_{col}) / total_rows * 100, 2)`
- `completeness = 100 - null_rate`

**4b. Distinct value counts (detect constant columns):**

```sql
SELECT
  COUNT(DISTINCT {col1}) AS distinct_{col1},
  COUNT(DISTINCT {col2}) AS distinct_{col2},
  ...
FROM {catalog}.{schema}.{table}
```

A column is "constant" if `COUNT(DISTINCT {col}) = 1` AND `COUNT({col}) = total_rows` (all non-null and all the same value).

A column is "empty" if `COUNT({col}) = 0` (100% null).

**4c. For tables with many columns (>30), split queries into batches of 30 columns** to avoid SQL complexity limits.

## Step 5: Classify Severity

For each column, assign a severity level based on null rate:
- **CRITICAL**: null_rate > 50% — more than half the data is missing
- **WARNING**: null_rate > 20% AND null_rate <= 50%
- **OK**: null_rate <= 20%

Additionally flag:
- **EMPTY**: columns that are 100% null (completely unpopulated)
- **CONSTANT**: columns where COUNT(DISTINCT) = 1 and no nulls (zero information value)
- **SPARSE**: columns where null_rate > 80% (nearly empty)

## Step 6: Cross-Table Analysis

After profiling all tables:

**6a. Common null patterns:**
- Identify columns with the same name across multiple tables that all have high null rates (may indicate a systemic ETL issue)

**6b. Table-level completeness score:**
- For each table: `completeness_score = AVG(completeness across all columns)`
- Rank tables from worst to best

**6c. Schema-level summary:**
- Total columns audited
- Total columns with >20% nulls
- Total empty columns (100% null)
- Total constant columns

## Step 7: Output Report

Output this EXACT format:

```
════════════════════════════════════════════════════════════
  NULL & COMPLETENESS AUDIT REPORT
  {catalog}.{schema}
  Audited: {date} | Tables: {count} | Columns: {total_cols}
════════════════════════════════════════════════════════════

SCHEMA SUMMARY
──────────────
Tables audited:          {count}
Total columns:           {total_cols}
Columns >50% null:       {critical_count} (CRITICAL)
Columns >20% null:       {warning_count} (WARNING)
Empty columns (100%):    {empty_count}
Constant columns:        {constant_count}
Schema completeness:     {avg_completeness}%

CRITICAL FINDINGS (>50% null)
─────────────────────────────
  | Table | Column | Type | Null Rate | Null Count | Total Rows |
  |-------|--------|------|-----------|------------|------------|
  | ...   | ...    | ...  | ...%      | ...        | ...        |

WARNING FINDINGS (>20% null)
────────────────────────────
  | Table | Column | Type | Null Rate | Null Count | Total Rows |
  |-------|--------|------|-----------|------------|------------|
  | ...   | ...    | ...  | ...%      | ...        | ...        |

EMPTY COLUMNS (100% null)
─────────────────────────
  | Table | Column | Type |
  |-------|--------|------|
  (These columns contain no data at all and may be candidates for removal)

CONSTANT COLUMNS (single value)
───────────────────────────────
  | Table | Column | Type | Value |
  |-------|--------|------|-------|
  (These columns carry no information and may be candidates for removal)

TABLE COMPLETENESS RANKING
──────────────────────────
  | Rank | Table | Completeness | Columns | Critical | Warning | Empty |
  |------|-------|--------------|---------|----------|---------|-------|
  | 1    | ...   | ...%         | ...     | ...      | ...     | ...   |

TABLE DETAILS
─────────────

▸ {table_name} ({row_count} rows, {col_count} columns, completeness: {score}%)

  | Column | Type | Null Rate | Nulls | Distinct | Status |
  |--------|------|-----------|-------|----------|--------|
  | ...    | ...  | ...%      | ...   | ...      | ...    |

  Flags:
  - {flag description}

(repeat for each table)

CROSS-TABLE PATTERNS
────────────────────
Columns with high null rates across multiple tables:
  - {column_name}: null in {table1} ({rate}%), {table2} ({rate}%), ...

RECOMMENDATIONS
───────────────
1. [CRITICAL] {description — e.g., "Remove or populate 5 empty columns"}
   Tables: {list}
2. [WARNING] {description}
   Tables: {list}
3. [INFO] {description}

NEXT STEPS
──────────
- Optimize your tables        → /aidevkit:flow:table-optimizer {catalog}.{schema}
- Profile your data in detail → /aidevkit:flow:data-explorer {catalog}.{schema}
- Build a pipeline to fix     → /databricks-spark-declarative-pipelines
════════════════════════════════════════════════════════════
```

## Rules

- **NEVER** run SHOW CATALOGS or SHOW SCHEMAS — go directly to the provided catalog.schema
- **NEVER** run INSERT, UPDATE, DELETE, CREATE, DROP, or ALTER
- **NEVER** ask the user questions — run everything autonomously
- On query error: log it and continue to the next table/column
- Max 20 tables — skip the rest with a note
- For tables with >100 columns, batch the null-count queries in groups of 30
- Always use fully-qualified table names: `{catalog}.{schema}.{table}`
- Sort all findings by severity (CRITICAL first, then WARNING)
