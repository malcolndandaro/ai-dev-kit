---
name: data-explorer
description: "Explore Databricks data interactively. Discover catalogs, schemas, tables, profile data quality, find relationships, and generate exploration reports. Use when you need to understand data, discover tables, profile columns, or find join candidates."
disable-model-invocation: true
argument-hint: "[catalog] [schema] [table]"
---

# Data Explorer Flow

Interactively explore your Databricks data — discover catalogs, schemas, and tables, profile data quality, identify relationships, and generate a structured exploration report.

## CRITICAL: Execution Rules

1. Execute steps **sequentially** — each step builds on the previous
2. **Ask the user** before narrowing scope (which catalog, which schema, which tables)
3. Use **MCP tools directly** for SQL execution — this flow does not invoke other skills
4. **Always use fully-qualified table names**: `catalog.schema.table`

## Workflow

```
┌─────────────────────────────────────────────────────────────┐
│  STEP 1: Connect & Discover Catalogs                        │
│  → get_best_warehouse, then execute_sql("SHOW CATALOGS")    │
├─────────────────────────────────────────────────────────────┤
│  STEP 2: Explore Schemas                                    │
│  → execute_sql("SHOW SCHEMAS IN <catalog>")                 │
├─────────────────────────────────────────────────────────────┤
│  STEP 3: Explore Tables                                     │
│  → execute_sql("SHOW TABLES IN <catalog>.<schema>")         │
│  → get_table_details(catalog, schema)                       │
├─────────────────────────────────────────────────────────────┤
│  STEP 4: Profile Selected Tables                            │
│  → execute_sql for row counts, nulls, distinct, min/max     │
├─────────────────────────────────────────────────────────────┤
│  STEP 5: Discover Relationships                             │
│  → Analyze FK patterns, shared columns, join candidates     │
├─────────────────────────────────────────────────────────────┤
│  STEP 6: Summarize & Suggest Next Actions                   │
│  → Report + recommend /databricks-aibi-dashboards,          │
│    /databricks-spark-declarative-pipelines, or              │
│    /e2e-data-warehouse as follow-ups                        │
└─────────────────────────────────────────────────────────────┘
```

---

## Step 1: Connect & Discover Catalogs

Get an available warehouse:
```
get_best_warehouse
```

If the user did **not** provide a catalog argument, list available catalogs:
```sql
SHOW CATALOGS
```

Present the catalog list and ask the user to choose one. If the user provided a catalog argument, skip to Step 2.

---

## Step 2: Explore Schemas

List schemas in the chosen catalog:
```sql
SHOW SCHEMAS IN <catalog>
```

If the user did **not** provide a schema argument, present the list and ask them to choose. Otherwise, proceed with the provided schema.

---

## Step 3: Explore Tables

List tables and get structural details:
```sql
SHOW TABLES IN <catalog>.<schema>
```

Then fetch detailed metadata:
```
get_table_details(catalog="<catalog>", schema="<schema>")
```

Present a summary table:

| Table | Type | Columns | Description |
|-------|------|---------|-------------|
| orders | MANAGED | 12 | Customer order records |
| ... | ... | ... | ... |

If the user did **not** provide a table argument, ask which table(s) they want to profile. The user can select one or multiple tables.

---

## Step 4: Profile Selected Tables

For each selected table, run profiling queries via `execute_sql`:

**Row count and null/distinct stats** (one query per table):
```sql
SELECT
  COUNT(*) AS row_count,
  COUNT(*) - COUNT(col1) AS col1_nulls,
  COUNT(DISTINCT col1) AS col1_distinct,
  COUNT(*) - COUNT(col2) AS col2_nulls,
  COUNT(DISTINCT col2) AS col2_distinct
FROM <catalog>.<schema>.<table>
```

**Numeric column stats** (for each numeric column):
```sql
SELECT
  MIN(<col>) AS min_val,
  MAX(<col>) AS max_val,
  AVG(<col>) AS avg_val,
  PERCENTILE(<col>, 0.5) AS median_val
FROM <catalog>.<schema>.<table>
```

**Date column stats** (for each date/timestamp column):
```sql
SELECT
  MIN(<col>) AS earliest,
  MAX(<col>) AS latest,
  DATEDIFF(MAX(<col>), MIN(<col>)) AS span_days
FROM <catalog>.<schema>.<table>
```

**Value distributions** (for categorical columns with <100 distinct values):
```sql
SELECT <col>, COUNT(*) AS cnt
FROM <catalog>.<schema>.<table>
GROUP BY <col>
ORDER BY cnt DESC
LIMIT 10
```

Present results:

| Column | Type | Nulls | Null % | Distinct | Min | Max | Top Value |
|--------|------|-------|--------|----------|-----|-----|-----------|
| order_id | BIGINT | 0 | 0% | 50000 | 1 | 50000 | - |
| status | STRING | 12 | 0.02% | 5 | - | - | completed (80%) |

---

## Step 5: Discover Relationships

Analyze column names across all tables in the schema to find join candidates:

1. **FK pattern detection** — Look for columns ending in `_id`, `_key`, `_code`, or matching `<other_table>_id` patterns
2. **Shared column names** — Find columns that appear in multiple tables (e.g., `customer_id` in both `orders` and `customers`)
3. **Name similarity** — Identify columns with similar names across tables that could be join keys

Present relationship findings:

| Table A | Column | Table B | Column | Relationship Type |
|---------|--------|---------|--------|-------------------|
| orders | customer_id | customers | id | FK candidate (naming pattern) |
| orders | product_id | products | id | FK candidate (naming pattern) |

**Suggest join queries** for the most likely relationships:
```sql
-- orders <-> customers (via customer_id = id)
SELECT o.*, c.name, c.email
FROM <catalog>.<schema>.orders o
JOIN <catalog>.<schema>.customers c ON o.customer_id = c.id
LIMIT 5
```

---

## Step 6: Summarize & Suggest Next Actions

Present a final exploration report:

**1. Schema Overview**
- Catalog, schema, number of tables, total rows

**2. Table Profiles**
- Per-table summary: row count, column count, key columns, date range
- Data quality flags: columns with >10% nulls, constant columns

**3. Relationships**
- Confirmed/suggested joins between tables

**4. Data Quality Notes**
- High null rates, potential data issues, cleanup recommendations

**5. Recommended Next Steps**

Based on the exploration results, suggest specific follow-up actions using slash commands:

- **"Build a dashboard on this data"** → Tell the user to invoke `/databricks-aibi-dashboards` with the discovered gold-ready tables
- **"Create a medallion pipeline"** → Tell the user to invoke `/databricks-spark-declarative-pipelines` to build bronze/silver/gold layers
- **"Generate more test data"** → Tell the user to invoke `/databricks-synthetic-data-gen` based on the discovered schema patterns
- **"Build a full data warehouse demo"** → Tell the user to invoke `/e2e-data-warehouse` to create a complete end-to-end demo
- **"Explore with natural language"** → Tell the user to invoke `/databricks-genie` to create a Genie Space for conversational exploration

---

## Available MCP Tools

| Tool | Usage |
|------|-------|
| `get_best_warehouse` | Get an available SQL warehouse ID |
| `execute_sql` | Run SQL queries for discovery and profiling |
| `get_table_details` | Get table schemas, column types, and metadata |

## Tips

- **Start broad, narrow down** — If the user doesn't know what they're looking for, start with catalogs and guide them
- **Batch profiling queries** — Combine multiple column stats into a single query to reduce round trips
- **Large tables** — For tables with >1M rows, use `TABLESAMPLE (1 PERCENT)` for distribution queries
- **Handle errors gracefully** — If a table is inaccessible, note it and continue with others
