---
name: data-explorer
description: "Explore Databricks data interactively. Discover catalogs, schemas, tables, profile data quality, find relationships, and generate exploration reports. Use when you need to understand data, discover tables, profile columns, or find join candidates."
disable-model-invocation: true
argument-hint: "[catalog] [schema] [table]"
---

# Data Explorer Flow

Interactively explore your Databricks data — discover catalogs, schemas, and tables, profile data quality, identify relationships, and generate a structured exploration report.

## Workflow

```
+---------------------------------------------------------------+
|  STEP 1: Connect & Discover                                   |
|  - Get warehouse via get_best_warehouse                       |
|  - If no catalog arg: SHOW CATALOGS via execute_sql           |
|  - Ask user to pick a catalog (or use provided arg)           |
+---------------------------------------------------------------+
|  STEP 2: Explore Schemas                                      |
|  - SHOW SCHEMAS IN <catalog> via execute_sql                  |
|  - If no schema arg: ask user to pick a schema                |
+---------------------------------------------------------------+
|  STEP 3: Explore Tables                                       |
|  - SHOW TABLES IN <catalog>.<schema> via execute_sql          |
|  - Get table details via get_table_details(catalog, schema)   |
|  - If no table arg: ask user to pick table(s) to profile      |
+---------------------------------------------------------------+
|  STEP 4: Profile Selected Tables                              |
|  - Row counts, column counts, data types                      |
|  - Null rates per column                                      |
|  - Distinct value counts                                      |
|  - Value distributions for categorical columns (TOP 10)       |
|  - Min/max/avg for numeric and date columns                   |
+---------------------------------------------------------------+
|  STEP 5: Discover Relationships                               |
|  - Identify FK patterns (_id, _key suffixes)                  |
|  - Find shared column names across tables in the schema       |
|  - Suggest join candidates with join key recommendations      |
+---------------------------------------------------------------+
|  STEP 6: Summarize Findings                                   |
|  - Present structured exploration report                      |
|  - Table overview with key metrics                            |
|  - Data quality notes (high null rates, low cardinality)      |
|  - Suggested joins and relationship diagram                   |
+---------------------------------------------------------------+
```

## Step-by-Step Instructions

### Step 1: Connect & Discover Catalogs

First, get an available warehouse:

```
get_best_warehouse
```

If the user did **not** provide a catalog argument, list available catalogs:

```sql
SHOW CATALOGS
```

Present the catalog list and ask the user to choose one. If the user provided a catalog argument, skip directly to Step 2.

### Step 2: Explore Schemas

List schemas in the chosen catalog:

```sql
SHOW SCHEMAS IN <catalog>
```

If the user did **not** provide a schema argument, present the list and ask them to choose. Otherwise, proceed with the provided schema.

### Step 3: Explore Tables

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

### Step 4: Profile Selected Tables

For each selected table, run profiling queries via `execute_sql`:

**Row count and basic stats:**

```sql
SELECT
  COUNT(*) AS row_count,
  COUNT(*) - COUNT(col1) AS col1_nulls,
  COUNT(DISTINCT col1) AS col1_distinct,
  COUNT(*) - COUNT(col2) AS col2_nulls,
  COUNT(DISTINCT col2) AS col2_distinct
FROM <catalog>.<schema>.<table>
```

**Numeric column stats (for each numeric column):**

```sql
SELECT
  MIN(<col>) AS min_val,
  MAX(<col>) AS max_val,
  AVG(<col>) AS avg_val,
  PERCENTILE(<col>, 0.5) AS median_val
FROM <catalog>.<schema>.<table>
```

**Date column stats (for each date/timestamp column):**

```sql
SELECT
  MIN(<col>) AS earliest,
  MAX(<col>) AS latest,
  DATEDIFF(MAX(<col>), MIN(<col>)) AS span_days
FROM <catalog>.<schema>.<table>
```

**Value distributions (for categorical columns with <100 distinct values):**

```sql
SELECT <col>, COUNT(*) AS cnt
FROM <catalog>.<schema>.<table>
GROUP BY <col>
ORDER BY cnt DESC
LIMIT 10
```

Present results in a structured profile:

| Column | Type | Nulls | Null % | Distinct | Min | Max | Top Value |
|--------|------|-------|--------|----------|-----|-----|-----------|
| order_id | BIGINT | 0 | 0% | 50000 | 1 | 50000 | - |
| status | STRING | 12 | 0.02% | 5 | - | - | completed (80%) |

### Step 5: Discover Relationships

Analyze column names across all tables in the schema to find join candidates:

1. **FK pattern detection** -- Look for columns ending in `_id`, `_key`, `_code`, or matching `<other_table>_id` patterns
2. **Shared column names** -- Find columns that appear in multiple tables (e.g., `customer_id` in both `orders` and `customers`)
3. **Name similarity** -- Identify columns with similar names across tables that could be join keys

Present relationship findings:

| Table A | Column | Table B | Column | Relationship Type |
|---------|--------|---------|--------|-------------------|
| orders | customer_id | customers | id | FK candidate (naming pattern) |
| orders | product_id | products | id | FK candidate (naming pattern) |
| orders | region | shipments | region | Shared column name |

**Suggest join queries** for the most likely relationships:

```sql
-- orders <-> customers (via customer_id = id)
SELECT o.*, c.name, c.email
FROM <catalog>.<schema>.orders o
JOIN <catalog>.<schema>.customers c ON o.customer_id = c.id
LIMIT 5
```

### Step 6: Summarize Findings

Present a final exploration report with these sections:

**1. Schema Overview**
- Catalog, schema, number of tables
- Total rows across all profiled tables

**2. Table Profiles**
- Per-table summary: row count, column count, key columns, date range
- Data quality flags: columns with >10% nulls, constant columns, high-cardinality strings

**3. Relationships**
- Confirmed/suggested joins between tables
- Entity-relationship summary

**4. Data Quality Notes**
- Columns with high null rates
- Potential data issues (e.g., future dates, negative values where unexpected)
- Recommendations for data cleanup

**5. Next Steps**
- Suggest follow-up actions: build a dashboard (`/databricks-aibi-dashboards`), create a pipeline, run deeper analysis

## Available MCP Tools

| Tool | Usage |
|------|-------|
| `get_best_warehouse` | Get an available SQL warehouse ID |
| `execute_sql` | Run SQL queries for discovery and profiling |
| `get_table_details` | Get table schemas, column types, and metadata |

## Tips

- **Start broad, narrow down** -- If the user doesn't know what they're looking for, start with catalogs and guide them through
- **Batch profiling queries** -- Combine multiple column stats into a single query where possible to reduce round trips
- **Respect permissions** -- Some catalogs/schemas may not be accessible; handle errors gracefully and suggest alternatives
- **Large tables** -- For tables with >1M rows, use `TABLESAMPLE` or `LIMIT` for distribution queries to avoid long-running scans
- **Use fully-qualified names** -- Always reference tables as `catalog.schema.table`

## Related Skills

- **[databricks-unity-catalog](../../databricks-skills/databricks-unity-catalog/SKILL.md)** -- For deeper Unity Catalog operations, system tables, and volumes
- **[databricks-aibi-dashboards](../../databricks-skills/databricks-aibi-dashboards/SKILL.md)** -- For building dashboards on explored data
- **[databricks-synthetic-data-gen](../../databricks-skills/databricks-synthetic-data-gen/SKILL.md)** -- For generating test data based on discovered schemas
