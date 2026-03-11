---
name: flow:warehouse-cost-analyzer
description: "Break down DBU consumption by warehouse, user, and query type over the last 30 days. Identifies the most expensive queries and provides cost optimization recommendations."
context: fork
agent: flow:warehouse-cost-analyzer-agent
allowed-tools:
  - Bash
  - Read
  - Grep
  - Glob
  - mcp__databricks__execute_sql
  - mcp__databricks__get_best_warehouse
disable-model-invocation: true
---

# Warehouse Cost Analyzer — Analyze costs for: $ARGUMENTS

You are an autonomous cost analysis agent. Break down SQL warehouse DBU consumption, identify expensive queries, and produce actionable optimization recommendations. Run ALL steps without asking the user any questions.

## Your Target

Parse `$ARGUMENTS` as `[warehouse_name_filter]`:
- If $ARGUMENTS is provided, use it as a filter pattern for warehouse names
- If $ARGUMENTS is empty, analyze ALL SQL warehouses

**Note:** This flow queries system tables, so no catalog.schema is needed. The user can optionally filter by warehouse name.

## Step 1: Get Warehouse

```
get_best_warehouse
```

If no warehouse available, stop and report the error.

## Step 2: Verify System Table Access

```sql
SELECT 1 FROM system.billing.usage LIMIT 1
```

If this query fails, report that system billing tables are not accessible and stop with instructions on how to enable them.

Also verify query history access:
```sql
SELECT 1 FROM system.query.history LIMIT 1
```

Note if query history is not accessible (some analysis will be skipped).

## Step 3: Get Billing Usage Data

**3a. Overall SQL warehouse usage (last 30 days):**

```sql
SELECT
  sku_name,
  usage_unit,
  ROUND(SUM(usage_quantity), 2) AS total_dbus,
  COUNT(DISTINCT usage_date) AS active_days,
  ROUND(SUM(usage_quantity) / COUNT(DISTINCT usage_date), 2) AS avg_daily_dbus
FROM system.billing.usage
WHERE usage_date > DATEADD(DAY, -30, CURRENT_DATE())
  AND sku_name LIKE '%SQL%'
GROUP BY sku_name, usage_unit
ORDER BY total_dbus DESC
```

**3b. Daily trend:**

```sql
SELECT
  usage_date,
  sku_name,
  ROUND(SUM(usage_quantity), 2) AS daily_dbus
FROM system.billing.usage
WHERE usage_date > DATEADD(DAY, -30, CURRENT_DATE())
  AND sku_name LIKE '%SQL%'
GROUP BY usage_date, sku_name
ORDER BY usage_date DESC
```

**3c. If a warehouse name filter was provided, add:**
```sql
AND usage_metadata.warehouse_id IS NOT NULL
```
And then cross-reference with warehouse names from query history.

## Step 4: Analyze Query History

**4a. Top 20 most expensive queries by duration:**

```sql
SELECT
  statement_id,
  statement_type,
  CAST(total_duration_ms / 1000 AS BIGINT) AS duration_seconds,
  rows_produced,
  bytes_read,
  executed_by,
  start_time,
  warehouse_id,
  LEFT(statement_text, 200) AS query_preview
FROM system.query.history
WHERE start_time > DATEADD(DAY, -30, CURRENT_TIMESTAMP())
ORDER BY total_duration_ms DESC
LIMIT 20
```

**4b. Usage by user:**

```sql
SELECT
  executed_by,
  COUNT(*) AS query_count,
  ROUND(SUM(total_duration_ms) / 1000 / 3600, 2) AS total_hours,
  ROUND(AVG(total_duration_ms) / 1000, 1) AS avg_duration_seconds,
  SUM(bytes_read) AS total_bytes_read,
  SUM(rows_produced) AS total_rows_produced
FROM system.query.history
WHERE start_time > DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY executed_by
ORDER BY total_hours DESC
LIMIT 30
```

**4c. Usage by statement type:**

```sql
SELECT
  statement_type,
  COUNT(*) AS query_count,
  ROUND(SUM(total_duration_ms) / 1000 / 3600, 2) AS total_hours,
  ROUND(AVG(total_duration_ms) / 1000, 1) AS avg_duration_seconds,
  SUM(bytes_read) AS total_bytes_read
FROM system.query.history
WHERE start_time > DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY statement_type
ORDER BY total_hours DESC
```

**4d. Usage by warehouse:**

```sql
SELECT
  warehouse_id,
  COUNT(*) AS query_count,
  ROUND(SUM(total_duration_ms) / 1000 / 3600, 2) AS total_hours,
  ROUND(AVG(total_duration_ms) / 1000, 1) AS avg_duration_seconds,
  COUNT(DISTINCT executed_by) AS unique_users,
  COUNT(DISTINCT DATE(start_time)) AS active_days
FROM system.query.history
WHERE start_time > DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY warehouse_id
ORDER BY total_hours DESC
```

## Step 5: Detect Repeated Queries

Identify queries that run frequently with identical text (potential caching or materialization opportunities):

```sql
SELECT
  LEFT(statement_text, 200) AS query_preview,
  COUNT(*) AS execution_count,
  ROUND(SUM(total_duration_ms) / 1000, 1) AS total_seconds,
  ROUND(AVG(total_duration_ms) / 1000, 1) AS avg_seconds,
  COUNT(DISTINCT executed_by) AS unique_users
FROM system.query.history
WHERE start_time > DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  AND statement_type = 'SELECT'
GROUP BY LEFT(statement_text, 200)
HAVING COUNT(*) > 10
ORDER BY total_seconds DESC
LIMIT 10
```

## Step 6: Identify Cost Optimization Opportunities

Analyze the collected data to find:

**6a. Long-running queries:**
- Queries taking >5 minutes that could benefit from optimization
- Look for full table scans (high bytes_read relative to rows_produced)

**6b. Repeated identical queries:**
- Queries running >10 times with the same text — candidates for materialized views or caching

**6c. Off-hours usage:**
```sql
SELECT
  HOUR(start_time) AS hour_of_day,
  COUNT(*) AS query_count,
  ROUND(SUM(total_duration_ms) / 1000 / 3600, 2) AS total_hours
FROM system.query.history
WHERE start_time > DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY HOUR(start_time)
ORDER BY hour_of_day
```
Identify low-usage hours where warehouses could be scaled down or auto-stopped.

**6d. Weekend vs. weekday patterns:**
- Compare weekday vs. weekend usage to identify potential auto-stop savings

## Step 7: Output Report

Output this EXACT format:

```
════════════════════════════════════════════════════════════
  WAREHOUSE COST ANALYSIS REPORT
  Period: Last 30 days | Generated: {date}
════════════════════════════════════════════════════════════

COST OVERVIEW
─────────────
Total SQL DBUs consumed:     {total_dbus}
Active days:                 {days}
Avg daily DBU consumption:   {avg_daily}
Warehouse SKUs active:       {count}

DBU BREAKDOWN BY SKU
────────────────────
  | SKU Name | Total DBUs | Avg Daily | Active Days | % of Total |
  |----------|------------|-----------|-------------|------------|
  | ...      | ...        | ...       | ...         | ...%       |

DAILY TREND (last 14 days)
──────────────────────────
  | Date | Total DBUs | vs. Avg |
  |------|------------|---------|
  | ...  | ...        | +/-...% |

(Show last 14 days, flag days that are >50% above average)

TOP WAREHOUSES BY USAGE
───────────────────────
  | Warehouse ID | Queries | Total Hours | Avg Duration | Users | Active Days |
  |--------------|---------|-------------|--------------|-------|-------------|
  | ...          | ...     | ...         | ...          | ...   | ...         |

TOP USERS BY CONSUMPTION
────────────────────────
  | User | Queries | Total Hours | Avg Duration | Bytes Read | Rows Produced |
  |------|---------|-------------|--------------|------------|---------------|
  | ...  | ...     | ...         | ...          | ...        | ...           |

USAGE BY QUERY TYPE
───────────────────
  | Statement Type | Queries | Total Hours | Avg Duration | Bytes Read |
  |----------------|---------|-------------|--------------|------------|
  | SELECT         | ...     | ...         | ...          | ...        |
  | INSERT         | ...     | ...         | ...          | ...        |
  | ...            | ...     | ...         | ...          | ...        |

TOP 20 MOST EXPENSIVE QUERIES
──────────────────────────────

▸ Query #{rank} — {duration} | {bytes_read} read | by {user}
  Type: {statement_type}
  Warehouse: {warehouse_id}
  Time: {start_time}
  Preview: {first 200 chars of query}

(repeat for top 20)

REPEATED QUERIES (optimization candidates)
───────────────────────────────────────────
  | Executions | Total Time | Avg Time | Users | Query Preview |
  |------------|------------|----------|-------|---------------|
  | ...        | ...        | ...      | ...   | ...           |

  These queries run frequently and may benefit from:
  - Materialized views
  - Result caching
  - Scheduled pre-computation

HOURLY USAGE PATTERN
────────────────────
  | Hour | Queries | Total Hours | Notes |
  |------|---------|-------------|-------|
  | 00   | ...     | ...         | ...   |
  | ...  | ...     | ...         | ...   |
  | 23   | ...     | ...         | ...   |

  Low-usage windows: {hours} — consider auto-stop or scaling down

OPTIMIZATION RECOMMENDATIONS
─────────────────────────────
1. [HIGH] {description — e.g., "5 queries consume 60% of total duration"}
   Estimated savings: {estimate}
   Action: {specific recommendation}

2. [MEDIUM] {description}
   Estimated savings: {estimate}
   Action: {recommendation}

3. [LOW] {description}
   Action: {recommendation}

NEXT STEPS
──────────
- Monitor job health           → /flow:job-run-monitor
- Audit data freshness         → /flow:data-freshness-monitor {catalog}.{schema}
- Review access permissions    → /flow:permission-auditor {catalog}
════════════════════════════════════════════════════════════
```

Format bytes as human-readable: KB/MB/GB/TB. Format durations as `Xh Ym Zs`.

## Rules

- **NEVER** run INSERT, UPDATE, DELETE, CREATE, DROP, or ALTER
- **NEVER** ask the user questions — run everything autonomously
- On query error: log it and continue to the next analysis step
- If system tables are not accessible, stop early with a clear error message explaining prerequisites
- Limit query history results to avoid excessive data transfer
- Truncate query text to 200 characters in output to keep the report readable
- Always format large numbers with human-readable units (K, M, G for bytes; h, m, s for time)
- Sort all rankings by impact (highest consumption first)
