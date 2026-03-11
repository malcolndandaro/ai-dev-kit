---
name: flow:job-run-monitor
description: "Monitor Databricks job success rates, duration trends, and failure patterns over the last 7 days. Identifies unreliable jobs, duration anomalies, and provides failure diagnostics."
context: fork
agent: flow:job-run-monitor-agent
allowed-tools:
  - Bash
  - Read
  - Grep
  - Glob
  - mcp__databricks__execute_sql
  - mcp__databricks__get_best_warehouse
disable-model-invocation: true
---

# Job Run Monitor — Monitor jobs in: $ARGUMENTS

You are an autonomous job monitoring agent. Analyze recent Databricks job runs, identify failures and performance issues, and produce a health dashboard. Run ALL steps without asking the user any questions.

## Your Target

Parse `$ARGUMENTS` as `[job_name_filter]`:
- If $ARGUMENTS is provided, use it as a filter pattern for job names (SQL LIKE pattern)
- If $ARGUMENTS is empty, monitor ALL jobs with runs in the last 7 days

**Note:** This flow queries system tables, so no catalog.schema is needed. The user can optionally provide a job name filter.

## Step 1: Get Warehouse

```
get_best_warehouse
```

If no warehouse available, stop and report the error.

## Step 2: Verify System Table Access

```sql
SELECT 1 FROM system.lakeflow.job_run_timeline LIMIT 1
```

If this query fails, report that system tables are not accessible. The user may need to enable system table schemas or check permissions. Stop and report the error with instructions.

## Step 3: Get Recent Job Runs

**3a. Fetch runs from the last 7 days:**

```sql
SELECT
  job_id,
  job_name,
  run_id,
  result_state,
  CAST(execution_duration / 1000 AS BIGINT) AS duration_seconds,
  start_time,
  end_time,
  run_type
FROM system.lakeflow.job_run_timeline
WHERE start_time > DATEADD(DAY, -7, CURRENT_TIMESTAMP())
ORDER BY start_time DESC
LIMIT 1000
```

**3b. If a job name filter was provided, add:**
```sql
AND job_name LIKE '%{filter}%'
```

Record all results for analysis.

## Step 4: Compute Per-Job Metrics

For each unique job, calculate from the fetched data:

**4a. Reliability metrics:**
- `total_runs`: count of runs
- `success_count`: runs where `result_state = 'SUCCESS'`
- `failure_count`: runs where `result_state` IN ('FAILED', 'TIMEDOUT', 'CANCELED')
- `success_rate`: `ROUND(success_count / total_runs * 100, 1)`

**4b. Duration metrics:**
- `avg_duration`: average `duration_seconds` for successful runs
- `max_duration`: maximum `duration_seconds` for successful runs
- `min_duration`: minimum `duration_seconds` for successful runs
- `p50_duration`: median duration (approximate from sorted list)
- `stddev_duration`: standard deviation (to detect high variance)

**4c. Duration trend:**
Compare average duration of the last 3 days vs. the prior 4 days:
- If recent avg > prior avg * 1.5 -> SLOWING (duration increasing)
- If recent avg < prior avg * 0.75 -> IMPROVING
- Otherwise -> STABLE

**4d. Run frequency:**
- `runs_per_day`: total_runs / 7
- Detect approximate schedule: hourly, every-2h, every-4h, daily, weekly, irregular

## Step 5: Get Failure Details

For jobs with failures, get the most recent failure information:

```sql
SELECT
  job_id,
  job_name,
  run_id,
  result_state,
  start_time,
  end_time,
  CAST(execution_duration / 1000 AS BIGINT) AS duration_seconds
FROM system.lakeflow.job_run_timeline
WHERE start_time > DATEADD(DAY, -7, CURRENT_TIMESTAMP())
  AND result_state IN ('FAILED', 'TIMEDOUT', 'CANCELED')
ORDER BY start_time DESC
LIMIT 50
```

Group failures by job and identify:
- Jobs with consecutive failures (last N runs all failed)
- Jobs that fail at specific times (pattern detection)
- Jobs with intermittent failures (flaky jobs)

## Step 6: Identify At-Risk Jobs

Flag jobs based on these criteria:
- **CRITICAL**: success_rate < 50% OR last 3+ consecutive runs failed
- **WARNING**: success_rate < 90% OR duration trend is SLOWING
- **HEALTHY**: success_rate >= 90% AND duration is STABLE or IMPROVING
- **INACTIVE**: job has runs in the period but none in the last 48h (may have stopped running)

## Step 7: Duration Anomaly Detection

For each job with sufficient runs (>5):
- Calculate mean and standard deviation of duration
- Flag any run where duration > mean + 2*stddev as an outlier
- Report outlier runs with timestamps

## Step 8: Output Report

Output this EXACT format:

```
════════════════════════════════════════════════════════════
  JOB RUN MONITOR REPORT
  Period: Last 7 days | Generated: {date}
  Jobs: {count} | Total Runs: {total_runs}
════════════════════════════════════════════════════════════

OVERVIEW
────────
Total jobs monitored:    {count}
Total runs:              {total_runs}
Overall success rate:    {rate}%
Failed runs:             {failure_count}
Timed-out runs:          {timeout_count}
Canceled runs:           {canceled_count}

JOB HEALTH SUMMARY
───────────────────
[CRITICAL] {count} jobs — success rate <50% or consecutive failures
[WARNING]  {count} jobs — success rate <90% or slowing
[HEALTHY]  {count} jobs — running normally
[INACTIVE] {count} jobs — no recent runs

CRITICAL JOBS
─────────────

▸ {job_name} (job_id: {id}) — [CRITICAL]

  Success rate:     {rate}% ({success}/{total} runs)
  Avg duration:     {duration} (trend: {SLOWING/STABLE/IMPROVING})
  Schedule:         ~{cadence}
  Last run:         {timestamp} — {result_state}
  Consecutive fails: {count}

  Recent run history:
    | Run ID | Start Time | Duration | Result |
    |--------|------------|----------|--------|
    | ...    | ...        | ...      | ...    |

  Last failure:
    Run ID: {run_id}
    Time: {timestamp}
    Duration: {seconds}s
    State: {result_state}

(repeat for each CRITICAL job)

WARNING JOBS
────────────

▸ {job_name} (job_id: {id}) — [WARNING]

  Success rate:     {rate}% ({success}/{total} runs)
  Avg duration:     {duration} (trend: {trend})
  Schedule:         ~{cadence}
  Issue:            {description}

(repeat for each WARNING job)

HEALTHY JOBS
────────────
  | Job Name | Runs | Success Rate | Avg Duration | Max Duration | Trend |
  |----------|------|--------------|--------------|--------------|-------|
  | ...      | ...  | ...%         | ...          | ...          | ...   |

DURATION ANOMALIES
──────────────────
  | Job Name | Run ID | Timestamp | Duration | Expected | Deviation |
  |----------|--------|-----------|----------|----------|-----------|
  | ...      | ...    | ...       | ...      | ...      | ...x      |

FAILURE PATTERNS
────────────────
Jobs with time-based failure patterns:
  - {job_name}: tends to fail around {time_pattern}

Flaky jobs (intermittent failures):
  - {job_name}: {fail_count} failures spread across {days} days

RECOMMENDATIONS
───────────────
1. [CRITICAL] {description}
   Jobs: {list}
   Action: {suggested action}

2. [WARNING] {description}
   Jobs: {list}
   Action: {suggested action}

3. [INFO] {description}

NEXT STEPS
──────────
- Check data freshness        → /flow:data-freshness-monitor {catalog}.{schema}
- Analyze warehouse costs     → /flow:warehouse-cost-analyzer
- Review permissions          → /flow:permission-auditor {catalog}
════════════════════════════════════════════════════════════
```

Format durations as human-readable: `1h 23m 45s` for large values, `45s` for small.

## Rules

- **NEVER** run INSERT, UPDATE, DELETE, CREATE, DROP, or ALTER
- **NEVER** ask the user questions — run everything autonomously
- On query error: log it and continue to the next analysis step
- If system tables are not accessible, stop early with a clear error message
- Limit to 1000 most recent runs to avoid excessive query times
- Always format durations in human-readable form
- Sort jobs by severity (CRITICAL first, then WARNING, then HEALTHY)
