"""
Unity Catalog - System Table Queries

Parameterized queries for system tables (audit, lineage, billing, compute, etc.).
Enforces date partition filters and row limits for performance.
"""
import re
from typing import Any, Dict, List, Optional

_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.\-]*$")

# System table configurations: (table_name, date_column_or_None)
_SYSTEM_TABLES = {
    "audit": ("system.access.audit", "event_date"),
    "table_lineage": ("system.access.table_lineage", "event_time"),
    "column_lineage": ("system.access.column_lineage", "event_time"),
    "billing_usage": ("system.billing.usage", "usage_date"),
    "billing_prices": ("system.billing.list_prices", None),
    "clusters": ("system.compute.clusters", "change_time"),
    "warehouse_events": ("system.compute.warehouse_events", "event_time"),
    "jobs": ("system.lakeflow.jobs", None),
    "job_runs": ("system.lakeflow.job_run_timeline", "period_start_time"),
    "pipeline_events": ("system.lakeflow.pipeline_events", "timestamp"),
    "query_history": ("system.query.history", "start_time"),
}


def _validate_identifier(name: str) -> str:
    """Validate a SQL identifier to prevent injection."""
    if not _IDENTIFIER_PATTERN.match(name):
        raise ValueError(f"Invalid SQL identifier: '{name}'")
    return name


def _execute_uc_sql(sql_query: str, warehouse_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Execute SQL using the existing execute_sql infrastructure."""
    from ..sql.sql import execute_sql
    return execute_sql(sql_query=sql_query, warehouse_id=warehouse_id)


def query_system_table(
    system_table: str,
    days_back: int = 7,
    limit: int = 100,
    table_name: Optional[str] = None,
    user_email: Optional[str] = None,
    action_name: Optional[str] = None,
    job_id: Optional[str] = None,
    pipeline_id: Optional[str] = None,
    warehouse_id: Optional[str] = None,
    custom_where: Optional[str] = None,
    custom_columns: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Query a Unity Catalog system table with appropriate filters.

    Automatically applies date partition filters for performance.

    Args:
        system_table: System table to query. Valid values:
            - "audit": Access audit logs (system.access.audit)
            - "table_lineage": Table-level lineage (system.access.table_lineage)
            - "column_lineage": Column-level lineage (system.access.column_lineage)
            - "billing_usage": DBU usage data (system.billing.usage)
            - "billing_prices": List prices (system.billing.list_prices)
            - "clusters": Cluster configurations (system.compute.clusters)
            - "warehouse_events": SQL warehouse events (system.compute.warehouse_events)
            - "jobs": Job definitions (system.lakeflow.jobs)
            - "job_runs": Job run history (system.lakeflow.job_run_timeline)
            - "pipeline_events": Pipeline events (system.lakeflow.pipeline_events)
            - "query_history": Query execution history (system.query.history)
        days_back: Number of days to look back (default: 7). Used for date partition filter.
        limit: Maximum rows to return (default: 100)
        table_name: Filter by table name (for lineage, audit)
        user_email: Filter by user email (for audit, query_history)
        action_name: Filter by action name (for audit)
        job_id: Filter by job ID (for job_runs)
        pipeline_id: Filter by pipeline ID (for pipeline_events)
        warehouse_id: SQL warehouse ID for executing the query
        custom_where: Additional WHERE clause conditions (appended with AND)
        custom_columns: Custom SELECT columns (default: *)

    Returns:
        Dict with "data" (query results), "sql" (executed query), "system_table" name

    Raises:
        ValueError: If system_table is not valid
    """
    if system_table not in _SYSTEM_TABLES:
        raise ValueError(
            f"Invalid system_table: '{system_table}'. "
            f"Valid values: {list(_SYSTEM_TABLES.keys())}"
        )

    table_full_name, date_column = _SYSTEM_TABLES[system_table]
    select_cols = custom_columns if custom_columns else "*"
    conditions = []

    # Date partition filter (critical for performance)
    if date_column:
        if "date" in date_column:
            conditions.append(f"{date_column} >= CURRENT_DATE() - INTERVAL {days_back} DAY")
        else:
            conditions.append(f"{date_column} >= CURRENT_TIMESTAMP() - INTERVAL {days_back} DAY")

    # System-table-specific filters
    if table_name:
        _validate_identifier(table_name)
        if system_table in ("table_lineage", "column_lineage"):
            conditions.append(
                f"(source_table_full_name = '{table_name}' OR target_table_full_name = '{table_name}')"
            )
        elif system_table == "audit":
            conditions.append(
                f"request_params.full_name_arg = '{table_name}'"
            )

    if user_email:
        if system_table == "audit":
            conditions.append(f"user_identity.email = '{user_email}'")
        elif system_table == "query_history":
            conditions.append(f"executed_by = '{user_email}'")

    if action_name and system_table == "audit":
        conditions.append(f"action_name = '{action_name}'")

    if job_id and system_table == "job_runs":
        conditions.append(f"job_id = '{job_id}'")

    if pipeline_id and system_table == "pipeline_events":
        conditions.append(f"pipeline_id = '{pipeline_id}'")

    if custom_where:
        conditions.append(f"({custom_where})")

    where = f"\nWHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT {select_cols}\nFROM {table_full_name}{where}\nORDER BY {date_column + ' DESC' if date_column else '1'}\nLIMIT {limit}"

    results = _execute_uc_sql(sql, warehouse_id=warehouse_id)
    return {
        "system_table": system_table,
        "full_table_name": table_full_name,
        "data": results,
        "row_count": len(results),
        "sql": sql,
    }
