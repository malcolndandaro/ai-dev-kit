"""Compute tools - Execute code on Databricks clusters."""
from typing import Dict, Any, List, Optional

from databricks_tools_core.compute import (
    execute_databricks_command as _execute_databricks_command,
    run_python_file_on_databricks as _run_python_file_on_databricks,
    list_clusters as _list_clusters,
    get_best_cluster as _get_best_cluster,
)
from databricks_tools_core.auth import get_default_cluster_id

from ..server import mcp


def _resolve_cluster_id(cluster_id: Optional[str]) -> str:
    """Resolve cluster ID using config default or provided value.

    If a default cluster_id is configured via MCP args, it takes precedence.
    Otherwise, uses the provided cluster_id or raises an error.
    """
    # Config default takes precedence (strict override)
    default_id = get_default_cluster_id()
    if default_id:
        return default_id

    if cluster_id:
        return cluster_id

    raise ValueError(
        "No cluster_id provided and no default configured. "
        "Either pass cluster_id or configure --cluster-id in MCP server args."
    )


@mcp.tool
def list_clusters(
    limit: int = 20,
    running_only: bool = False,
    shared_only: bool = False,
    cluster_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    List clusters in the workspace with optional filtering, or fetch a specific cluster by ID.

    Optimized for large workspaces with early termination.
    RUNNING clusters are always listed first.

    Args:
        limit: Maximum number of clusters to return (default: 20)
        running_only: If True, only return RUNNING clusters (faster in large workspaces)
        shared_only: If True, only return shared/standard access mode clusters (not single-user)
        cluster_id: If provided, fetch only this specific cluster by ID (ignores other filters)

    Returns:
        Dictionary with 'result' containing list of cluster info dicts including:
        - id, name, state, spark_version, node_type_id
        - single_user_name (None for shared clusters)
        - data_security_mode
    """
    return {"result": _list_clusters(limit=limit, running_only=running_only, shared_only=shared_only, cluster_id=cluster_id)}


@mcp.tool
def get_best_cluster(
    running_only: bool = True,
    name_filter: Optional[str] = None,
) -> Optional[str]:
    """
    Get the ID of the best available cluster.

    Prioritizes shared/standard access mode clusters (not single-user assigned).
    Optimized for large workspaces with early termination.

    Priority:
    1. Running shared-access cluster matching name_filter exactly
    2. Running shared-access cluster with name_filter in name
    3. Any running shared-access cluster
    4. Running single-user cluster (fallback)
    5. Stopped clusters (only if running_only=False)

    Args:
        running_only: If True (default), only consider RUNNING clusters
        name_filter: Custom name to search for in cluster names (case-insensitive).
                    Use this to find a specific cluster by name.
                    If None, defaults to searching for 'shared'.

    Returns:
        Cluster ID string, or None if no clusters available.
    """
    return _get_best_cluster(running_only=running_only, name_filter=name_filter)


@mcp.tool
def execute_databricks_command(
    language: str,
    code: str,
    cluster_id: Optional[str] = None,
    timeout: int = 120,
) -> Dict[str, Any]:
    """
    Execute code on a Databricks cluster.

    Creates an execution context, runs the code, and cleans up automatically.

    If a default cluster_id is configured via MCP server args (--cluster-id),
    it will be used automatically and the cluster_id parameter is optional.

    Args:
        language: Programming language ("python", "scala", "sql", "r")
        code: Code to execute
        cluster_id: ID of the cluster to run on (optional if default configured)
        timeout: Maximum wait time in seconds (default: 120)

    Returns:
        Dictionary with success status and output or error message.
    """
    resolved_cluster_id = _resolve_cluster_id(cluster_id)

    result = _execute_databricks_command(
        cluster_id=resolved_cluster_id,
        language=language,
        code=code,
        timeout=timeout,
    )
    return {
        "success": result.success,
        "output": result.output,
        "error": result.error,
    }


@mcp.tool
def run_python_file_on_databricks(
    file_path: str,
    cluster_id: Optional[str] = None,
    timeout: int = 600,
) -> Dict[str, Any]:
    """
    Read a local Python file and execute it on a Databricks cluster.

    Useful for running data generation scripts or other Python code.

    If a default cluster_id is configured via MCP server args (--cluster-id),
    it will be used automatically and the cluster_id parameter is optional.

    Args:
        file_path: Local path to the Python file
        cluster_id: ID of the cluster to run on (optional if default configured)
        timeout: Maximum wait time in seconds (default: 600)

    Returns:
        Dictionary with success status and output or error message.
    """
    resolved_cluster_id = _resolve_cluster_id(cluster_id)

    result = _run_python_file_on_databricks(
        cluster_id=resolved_cluster_id,
        file_path=file_path,
        timeout=timeout,
    )
    return {
        "success": result.success,
        "output": result.output,
        "error": result.error,
    }
