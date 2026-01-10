"""Authentication context for Databricks WorkspaceClient.

Uses Python contextvars to pass authentication through the async call stack
without threading parameters through every function.

Usage in FastAPI:
    # In request handler or middleware
    set_databricks_auth(host, token)
    try:
        # Any code here can call get_workspace_client()
        result = some_databricks_function()
    finally:
        clear_databricks_auth()

Usage in MCP server:
    from databricks_tools_core.auth import set_databricks_config

    # Set config at startup
    set_databricks_config(profile="my-profile", cluster_id="abc123", warehouse_id="xyz789")

Usage in functions:
    from databricks_tools_core.auth import get_workspace_client

    def my_function():
        client = get_workspace_client()  # Uses context auth or env vars
        # ...
"""

from contextvars import ContextVar
from typing import Optional

from databricks.sdk import WorkspaceClient

# Context variables for per-request authentication
_host_ctx: ContextVar[Optional[str]] = ContextVar('databricks_host', default=None)
_token_ctx: ContextVar[Optional[str]] = ContextVar('databricks_token', default=None)

# Context variables for MCP server configuration
_profile_ctx: ContextVar[Optional[str]] = ContextVar('databricks_profile', default=None)
_cluster_id_ctx: ContextVar[Optional[str]] = ContextVar('default_cluster_id', default=None)
_warehouse_id_ctx: ContextVar[Optional[str]] = ContextVar('default_warehouse_id', default=None)
_serverless_ctx: ContextVar[Optional[bool]] = ContextVar('serverless', default=None)


def set_databricks_auth(host: Optional[str], token: Optional[str]) -> None:
    """Set Databricks authentication for the current async context.

    Call this at the start of a request to set per-user credentials.
    The credentials will be used by all get_workspace_client() calls
    within this async context.

    Args:
        host: Databricks workspace URL (e.g., https://xxx.cloud.databricks.com)
        token: Databricks access token
    """
    _host_ctx.set(host)
    _token_ctx.set(token)


def clear_databricks_auth() -> None:
    """Clear Databricks authentication from the current context.

    Call this at the end of a request to clean up.
    """
    _host_ctx.set(None)
    _token_ctx.set(None)


def set_databricks_config(
    profile: Optional[str] = None,
    cluster_id: Optional[str] = None,
    warehouse_id: Optional[str] = None,
    serverless: Optional[bool] = None,
) -> None:
    """Set Databricks configuration for the MCP server.

    Call this at server startup to configure default profile and resource IDs.
    These values will be used as defaults by tools.

    Args:
        profile: Databricks config profile name from ~/.databrickscfg
        cluster_id: Default cluster ID for compute operations
        warehouse_id: Default warehouse ID for SQL operations
        serverless: Whether to use serverless compute (True/False)
    """
    _profile_ctx.set(profile)
    _cluster_id_ctx.set(cluster_id)
    _warehouse_id_ctx.set(warehouse_id)
    _serverless_ctx.set(serverless)


def get_default_cluster_id() -> Optional[str]:
    """Get the default cluster ID from config.

    Returns:
        Cluster ID string if configured, None otherwise
    """
    return _cluster_id_ctx.get()


def get_default_warehouse_id() -> Optional[str]:
    """Get the default warehouse ID from config.

    Returns:
        Warehouse ID string if configured, None otherwise
    """
    return _warehouse_id_ctx.get()


def get_default_serverless() -> Optional[bool]:
    """Get the default serverless setting from config.

    Returns:
        Serverless boolean if configured, None otherwise
    """
    return _serverless_ctx.get()


def get_workspace_client() -> WorkspaceClient:
    """Get a WorkspaceClient using context auth or environment variables.

    Authentication priority:
    1. Context variables (set via set_databricks_auth)
    2. Profile from config (set via set_databricks_config)
    3. Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN)
    4. Databricks config file (~/.databrickscfg)

    Returns:
        Configured WorkspaceClient instance
    """
    host = _host_ctx.get()
    token = _token_ctx.get()
    cluster_id = _cluster_id_ctx.get()
    serverless = _serverless_ctx.get()

    if host and token:
        return WorkspaceClient(host=host, token=token, cluster_id=cluster_id)

    # Check if profile is configured
    profile = _profile_ctx.get()
    if profile:
        return WorkspaceClient(profile=profile, cluster_id=cluster_id)

    # Fall back to default authentication (env vars, config file)
    return WorkspaceClient(cluster_id=cluster_id)
