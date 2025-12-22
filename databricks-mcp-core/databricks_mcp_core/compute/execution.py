"""
Compute - Execution Context Operations

Functions for executing code on Databricks clusters using execution contexts.
Uses Databricks Command Execution API 1.2.
"""
import time
from typing import Dict, Any, Optional
from ..client import DatabricksClient


class ExecutionResult:
    """Result from code execution"""

    def __init__(self, success: bool, output: Optional[str] = None, error: Optional[str] = None):
        self.success = success
        self.output = output
        self.error = error

    def __repr__(self):
        if self.success:
            return f"ExecutionResult(success=True, output={repr(self.output)})"
        return f"ExecutionResult(success=False, error={repr(self.error)})"


def create_context(client: DatabricksClient, cluster_id: str, language: str = "python") -> str:
    """
    Create a new execution context on a Databricks cluster.

    Args:
        client: Databricks client instance
        cluster_id: ID of the cluster to create context on
        language: Programming language ("python", "scala", "sql", "r")

    Returns:
        Context ID string

    Raises:
        requests.HTTPError: If API request fails
    """
    response = client.post(
        "/api/1.2/contexts/create",
        json={"clusterId": cluster_id, "language": language}
    )
    return response["id"]


def execute_command_with_context(
    client: DatabricksClient,
    cluster_id: str,
    context_id: str,
    code: str,
    timeout: int = 120
) -> ExecutionResult:
    """
    Execute code using an existing execution context (maintains state between calls).

    Args:
        client: Databricks client instance
        cluster_id: ID of the cluster
        context_id: ID of the execution context
        code: Code to execute
        timeout: Maximum time to wait for execution (seconds)

    Returns:
        ExecutionResult with output or error

    Raises:
        requests.HTTPError: If API request fails
    """
    # Submit command
    response = client.post(
        "/api/1.2/commands/execute",
        json={
            "clusterId": cluster_id,
            "contextId": context_id,
            "language": "python",
            "command": code
        }
    )
    command_id = response["id"]

    # Poll for result
    start_time = time.time()
    while True:
        status_response = client.get(
            "/api/1.2/commands/status",
            params={
                "clusterId": cluster_id,
                "contextId": context_id,
                "commandId": command_id
            }
        )

        status = status_response.get("status")
        if status in ["Finished", "Error", "Cancelled"]:
            break

        if time.time() - start_time > timeout:
            return ExecutionResult(success=False, error="Command timed out")

        time.sleep(2)

    # Handle results
    results = status_response.get("results", {})
    result_type = results.get("resultType", "")

    if status == "Error" or result_type == "error":
        error_msg = results.get("cause", results.get("summary", "Unknown error"))
        return ExecutionResult(success=False, error=error_msg)

    result_data = results.get("data")
    output = str(result_data) if result_data else "Success (no output)"
    return ExecutionResult(success=True, output=output)


def destroy_context(client: DatabricksClient, cluster_id: str, context_id: str) -> None:
    """
    Destroy an execution context.

    Args:
        client: Databricks client instance
        cluster_id: ID of the cluster
        context_id: ID of the context to destroy

    Raises:
        requests.HTTPError: If API request fails
    """
    client.post(
        "/api/1.2/contexts/destroy",
        json={"clusterId": cluster_id, "contextId": context_id}
    )


def execute_databricks_command(
    client: DatabricksClient,
    cluster_id: str,
    language: str,
    code: str,
    timeout: int = 120
) -> ExecutionResult:
    """
    Execute code on Databricks cluster (creates and destroys context automatically).

    This is a convenience function for one-off command execution. For multiple
    commands that need to maintain state, use create_context() and
    execute_command_with_context() explicitly.

    Args:
        client: Databricks client instance
        cluster_id: ID of the cluster
        language: Programming language ("python", "scala", "sql", "r")
        code: Code to execute
        timeout: Maximum time to wait for execution (seconds)

    Returns:
        ExecutionResult with output or error

    Raises:
        requests.HTTPError: If API request fails
    """
    # Create execution context
    response = client.post(
        "/api/1.2/contexts/create",
        json={"clusterId": cluster_id, "language": language}
    )
    context_id = response["id"]

    try:
        # Submit command
        cmd_response = client.post(
            "/api/1.2/commands/execute",
            json={
                "clusterId": cluster_id,
                "contextId": context_id,
                "language": language,
                "command": code
            }
        )
        command_id = cmd_response["id"]

        # Poll for result
        start_time = time.time()
        while True:
            status_response = client.get(
                "/api/1.2/commands/status",
                params={
                    "clusterId": cluster_id,
                    "contextId": context_id,
                    "commandId": command_id
                }
            )

            status = status_response.get("status")
            if status in ["Finished", "Error", "Cancelled"]:
                break

            if time.time() - start_time > timeout:
                return ExecutionResult(success=False, error="Command timed out")

            time.sleep(2)

        # Handle results
        results = status_response.get("results", {})
        result_type = results.get("resultType", "")

        if status == "Error" or result_type == "error":
            error_msg = results.get("cause", results.get("summary", "Unknown error"))
            return ExecutionResult(success=False, error=error_msg)

        result_data = results.get("data")
        output = str(result_data) if result_data else "Success (no output)"
        return ExecutionResult(success=True, output=output)

    finally:
        # Always clean up context
        try:
            destroy_context(client, cluster_id, context_id)
        except:
            pass  # Ignore cleanup errors
