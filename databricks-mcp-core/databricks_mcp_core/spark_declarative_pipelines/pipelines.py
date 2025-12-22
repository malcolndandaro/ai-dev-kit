"""
Spark Declarative Pipelines - Pipeline Management

Functions for managing SDP pipeline lifecycle using Databricks Pipelines API.
"""
from typing import Dict, Any, List, Optional
from ..client import DatabricksClient


def create_pipeline(
    client: DatabricksClient,
    name: str,
    storage: str,
    target: str,
    libraries: List[Dict[str, Any]],
    clusters: Optional[List[Dict[str, Any]]] = None,
    configuration: Optional[Dict[str, str]] = None,
    continuous: bool = False
) -> Dict[str, Any]:
    """
    Create a new Spark Declarative Pipeline.

    Args:
        client: Databricks client instance
        name: Pipeline name
        storage: Storage location for pipeline data
        target: Target catalog.schema for output tables
        libraries: List of notebook/file paths
                   Example: [{"notebook": {"path": "/path/to/file.py"}}]
        clusters: Optional cluster configuration
        configuration: Optional Spark configuration key-value pairs
        continuous: If True, pipeline runs continuously

    Returns:
        Dictionary with pipeline metadata including pipeline_id

    Raises:
        requests.HTTPError: If API request fails
    """
    payload = {
        "name": name,
        "storage": storage,
        "target": target,
        "libraries": libraries,
        "continuous": continuous
    }

    if clusters:
        payload["clusters"] = clusters
    if configuration:
        payload["configuration"] = configuration

    return client.post("/api/2.0/pipelines", json=payload)


def get_pipeline(client: DatabricksClient, pipeline_id: str) -> Dict[str, Any]:
    """
    Get pipeline details and configuration.

    Args:
        client: Databricks client instance
        pipeline_id: Pipeline ID

    Returns:
        Dictionary with full pipeline configuration and state

    Raises:
        requests.HTTPError: If API request fails
    """
    return client.get(f"/api/2.0/pipelines/{pipeline_id}")


def update_pipeline(
    client: DatabricksClient,
    pipeline_id: str,
    name: Optional[str] = None,
    storage: Optional[str] = None,
    target: Optional[str] = None,
    libraries: Optional[List[Dict[str, Any]]] = None,
    clusters: Optional[List[Dict[str, Any]]] = None,
    configuration: Optional[Dict[str, str]] = None,
    continuous: Optional[bool] = None
) -> Dict[str, Any]:
    """
    Update pipeline configuration (not code files).

    Args:
        client: Databricks client instance
        pipeline_id: Pipeline ID
        name: New pipeline name
        storage: New storage location
        target: New target catalog.schema
        libraries: New library paths
        clusters: New cluster configuration
        configuration: New Spark configuration
        continuous: New continuous mode setting

    Returns:
        Dictionary with updated pipeline metadata

    Raises:
        requests.HTTPError: If API request fails
    """
    payload = {}

    if name is not None:
        payload["name"] = name
    if storage is not None:
        payload["storage"] = storage
    if target is not None:
        payload["target"] = target
    if libraries is not None:
        payload["libraries"] = libraries
    if clusters is not None:
        payload["clusters"] = clusters
    if configuration is not None:
        payload["configuration"] = configuration
    if continuous is not None:
        payload["continuous"] = continuous

    return client.put(f"/api/2.0/pipelines/{pipeline_id}", json=payload)


def delete_pipeline(client: DatabricksClient, pipeline_id: str) -> None:
    """
    Delete a pipeline.

    Args:
        client: Databricks client instance
        pipeline_id: Pipeline ID

    Raises:
        requests.HTTPError: If API request fails
    """
    client.delete(f"/api/2.0/pipelines/{pipeline_id}")


def start_update(
    client: DatabricksClient,
    pipeline_id: str,
    refresh_selection: Optional[List[str]] = None,
    full_refresh: bool = False,
    full_refresh_selection: Optional[List[str]] = None,
    validate_only: bool = False
) -> str:
    """
    Start a pipeline update or dry-run validation.

    Args:
        client: Databricks client instance
        pipeline_id: Pipeline ID
        refresh_selection: List of table names to refresh
        full_refresh: If True, performs full refresh
        full_refresh_selection: List of table names for full refresh
        validate_only: If True, performs dry-run validation without updating datasets

    Returns:
        Update ID for polling status

    Raises:
        requests.HTTPError: If API request fails
    """
    payload = {
        "full_refresh": full_refresh,
        "validate_only": validate_only
    }

    if refresh_selection:
        payload["refresh_selection"] = refresh_selection
    if full_refresh_selection:
        payload["full_refresh_selection"] = full_refresh_selection

    response = client.post(f"/api/2.0/pipelines/{pipeline_id}/updates", json=payload)
    return response["update_id"]


def get_update(
    client: DatabricksClient,
    pipeline_id: str,
    update_id: str
) -> Dict[str, Any]:
    """
    Get pipeline update status and results.

    Args:
        client: Databricks client instance
        pipeline_id: Pipeline ID
        update_id: Update ID from start_update

    Returns:
        Dictionary with update status (state: QUEUED, RUNNING, COMPLETED, FAILED, etc.)

    Raises:
        requests.HTTPError: If API request fails
    """
    return client.get(f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}")


def stop_pipeline(client: DatabricksClient, pipeline_id: str) -> None:
    """
    Stop a running pipeline.

    Args:
        client: Databricks client instance
        pipeline_id: Pipeline ID

    Raises:
        requests.HTTPError: If API request fails
    """
    client.post(f"/api/2.0/pipelines/{pipeline_id}/stop", json={})


def get_pipeline_events(
    client: DatabricksClient,
    pipeline_id: str,
    max_results: int = 100
) -> List[Dict[str, Any]]:
    """
    Get pipeline events, issues, and error messages.

    Args:
        client: Databricks client instance
        pipeline_id: Pipeline ID
        max_results: Maximum number of events to return

    Returns:
        List of event dictionaries with error details

    Raises:
        requests.HTTPError: If API request fails
    """
    response = client.get(
        f"/api/2.0/pipelines/{pipeline_id}/events",
        params={"max_results": max_results}
    )
    return response.get("events", [])
