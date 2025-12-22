"""
Unity Catalog - Catalog Operations

Functions for listing and getting catalog information.
"""
from typing import List, Dict, Any
from ..client import DatabricksClient


def list_catalogs(client: DatabricksClient) -> List[Dict[str, Any]]:
    """
    List all catalogs in Unity Catalog.

    Args:
        client: Databricks client instance

    Returns:
        List of catalog dictionaries with metadata

    Raises:
        requests.HTTPError: If API request fails
    """
    response = client.get("/api/2.1/unity-catalog/catalogs")
    return response.get("catalogs", [])


def get_catalog(client: DatabricksClient, catalog_name: str) -> Dict[str, Any]:
    """
    Get detailed information about a specific catalog.

    Args:
        client: Databricks client instance
        catalog_name: Name of the catalog

    Returns:
        Dictionary with catalog metadata including:
        - name, full_name, owner, comment
        - created_at, updated_at
        - storage_location

    Raises:
        requests.HTTPError: If API request fails
    """
    return client.get(f"/api/2.1/unity-catalog/catalogs/{catalog_name}")
