"""
Unity Catalog - Schema Operations

Functions for managing schemas (databases) in Unity Catalog.
"""
from typing import List, Dict, Any, Optional
from ..client import DatabricksClient


def list_schemas(client: DatabricksClient, catalog_name: str) -> List[Dict[str, Any]]:
    """
    List all schemas in a catalog.

    Args:
        client: Databricks client instance
        catalog_name: Name of the catalog

    Returns:
        List of schema dictionaries with metadata

    Raises:
        requests.HTTPError: If API request fails
    """
    response = client.get(
        "/api/2.1/unity-catalog/schemas",
        params={"catalog_name": catalog_name}
    )
    return response.get("schemas", [])


def get_schema(client: DatabricksClient, full_schema_name: str) -> Dict[str, Any]:
    """
    Get detailed information about a specific schema.

    Args:
        client: Databricks client instance
        full_schema_name: Full schema name (catalog.schema format)

    Returns:
        Dictionary with schema metadata including:
        - name, full_name, catalog_name, owner, comment
        - created_at, updated_at
        - storage_location

    Raises:
        requests.HTTPError: If API request fails
    """
    return client.get(f"/api/2.1/unity-catalog/schemas/{full_schema_name}")


def create_schema(
    client: DatabricksClient,
    catalog_name: str,
    schema_name: str,
    comment: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a new schema in Unity Catalog.

    Args:
        client: Databricks client instance
        catalog_name: Name of the catalog
        schema_name: Name of the schema to create
        comment: Optional description of the schema

    Returns:
        Dictionary with created schema metadata

    Raises:
        requests.HTTPError: If API request fails
    """
    payload = {
        "name": schema_name,
        "catalog_name": catalog_name
    }
    if comment:
        payload["comment"] = comment

    return client.post("/api/2.1/unity-catalog/schemas", json=payload)


def update_schema(
    client: DatabricksClient,
    full_schema_name: str,
    new_name: Optional[str] = None,
    comment: Optional[str] = None,
    owner: Optional[str] = None
) -> Dict[str, Any]:
    """
    Update an existing schema in Unity Catalog.

    Args:
        client: Databricks client instance
        full_schema_name: Full schema name (catalog.schema format)
        new_name: New name for the schema
        comment: New comment/description
        owner: New owner

    Returns:
        Dictionary with updated schema metadata

    Raises:
        ValueError: If no fields are provided to update
        requests.HTTPError: If API request fails
    """
    payload = {}
    if new_name:
        payload["new_name"] = new_name
    if comment:
        payload["comment"] = comment
    if owner:
        payload["owner"] = owner

    if not payload:
        raise ValueError("At least one field (new_name, comment, or owner) must be provided")

    return client.patch(f"/api/2.1/unity-catalog/schemas/{full_schema_name}", json=payload)


def delete_schema(client: DatabricksClient, full_schema_name: str) -> None:
    """
    Delete a schema from Unity Catalog.

    Args:
        client: Databricks client instance
        full_schema_name: Full schema name (catalog.schema format)

    Raises:
        requests.HTTPError: If API request fails
    """
    client.delete(f"/api/2.1/unity-catalog/schemas/{full_schema_name}")
