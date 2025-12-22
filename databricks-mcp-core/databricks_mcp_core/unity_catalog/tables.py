"""
Unity Catalog - Table Operations

Functions for managing tables in Unity Catalog.
"""
from typing import List, Dict, Any, Optional
from ..client import DatabricksClient


def list_tables(client: DatabricksClient, catalog_name: str, schema_name: str) -> List[Dict[str, Any]]:
    """
    List all tables in a schema.

    Args:
        client: Databricks client instance
        catalog_name: Name of the catalog
        schema_name: Name of the schema

    Returns:
        List of table dictionaries with metadata

    Raises:
        requests.HTTPError: If API request fails
    """
    response = client.get(
        "/api/2.1/unity-catalog/tables",
        params={
            "catalog_name": catalog_name,
            "schema_name": schema_name
        }
    )
    return response.get("tables", [])


def get_table(client: DatabricksClient, full_table_name: str) -> Dict[str, Any]:
    """
    Get detailed information about a specific table.

    Args:
        client: Databricks client instance
        full_table_name: Full table name (catalog.schema.table format)

    Returns:
        Dictionary with table metadata including:
        - name, full_name, catalog_name, schema_name
        - table_type, owner, comment
        - created_at, updated_at
        - storage_location, columns

    Raises:
        requests.HTTPError: If API request fails
    """
    return client.get(f"/api/2.1/unity-catalog/tables/{full_table_name}")


def create_table(
    client: DatabricksClient,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    columns: List[Dict[str, Any]],
    table_type: str = "MANAGED",
    comment: Optional[str] = None,
    storage_location: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a new table in Unity Catalog.

    Args:
        client: Databricks client instance
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        table_name: Name of the table to create
        columns: List of column definitions, each with 'name' and 'type_name' keys
                 Example: [{"name": "id", "type_name": "INT"}, {"name": "value", "type_name": "STRING"}]
        table_type: Type of table - "MANAGED" or "EXTERNAL" (default: "MANAGED")
        comment: Optional description of the table
        storage_location: Storage location for EXTERNAL tables

    Returns:
        Dictionary with created table metadata

    Raises:
        requests.HTTPError: If API request fails
    """
    payload = {
        "name": table_name,
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_type": table_type,
        "columns": columns,
        "data_source_format": "DELTA"
    }

    if comment:
        payload["comment"] = comment

    if storage_location and table_type == "EXTERNAL":
        payload["storage_location"] = storage_location

    return client.post("/api/2.1/unity-catalog/tables", json=payload)


def delete_table(client: DatabricksClient, full_table_name: str) -> None:
    """
    Delete a table from Unity Catalog.

    Args:
        client: Databricks client instance
        full_table_name: Full table name (catalog.schema.table format)

    Raises:
        requests.HTTPError: If API request fails
    """
    client.delete(f"/api/2.1/unity-catalog/tables/{full_table_name}")
