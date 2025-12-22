"""
Spark Declarative Pipelines - Workspace File Operations

Functions for managing workspace files and directories for SDP pipelines.
"""
import base64
from typing import Dict, Any, List
from ..client import DatabricksClient


def list_files(
    client: DatabricksClient,
    path: str
) -> List[Dict[str, Any]]:
    """
    List files and directories in a workspace path.

    Args:
        client: Databricks client instance
        path: Workspace path to list

    Returns:
        List of file/directory metadata with keys:
        - path: Full workspace path
        - object_type: DIRECTORY, NOTEBOOK, FILE, LIBRARY, or REPO
        - language: For notebooks (PYTHON, SQL, SCALA, R)
        - object_id: Unique identifier

    Raises:
        requests.HTTPError: If API request fails
    """
    response = client.get("/api/2.0/workspace/list", params={"path": path})
    return response.get("objects", [])


def get_file_status(
    client: DatabricksClient,
    path: str
) -> Dict[str, Any]:
    """
    Get file or directory metadata.

    Args:
        client: Databricks client instance
        path: Workspace path

    Returns:
        Dictionary with metadata:
        - path: Full workspace path
        - object_type: DIRECTORY, NOTEBOOK, FILE, LIBRARY, or REPO
        - language: For notebooks (PYTHON, SQL, SCALA, R)
        - object_id: Unique identifier
        - size: File size in bytes (for files)
        - created_at: Creation timestamp
        - modified_at: Last modification timestamp

    Raises:
        requests.HTTPError: If API request fails
    """
    return client.get("/api/2.0/workspace/get-status", params={"path": path})


def read_file(
    client: DatabricksClient,
    path: str
) -> str:
    """
    Read workspace file contents.

    Args:
        client: Databricks client instance
        path: Workspace file path

    Returns:
        Decoded file contents as string

    Raises:
        requests.HTTPError: If API request fails
    """
    response = client.get(
        "/api/2.0/workspace/export",
        params={"path": path, "format": "SOURCE"}
    )
    content_b64 = response["content"]
    return base64.b64decode(content_b64).decode("utf-8")


def write_file(
    client: DatabricksClient,
    path: str,
    content: str,
    language: str = "PYTHON",
    overwrite: bool = True
) -> None:
    """
    Write or update workspace file.

    Args:
        client: Databricks client instance
        path: Workspace file path
        content: File content as string
        language: PYTHON, SQL, SCALA, or R
        overwrite: If True, replaces existing file

    Raises:
        requests.HTTPError: If API request fails
    """
    content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    client.post(
        "/api/2.0/workspace/import",
        json={
            "path": path,
            "content": content_b64,
            "language": language,
            "format": "SOURCE",
            "overwrite": overwrite
        }
    )


def create_directory(
    client: DatabricksClient,
    path: str
) -> None:
    """
    Create workspace directory.

    Args:
        client: Databricks client instance
        path: Workspace directory path

    Raises:
        requests.HTTPError: If API request fails
    """
    client.post("/api/2.0/workspace/mkdirs", json={"path": path})


def delete_path(
    client: DatabricksClient,
    path: str,
    recursive: bool = False
) -> None:
    """
    Delete workspace file or directory.

    Args:
        client: Databricks client instance
        path: Workspace path to delete
        recursive: If True, recursively deletes directories

    Raises:
        requests.HTTPError: If API request fails
    """
    client.post(
        "/api/2.0/workspace/delete",
        json={"path": path, "recursive": recursive}
    )
