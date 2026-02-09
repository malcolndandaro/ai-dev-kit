"""Resource tracking manifest tools.

Exposes the resource manifest as MCP tools so agents can list and clean up
resources created across sessions.
"""

import logging
from typing import Any, Dict, List, Optional

from ..manifest import list_resources, remove_resource
from ..server import mcp

logger = logging.getLogger(__name__)

# Mapping from resource type to the delete function and its ID parameter name
_DELETE_DISPATCHERS = {
    "dashboard": ("_trash_dashboard", "dashboard_id"),
    "job": ("_delete_job", "job_id"),
    "pipeline": ("_delete_pipeline", "pipeline_id"),
    "genie_space": ("_delete_genie", "space_id"),
    "knowledge_assistant": ("_delete_ka", "tile_id"),
    "multi_agent_supervisor": ("_delete_mas", "tile_id"),
}


def _delete_from_databricks(resource_type: str, resource_id: str) -> Optional[str]:
    """Delete a resource from Databricks. Returns error string or None on success."""
    try:
        if resource_type == "dashboard":
            from databricks_tools_core.aibi_dashboards import trash_dashboard

            trash_dashboard(dashboard_id=resource_id)
        elif resource_type == "job":
            from databricks_tools_core.jobs import delete_job

            delete_job(job_id=int(resource_id))
        elif resource_type == "pipeline":
            from databricks_tools_core.spark_declarative_pipelines.pipelines import (
                delete_pipeline,
            )

            delete_pipeline(pipeline_id=resource_id)
        elif resource_type == "genie_space":
            from databricks_tools_core.agent_bricks import AgentBricksManager

            manager = AgentBricksManager()
            manager.genie_delete(resource_id)
        elif resource_type == "knowledge_assistant":
            from databricks_tools_core.agent_bricks import AgentBricksManager

            manager = AgentBricksManager()
            manager.delete(resource_id)
        elif resource_type == "multi_agent_supervisor":
            from databricks_tools_core.agent_bricks import AgentBricksManager

            manager = AgentBricksManager()
            manager.delete(resource_id)
        elif resource_type in ("catalog", "schema", "volume"):
            # UC objects: schemas and catalogs are deleted by qualified name (which is also the ID)
            from databricks_tools_core.auth import get_workspace_client

            w = get_workspace_client()
            if resource_type == "catalog":
                w.catalogs.delete(name=resource_id, force=True)
            elif resource_type == "schema":
                w.schemas.delete(full_name_arg=resource_id)
            elif resource_type == "volume":
                w.volumes.delete(name=resource_id)
        else:
            return f"Unsupported resource type for deletion: {resource_type}"
        return None
    except Exception as exc:
        return str(exc)


@mcp.tool
def list_tracked_resources(type: Optional[str] = None) -> Dict[str, Any]:
    """List resources tracked in the project manifest.

    The manifest records every resource created through the MCP server
    (dashboards, jobs, pipelines, Genie spaces, KAs, MAS, schemas, volumes, etc.).
    Use this to see what was created across sessions.

    Args:
        type: Optional filter by resource type. One of: "dashboard", "job",
            "pipeline", "genie_space", "knowledge_assistant",
            "multi_agent_supervisor", "catalog", "schema", "volume".
            If not provided, returns all tracked resources.

    Returns:
        Dictionary with:
        - resources: List of tracked resources (type, name, id, url, timestamps)
        - count: Number of resources returned
    """
    resources = list_resources(resource_type=type)
    return {
        "resources": resources,
        "count": len(resources),
    }


@mcp.tool
def delete_tracked_resource(
    type: str,
    resource_id: str,
    delete_from_databricks: bool = False,
) -> Dict[str, Any]:
    """Delete a resource from the project manifest, and optionally from Databricks.

    Use this to clean up resources that were created during development/testing.

    Args:
        type: Resource type (e.g., "dashboard", "job", "pipeline", "genie_space",
            "knowledge_assistant", "multi_agent_supervisor", "catalog", "schema", "volume")
        resource_id: The resource ID (as shown in list_tracked_resources)
        delete_from_databricks: If True, also delete the resource from Databricks
            before removing it from the manifest. Default: False (manifest-only).

    Returns:
        Dictionary with:
        - success: Whether the operation succeeded
        - removed_from_manifest: Whether the resource was found and removed
        - deleted_from_databricks: Whether the resource was deleted from Databricks
        - error: Error message if deletion failed
    """
    result: Dict[str, Any] = {
        "success": True,
        "removed_from_manifest": False,
        "deleted_from_databricks": False,
        "error": None,
    }

    # Optionally delete from Databricks first
    if delete_from_databricks:
        error = _delete_from_databricks(type, resource_id)
        if error:
            result["error"] = f"Databricks deletion failed: {error}"
            result["success"] = False
            # Still remove from manifest even if Databricks deletion failed
        else:
            result["deleted_from_databricks"] = True

    # Remove from manifest
    removed = remove_resource(resource_type=type, resource_id=resource_id)
    result["removed_from_manifest"] = removed

    if not removed and not result.get("error"):
        result["error"] = f"Resource {type}/{resource_id} not found in manifest"
        result["success"] = result.get("deleted_from_databricks", False)

    return result
