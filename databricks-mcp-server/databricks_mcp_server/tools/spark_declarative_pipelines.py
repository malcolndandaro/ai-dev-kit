"""
MCP tool wrappers for Spark Declarative Pipelines operations.
"""
from typing import Dict, Any, List
from databricks_mcp_core.client import DatabricksClient
from databricks_mcp_core.spark_declarative_pipelines import pipelines, workspace_files
import json

# Lazy client initialization to avoid import-time errors
_client = None

def get_client():
    """Get or create the Databricks client instance."""
    global _client
    if _client is None:
        _client = DatabricksClient()
    return _client


# Pipeline Management Tools

def create_pipeline_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Create a new Spark Declarative Pipeline."""
    try:
        client = get_client()
        result = pipelines.create_pipeline(
            client=client,
            name=arguments["name"],
            storage=arguments["storage"],
            target=arguments["target"],
            libraries=arguments["libraries"],
            clusters=arguments.get("clusters"),
            configuration=arguments.get("configuration"),
            continuous=arguments.get("continuous", False)
        )

        pipeline_id = result.get("pipeline_id", "unknown")
        return {
            "content": [
                {
                    "type": "text",
                    "text": f"✅ Pipeline created successfully!\n\n"
                           f"Pipeline ID: {pipeline_id}\n"
                           f"Name: {arguments['name']}\n"
                           f"Target: {arguments['target']}\n"
                           f"Storage: {arguments['storage']}\n"
                           f"Continuous: {arguments.get('continuous', False)}\n\n"
                           f"Full response:\n{json.dumps(result, indent=2)}"
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error creating pipeline: {str(e)}"}],
            "isError": True
        }


def get_pipeline_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Get pipeline details and configuration."""
    try:
        client = get_client()
        result = pipelines.get_pipeline(client, arguments["pipeline_id"])

        name = result.get("name", "unknown")
        state = result.get("state", "unknown")
        target = result.get("spec", {}).get("target", "unknown")

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"📊 Pipeline Details\n\n"
                           f"Name: {name}\n"
                           f"State: {state}\n"
                           f"Target: {target}\n"
                           f"Pipeline ID: {arguments['pipeline_id']}\n\n"
                           f"Full configuration:\n{json.dumps(result, indent=2)}"
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error getting pipeline: {str(e)}"}],
            "isError": True
        }


def update_pipeline_config_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Update pipeline configuration (not code files)."""
    try:
        client = get_client()
        result = pipelines.update_pipeline(
            client=client,
            pipeline_id=arguments["pipeline_id"],
            name=arguments.get("name"),
            storage=arguments.get("storage"),
            target=arguments.get("target"),
            libraries=arguments.get("libraries"),
            clusters=arguments.get("clusters"),
            configuration=arguments.get("configuration"),
            continuous=arguments.get("continuous")
        )

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"✅ Pipeline configuration updated successfully!\n\n"
                           f"Pipeline ID: {arguments['pipeline_id']}\n\n"
                           f"Full response:\n{json.dumps(result, indent=2)}"
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error updating pipeline: {str(e)}"}],
            "isError": True
        }


def delete_pipeline_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Delete a pipeline."""
    try:
        client = get_client()
        pipelines.delete_pipeline(client, arguments["pipeline_id"])

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"✅ Pipeline deleted successfully!\n\n"
                           f"Pipeline ID: {arguments['pipeline_id']}"
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error deleting pipeline: {str(e)}"}],
            "isError": True
        }


def start_pipeline_update_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Start a pipeline update (full or incremental refresh)."""
    try:
        client = get_client()
        update_id = pipelines.start_update(
            client=client,
            pipeline_id=arguments["pipeline_id"],
            refresh_selection=arguments.get("refresh_selection"),
            full_refresh=arguments.get("full_refresh", False),
            full_refresh_selection=arguments.get("full_refresh_selection"),
            validate_only=False
        )

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"✅ Pipeline update started!\n\n"
                           f"Pipeline ID: {arguments['pipeline_id']}\n"
                           f"Update ID: {update_id}\n"
                           f"Full Refresh: {arguments.get('full_refresh', False)}\n\n"
                           f"Use get_pipeline_update_status to poll for completion."
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error starting update: {str(e)}"}],
            "isError": True
        }


def validate_pipeline_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Perform dry-run validation without updating datasets."""
    try:
        client = get_client()
        update_id = pipelines.start_update(
            client=client,
            pipeline_id=arguments["pipeline_id"],
            refresh_selection=arguments.get("refresh_selection"),
            full_refresh=arguments.get("full_refresh", False),
            full_refresh_selection=arguments.get("full_refresh_selection"),
            validate_only=True
        )

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"✅ Pipeline validation started (dry-run mode)!\n\n"
                           f"Pipeline ID: {arguments['pipeline_id']}\n"
                           f"Update ID: {update_id}\n\n"
                           f"Use get_pipeline_update_status to check validation results."
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error starting validation: {str(e)}"}],
            "isError": True
        }


def get_pipeline_update_status_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Get pipeline update status and results."""
    try:
        client = get_client()
        result = pipelines.get_update(
            client,
            arguments["pipeline_id"],
            arguments["update_id"]
        )

        update_info = result.get("update", {})
        state = update_info.get("state", "unknown")

        # Format based on state
        emoji = {
            "QUEUED": "⏳",
            "RUNNING": "🔄",
            "COMPLETED": "✅",
            "FAILED": "❌",
            "CANCELED": "🚫"
        }.get(state, "❓")

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"{emoji} Pipeline Update Status\n\n"
                           f"State: {state}\n"
                           f"Pipeline ID: {arguments['pipeline_id']}\n"
                           f"Update ID: {arguments['update_id']}\n\n"
                           f"Full status:\n{json.dumps(result, indent=2)}"
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error getting update status: {str(e)}"}],
            "isError": True
        }


def stop_pipeline_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Stop a running pipeline."""
    try:
        client = get_client()
        pipelines.stop_pipeline(client, arguments["pipeline_id"])

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"✅ Pipeline stop request sent!\n\n"
                           f"Pipeline ID: {arguments['pipeline_id']}"
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error stopping pipeline: {str(e)}"}],
            "isError": True
        }


def get_pipeline_events_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Get pipeline events, issues, and error messages."""
    try:
        client = get_client()
        events = pipelines.get_pipeline_events(
            client,
            arguments["pipeline_id"],
            arguments.get("max_results", 100)
        )

        event_count = len(events)
        error_count = sum(1 for e in events if e.get("event_type", "").endswith("_failed"))

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"📋 Pipeline Events\n\n"
                           f"Pipeline ID: {arguments['pipeline_id']}\n"
                           f"Total Events: {event_count}\n"
                           f"Error Events: {error_count}\n\n"
                           f"Events:\n{json.dumps(events, indent=2)}"
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error getting pipeline events: {str(e)}"}],
            "isError": True
        }


# Workspace File Tools

def list_pipeline_files_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """List files and directories in a workspace path."""
    try:
        client = get_client()
        files = workspace_files.list_files(client, arguments["path"])

        file_count = sum(1 for f in files if f.get("object_type") != "DIRECTORY")
        dir_count = sum(1 for f in files if f.get("object_type") == "DIRECTORY")

        file_list = "\n".join([
            f"  {'📁' if f.get('object_type') == 'DIRECTORY' else '📄'} {f.get('path', 'unknown')}"
            for f in files
        ])

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"📂 Workspace Files in {arguments['path']}\n\n"
                           f"Files: {file_count}\n"
                           f"Directories: {dir_count}\n\n"
                           f"{file_list}\n\n"
                           f"Full details:\n{json.dumps(files, indent=2)}"
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error listing files: {str(e)}"}],
            "isError": True
        }


def get_pipeline_file_status_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Get file or directory metadata."""
    try:
        client = get_client()
        status = workspace_files.get_file_status(client, arguments["path"])

        obj_type = status.get("object_type", "unknown")
        language = status.get("language", "N/A")
        size = status.get("size", "N/A")

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"📄 File Status\n\n"
                           f"Path: {arguments['path']}\n"
                           f"Type: {obj_type}\n"
                           f"Language: {language}\n"
                           f"Size: {size}\n\n"
                           f"Full metadata:\n{json.dumps(status, indent=2)}"
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error getting file status: {str(e)}"}],
            "isError": True
        }


def read_pipeline_file_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Read workspace file contents."""
    try:
        client = get_client()
        content = workspace_files.read_file(client, arguments["path"])

        line_count = content.count('\n') + 1
        char_count = len(content)

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"📄 File Contents: {arguments['path']}\n\n"
                           f"Lines: {line_count}\n"
                           f"Characters: {char_count}\n\n"
                           f"```\n{content}\n```"
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error reading file: {str(e)}"}],
            "isError": True
        }


def write_pipeline_file_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Write or update workspace file."""
    try:
        client = get_client()
        workspace_files.write_file(
            client=client,
            path=arguments["path"],
            content=arguments["content"],
            language=arguments.get("language", "PYTHON"),
            overwrite=arguments.get("overwrite", True)
        )

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"✅ File written successfully!\n\n"
                           f"Path: {arguments['path']}\n"
                           f"Language: {arguments.get('language', 'PYTHON')}\n"
                           f"Overwrite: {arguments.get('overwrite', True)}"
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error writing file: {str(e)}"}],
            "isError": True
        }


def create_pipeline_directory_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Create workspace directory."""
    try:
        client = get_client()
        workspace_files.create_directory(client, arguments["path"])

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"✅ Directory created successfully!\n\n"
                           f"Path: {arguments['path']}"
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error creating directory: {str(e)}"}],
            "isError": True
        }


def delete_pipeline_path_tool(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Delete workspace file or directory."""
    try:
        client = get_client()
        workspace_files.delete_path(
            client,
            arguments["path"],
            arguments.get("recursive", False)
        )

        return {
            "content": [
                {
                    "type": "text",
                    "text": f"✅ Path deleted successfully!\n\n"
                           f"Path: {arguments['path']}\n"
                           f"Recursive: {arguments.get('recursive', False)}"
                }
            ]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"❌ Error deleting path: {str(e)}"}],
            "isError": True
        }


# Tool definitions and handlers

TOOL_HANDLERS = {
    "create_pipeline": create_pipeline_tool,
    "get_pipeline": get_pipeline_tool,
    "update_pipeline_config": update_pipeline_config_tool,
    "delete_pipeline": delete_pipeline_tool,
    "start_pipeline_update": start_pipeline_update_tool,
    "validate_pipeline": validate_pipeline_tool,
    "get_pipeline_update_status": get_pipeline_update_status_tool,
    "stop_pipeline": stop_pipeline_tool,
    "get_pipeline_events": get_pipeline_events_tool,
    "list_pipeline_files": list_pipeline_files_tool,
    "get_pipeline_file_status": get_pipeline_file_status_tool,
    "read_pipeline_file": read_pipeline_file_tool,
    "write_pipeline_file": write_pipeline_file_tool,
    "create_pipeline_directory": create_pipeline_directory_tool,
    "delete_pipeline_path": delete_pipeline_path_tool
}


def get_tool_definitions() -> List[Dict[str, Any]]:
    """Get MCP tool definitions for Spark Declarative Pipelines."""
    return [
        {
            "name": "create_pipeline",
            "description": "Create a new Spark Declarative Pipeline",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Pipeline name"},
                    "storage": {"type": "string", "description": "Storage location for pipeline data"},
                    "target": {"type": "string", "description": "Target catalog.schema for output tables"},
                    "libraries": {
                        "type": "array",
                        "description": "List of notebook/file paths. Example: [{\"notebook\": {\"path\": \"/path/to/file.py\"}}]",
                        "items": {"type": "object"}
                    },
                    "clusters": {
                        "type": "array",
                        "description": "Optional cluster configuration",
                        "items": {"type": "object"}
                    },
                    "configuration": {
                        "type": "object",
                        "description": "Optional Spark configuration key-value pairs"
                    },
                    "continuous": {
                        "type": "boolean",
                        "description": "If true, pipeline runs continuously",
                        "default": False
                    }
                },
                "required": ["name", "storage", "target", "libraries"]
            }
        },
        {
            "name": "get_pipeline",
            "description": "Get pipeline details and configuration",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string", "description": "Pipeline ID"}
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "update_pipeline_config",
            "description": "Update pipeline configuration (not code files). Only specify fields you want to change.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string", "description": "Pipeline ID"},
                    "name": {"type": "string", "description": "New pipeline name"},
                    "storage": {"type": "string", "description": "New storage location"},
                    "target": {"type": "string", "description": "New target catalog.schema"},
                    "libraries": {
                        "type": "array",
                        "description": "New library paths",
                        "items": {"type": "object"}
                    },
                    "clusters": {
                        "type": "array",
                        "description": "New cluster configuration",
                        "items": {"type": "object"}
                    },
                    "configuration": {
                        "type": "object",
                        "description": "New Spark configuration"
                    },
                    "continuous": {
                        "type": "boolean",
                        "description": "New continuous mode setting"
                    }
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "delete_pipeline",
            "description": "Delete a pipeline",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string", "description": "Pipeline ID"}
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "start_pipeline_update",
            "description": "Start a pipeline update (full or incremental refresh). Returns update_id for polling.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string", "description": "Pipeline ID"},
                    "refresh_selection": {
                        "type": "array",
                        "description": "List of table names to refresh",
                        "items": {"type": "string"}
                    },
                    "full_refresh": {
                        "type": "boolean",
                        "description": "If true, performs full refresh",
                        "default": False
                    },
                    "full_refresh_selection": {
                        "type": "array",
                        "description": "List of table names for full refresh",
                        "items": {"type": "string"}
                    }
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "validate_pipeline",
            "description": "Perform dry-run validation without updating datasets. Returns update_id for polling results.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string", "description": "Pipeline ID"},
                    "refresh_selection": {
                        "type": "array",
                        "description": "List of table names to validate",
                        "items": {"type": "string"}
                    },
                    "full_refresh": {
                        "type": "boolean",
                        "description": "If true, validates with full refresh",
                        "default": False
                    },
                    "full_refresh_selection": {
                        "type": "array",
                        "description": "List of table names for full refresh validation",
                        "items": {"type": "string"}
                    }
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "get_pipeline_update_status",
            "description": "Get pipeline update status and results. Poll this to check if update/validation is complete.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string", "description": "Pipeline ID"},
                    "update_id": {"type": "string", "description": "Update ID from start_pipeline_update or validate_pipeline"}
                },
                "required": ["pipeline_id", "update_id"]
            }
        },
        {
            "name": "stop_pipeline",
            "description": "Stop a running pipeline",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string", "description": "Pipeline ID"}
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "get_pipeline_events",
            "description": "Get pipeline events, issues, and error messages",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string", "description": "Pipeline ID"},
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum number of events to return",
                        "default": 100
                    }
                },
                "required": ["pipeline_id"]
            }
        },
        {
            "name": "list_pipeline_files",
            "description": "List files and directories in a workspace path",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace path to list"}
                },
                "required": ["path"]
            }
        },
        {
            "name": "get_pipeline_file_status",
            "description": "Get file or directory metadata",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace path"}
                },
                "required": ["path"]
            }
        },
        {
            "name": "read_pipeline_file",
            "description": "Read workspace file contents",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace file path"}
                },
                "required": ["path"]
            }
        },
        {
            "name": "write_pipeline_file",
            "description": "Write or update workspace file",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace file path"},
                    "content": {"type": "string", "description": "File content"},
                    "language": {
                        "type": "string",
                        "description": "File language: PYTHON, SQL, SCALA, or R",
                        "default": "PYTHON"
                    },
                    "overwrite": {
                        "type": "boolean",
                        "description": "If true, replaces existing file",
                        "default": True
                    }
                },
                "required": ["path", "content"]
            }
        },
        {
            "name": "create_pipeline_directory",
            "description": "Create workspace directory",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace directory path"}
                },
                "required": ["path"]
            }
        },
        {
            "name": "delete_pipeline_path",
            "description": "Delete workspace file or directory",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace path to delete"},
                    "recursive": {
                        "type": "boolean",
                        "description": "If true, recursively deletes directories",
                        "default": False
                    }
                },
                "required": ["path"]
            }
        }
    ]
