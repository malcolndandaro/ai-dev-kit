"""
MCP Tool Wrappers for Compute Operations

Wraps databricks-mcp-core compute functions as MCP tools.
"""
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../databricks-mcp-core"))

from databricks_mcp_core.client import DatabricksClient
from databricks_mcp_core.compute import execution


# Lazy client initialization
_client = None

def get_client():
    """
    Get or create Databricks client.

    Uses DATABRICKS_CONFIG_PROFILE env var if set, otherwise falls back to
    DATABRICKS_HOST/DATABRICKS_TOKEN env vars.
    """
    global _client
    if _client is None:
        _client = DatabricksClient()
    return _client


def create_context_tool(arguments: dict) -> dict:
    """MCP tool: Create execution context"""
    try:
        context_id = execution.create_context(
            get_client(),
            arguments.get("cluster_id"),
            arguments.get("language", "python")
        )
        return {
            "content": [{
                "type": "text",
                "text": f"Context created successfully!\n\nContext ID: {context_id}\nCluster ID: {arguments.get('cluster_id')}\nLanguage: {arguments.get('language', 'python')}\n\nUse this context_id for subsequent commands to maintain state."
            }]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error creating context: {str(e)}"}],
            "isError": True
        }


def execute_command_with_context_tool(arguments: dict) -> dict:
    """MCP tool: Execute command with context"""
    try:
        result = execution.execute_command_with_context(
            get_client(),
            arguments.get("cluster_id"),
            arguments.get("context_id"),
            arguments.get("code")
        )
        if result.success:
            return {"content": [{"type": "text", "text": result.output}]}
        else:
            return {
                "content": [{"type": "text", "text": f"Error: {result.error}"}],
                "isError": True
            }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error: {str(e)}"}],
            "isError": True
        }


def destroy_context_tool(arguments: dict) -> dict:
    """MCP tool: Destroy execution context"""
    try:
        execution.destroy_context(
            get_client(),
            arguments.get("cluster_id"),
            arguments.get("context_id")
        )
        return {
            "content": [{"type": "text", "text": f"Context {arguments.get('context_id')} destroyed successfully!"}]
        }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error destroying context: {str(e)}"}],
            "isError": True
        }


def databricks_command_tool(arguments: dict) -> dict:
    """MCP tool: Execute one-off Databricks command"""
    try:
        result = execution.execute_databricks_command(
            get_client(),
            arguments.get("cluster_id"),
            arguments.get("language", "python"),
            arguments.get("code")
        )
        if result.success:
            return {"content": [{"type": "text", "text": result.output}]}
        else:
            return {
                "content": [{"type": "text", "text": f"Error: {result.error}"}],
                "isError": True
            }
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error: {str(e)}"}],
            "isError": True
        }


# Tool handler mapping
TOOL_HANDLERS = {
    "create_context": create_context_tool,
    "execute_command_with_context": execute_command_with_context_tool,
    "destroy_context": destroy_context_tool,
    "databricks_command": databricks_command_tool,
}


def get_tool_definitions():
    """Return MCP tool definitions for compute operations"""
    return [
        {
            "name": "create_context",
            "description": "Create a new execution context on Databricks cluster. Returns a context_id that can be used for subsequent commands to maintain state (variables, imports, etc) between calls.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster_id": {
                        "type": "string",
                        "description": "Databricks cluster ID"
                    },
                    "language": {
                        "type": "string",
                        "description": "Language (python, scala, sql, r)",
                        "default": "python"
                    }
                },
                "required": ["cluster_id"]
            }
        },
        {
            "name": "execute_command_with_context",
            "description": "Execute code using an existing context. This maintains state between calls - variables, imports, and data persist across commands. Use create_context first to get a context_id.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster_id": {
                        "type": "string",
                        "description": "Databricks cluster ID"
                    },
                    "context_id": {
                        "type": "string",
                        "description": "Context ID from create_context"
                    },
                    "code": {
                        "type": "string",
                        "description": "Python code to execute"
                    }
                },
                "required": ["cluster_id", "context_id", "code"]
            }
        },
        {
            "name": "destroy_context",
            "description": "Destroy an execution context to free resources. Call this when you're done with a context.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster_id": {
                        "type": "string",
                        "description": "Databricks cluster ID"
                    },
                    "context_id": {
                        "type": "string",
                        "description": "Context ID to destroy"
                    }
                },
                "required": ["cluster_id", "context_id"]
            }
        },
        {
            "name": "databricks_command",
            "description": "Executes Python code on a Databricks cluster via the Command Execution API (creates and destroys context automatically - does NOT maintain state between calls)",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster_id": {
                        "type": "string",
                        "description": "Databricks cluster ID"
                    },
                    "language": {
                        "type": "string",
                        "description": "Language (python, scala, etc)",
                        "default": "python"
                    },
                    "code": {
                        "type": "string",
                        "description": "Code to execute"
                    }
                },
                "required": ["cluster_id", "language", "code"]
            }
        },
    ]
