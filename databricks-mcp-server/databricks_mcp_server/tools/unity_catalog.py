"""
MCP Tool Wrappers for Unity Catalog Operations

Wraps databricks-mcp-core Unity Catalog functions as MCP tools.
"""
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../databricks-mcp-core"))

from databricks_mcp_core.client import DatabricksClient
from databricks_mcp_core.unity_catalog import catalogs, schemas, tables


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


# === Catalog Tools ===

def list_catalogs_tool(arguments: dict) -> dict:
    """MCP tool: List all catalogs"""
    try:
        catalog_list = catalogs.list_catalogs(get_client())
        output = f"Found {len(catalog_list)} catalogs:\n\n"
        for catalog in catalog_list:
            output += f"📚 {catalog.get('name')}\n"
            if catalog.get('comment'):
                output += f"   Comment: {catalog.get('comment')}\n"
            output += f"   Owner: {catalog.get('owner')}\n"
            output += f"   Created: {catalog.get('created_at')}\n\n"
        return {"content": [{"type": "text", "text": output}]}
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error: {str(e)}"}],
            "isError": True
        }


def get_catalog_tool(arguments: dict) -> dict:
    """MCP tool: Get catalog details"""
    try:
        catalog = catalogs.get_catalog(get_client(), arguments.get("catalog_name"))
        output = f"📚 Catalog: {catalog.get('name')}\n"
        output += f"   Full Name: {catalog.get('full_name')}\n"
        output += f"   Owner: {catalog.get('owner')}\n"
        output += f"   Comment: {catalog.get('comment', 'N/A')}\n"
        output += f"   Created: {catalog.get('created_at')}\n"
        output += f"   Updated: {catalog.get('updated_at')}\n"
        output += f"   Storage Location: {catalog.get('storage_location', 'N/A')}\n"
        return {"content": [{"type": "text", "text": output}]}
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error: {str(e)}"}],
            "isError": True
        }


# === Schema Tools ===

def list_schemas_tool(arguments: dict) -> dict:
    """MCP tool: List schemas in a catalog"""
    try:
        schema_list = schemas.list_schemas(get_client(), arguments.get("catalog_name"))
        catalog_name = arguments.get("catalog_name")
        output = f"Found {len(schema_list)} schemas in catalog '{catalog_name}':\n\n"
        for schema in schema_list:
            output += f"📁 {schema.get('name')}\n"
            if schema.get('comment'):
                output += f"   Comment: {schema.get('comment')}\n"
            output += f"   Owner: {schema.get('owner')}\n"
            output += f"   Full Name: {schema.get('full_name')}\n\n"
        return {"content": [{"type": "text", "text": output}]}
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error: {str(e)}"}],
            "isError": True
        }


def get_schema_tool(arguments: dict) -> dict:
    """MCP tool: Get schema details"""
    try:
        schema = schemas.get_schema(get_client(), arguments.get("full_schema_name"))
        output = f"📁 Schema: {schema.get('name')}\n"
        output += f"   Full Name: {schema.get('full_name')}\n"
        output += f"   Catalog: {schema.get('catalog_name')}\n"
        output += f"   Owner: {schema.get('owner')}\n"
        output += f"   Comment: {schema.get('comment', 'N/A')}\n"
        output += f"   Created: {schema.get('created_at')}\n"
        output += f"   Updated: {schema.get('updated_at')}\n"
        output += f"   Storage Location: {schema.get('storage_location', 'N/A')}\n"
        return {"content": [{"type": "text", "text": output}]}
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error: {str(e)}"}],
            "isError": True
        }


def create_schema_tool(arguments: dict) -> dict:
    """MCP tool: Create schema"""
    try:
        schema = schemas.create_schema(
            get_client(),
            arguments.get("catalog_name"),
            arguments.get("schema_name"),
            arguments.get("comment")
        )
        output = f"✅ Schema created successfully!\n\n"
        output += f"📁 Schema: {schema.get('name')}\n"
        output += f"   Full Name: {schema.get('full_name')}\n"
        output += f"   Catalog: {schema.get('catalog_name')}\n"
        output += f"   Owner: {schema.get('owner')}\n"
        if schema.get('comment'):
            output += f"   Comment: {schema.get('comment')}\n"
        return {"content": [{"type": "text", "text": output}]}
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error creating schema: {str(e)}"}],
            "isError": True
        }


def update_schema_tool(arguments: dict) -> dict:
    """MCP tool: Update schema"""
    try:
        schema = schemas.update_schema(
            get_client(),
            arguments.get("full_schema_name"),
            arguments.get("new_name"),
            arguments.get("comment"),
            arguments.get("owner")
        )
        output = f"✅ Schema updated successfully!\n\n"
        output += f"📁 Schema: {schema.get('name')}\n"
        output += f"   Full Name: {schema.get('full_name')}\n"
        output += f"   Owner: {schema.get('owner')}\n"
        return {"content": [{"type": "text", "text": output}]}
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error updating schema: {str(e)}"}],
            "isError": True
        }


def delete_schema_tool(arguments: dict) -> dict:
    """MCP tool: Delete schema"""
    try:
        full_schema_name = arguments.get("full_schema_name")
        schemas.delete_schema(get_client(), full_schema_name)
        output = f"✅ Schema '{full_schema_name}' deleted successfully!"
        return {"content": [{"type": "text", "text": output}]}
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error deleting schema: {str(e)}"}],
            "isError": True
        }


# === Table Tools ===

def list_tables_tool(arguments: dict) -> dict:
    """MCP tool: List tables in a schema"""
    try:
        table_list = tables.list_tables(
            get_client(),
            arguments.get("catalog_name"),
            arguments.get("schema_name")
        )
        catalog_name = arguments.get("catalog_name")
        schema_name = arguments.get("schema_name")
        output = f"Found {len(table_list)} tables in {catalog_name}.{schema_name}:\n\n"
        for table in table_list:
            output += f"📊 {table.get('name')}\n"
            output += f"   Type: {table.get('table_type')}\n"
            if table.get('comment'):
                output += f"   Comment: {table.get('comment')}\n"
            output += f"   Owner: {table.get('owner')}\n"
            output += f"   Full Name: {table.get('full_name')}\n\n"
        return {"content": [{"type": "text", "text": output}]}
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error: {str(e)}"}],
            "isError": True
        }


def get_table_tool(arguments: dict) -> dict:
    """MCP tool: Get table details"""
    try:
        table = tables.get_table(get_client(), arguments.get("full_table_name"))
        output = f"📊 Table: {table.get('name')}\n"
        output += f"   Full Name: {table.get('full_name')}\n"
        output += f"   Catalog: {table.get('catalog_name')}\n"
        output += f"   Schema: {table.get('schema_name')}\n"
        output += f"   Type: {table.get('table_type')}\n"
        output += f"   Owner: {table.get('owner')}\n"
        output += f"   Comment: {table.get('comment', 'N/A')}\n"
        output += f"   Created: {table.get('created_at')}\n"
        output += f"   Updated: {table.get('updated_at')}\n"
        output += f"   Storage Location: {table.get('storage_location', 'N/A')}\n"

        # Add column information
        columns = table.get('columns', [])
        if columns:
            output += f"\n   Columns ({len(columns)}):\n"
            for col in columns:
                output += f"     - {col.get('name')}: {col.get('type_name')}\n"
                if col.get('comment'):
                    output += f"       {col.get('comment')}\n"

        return {"content": [{"type": "text", "text": output}]}
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error: {str(e)}"}],
            "isError": True
        }


def create_table_tool(arguments: dict) -> dict:
    """MCP tool: Create table"""
    try:
        table = tables.create_table(
            get_client(),
            arguments.get("catalog_name"),
            arguments.get("schema_name"),
            arguments.get("table_name"),
            arguments.get("columns"),
            arguments.get("table_type", "MANAGED"),
            arguments.get("comment"),
            arguments.get("storage_location")
        )
        output = f"✅ Table created successfully!\n\n"
        output += f"📊 Table: {table.get('name')}\n"
        output += f"   Full Name: {table.get('full_name')}\n"
        output += f"   Catalog: {table.get('catalog_name')}\n"
        output += f"   Schema: {table.get('schema_name')}\n"
        output += f"   Type: {table.get('table_type')}\n"
        output += f"   Owner: {table.get('owner')}\n"
        if table.get('comment'):
            output += f"   Comment: {table.get('comment')}\n"

        # Show columns
        created_columns = table.get('columns', [])
        if created_columns:
            output += f"\n   Columns ({len(created_columns)}):\n"
            for col in created_columns:
                output += f"     - {col.get('name')}: {col.get('type_name')}\n"

        return {"content": [{"type": "text", "text": output}]}
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error creating table: {str(e)}"}],
            "isError": True
        }


def delete_table_tool(arguments: dict) -> dict:
    """MCP tool: Delete table"""
    try:
        full_table_name = arguments.get("full_table_name")
        tables.delete_table(get_client(), full_table_name)
        output = f"✅ Table '{full_table_name}' deleted successfully!"
        return {"content": [{"type": "text", "text": output}]}
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error deleting table: {str(e)}"}],
            "isError": True
        }


# Tool handler mapping
TOOL_HANDLERS = {
    "list_catalogs": list_catalogs_tool,
    "get_catalog": get_catalog_tool,
    "list_schemas": list_schemas_tool,
    "get_schema": get_schema_tool,
    "create_schema": create_schema_tool,
    "update_schema": update_schema_tool,
    "delete_schema": delete_schema_tool,
    "list_tables": list_tables_tool,
    "get_table": get_table_tool,
    "create_table": create_table_tool,
    "delete_table": delete_table_tool,
}


def get_tool_definitions():
    """Return MCP tool definitions for Unity Catalog operations"""
    return [
        {
            "name": "list_catalogs",
            "description": "List all catalogs in Unity Catalog",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        },
        {
            "name": "get_catalog",
            "description": "Get detailed information about a specific catalog",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "catalog_name": {
                        "type": "string",
                        "description": "Name of the catalog"
                    }
                },
                "required": ["catalog_name"]
            }
        },
        {
            "name": "list_schemas",
            "description": "List all schemas in a catalog",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "catalog_name": {
                        "type": "string",
                        "description": "Name of the catalog"
                    }
                },
                "required": ["catalog_name"]
            }
        },
        {
            "name": "get_schema",
            "description": "Get detailed information about a specific schema",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "full_schema_name": {
                        "type": "string",
                        "description": "Full schema name (catalog.schema format)"
                    }
                },
                "required": ["full_schema_name"]
            }
        },
        {
            "name": "list_tables",
            "description": "List all tables in a schema",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "catalog_name": {
                        "type": "string",
                        "description": "Name of the catalog"
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Name of the schema"
                    }
                },
                "required": ["catalog_name", "schema_name"]
            }
        },
        {
            "name": "get_table",
            "description": "Get detailed information about a specific table including columns",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "full_table_name": {
                        "type": "string",
                        "description": "Full table name (catalog.schema.table format)"
                    }
                },
                "required": ["full_table_name"]
            }
        },
        {
            "name": "create_schema",
            "description": "Create a new schema in Unity Catalog",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "catalog_name": {
                        "type": "string",
                        "description": "Name of the catalog"
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Name of the schema to create"
                    },
                    "comment": {
                        "type": "string",
                        "description": "Optional description of the schema"
                    }
                },
                "required": ["catalog_name", "schema_name"]
            }
        },
        {
            "name": "update_schema",
            "description": "Update an existing schema (rename, change comment, or change owner)",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "full_schema_name": {
                        "type": "string",
                        "description": "Full schema name (catalog.schema format)"
                    },
                    "new_name": {
                        "type": "string",
                        "description": "New name for the schema"
                    },
                    "comment": {
                        "type": "string",
                        "description": "New comment/description"
                    },
                    "owner": {
                        "type": "string",
                        "description": "New owner"
                    }
                },
                "required": ["full_schema_name"]
            }
        },
        {
            "name": "delete_schema",
            "description": "Delete a schema from Unity Catalog",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "full_schema_name": {
                        "type": "string",
                        "description": "Full schema name (catalog.schema format)"
                    }
                },
                "required": ["full_schema_name"]
            }
        },
        {
            "name": "create_table",
            "description": "Create a new table in Unity Catalog with specified columns",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "catalog_name": {
                        "type": "string",
                        "description": "Name of the catalog"
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Name of the schema"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to create"
                    },
                    "columns": {
                        "type": "array",
                        "description": "List of column definitions. Each column should be an object with 'name' and 'type_name' keys. Example: [{\"name\": \"id\", \"type_name\": \"INT\"}, {\"name\": \"value\", \"type_name\": \"STRING\"}]",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": "Column name"
                                },
                                "type_name": {
                                    "type": "string",
                                    "description": "Column type (e.g., INT, STRING, BOOLEAN, DOUBLE, TIMESTAMP)"
                                }
                            },
                            "required": ["name", "type_name"]
                        }
                    },
                    "table_type": {
                        "type": "string",
                        "description": "Type of table: MANAGED or EXTERNAL",
                        "default": "MANAGED"
                    },
                    "comment": {
                        "type": "string",
                        "description": "Optional description of the table"
                    },
                    "storage_location": {
                        "type": "string",
                        "description": "Storage location for EXTERNAL tables"
                    }
                },
                "required": ["catalog_name", "schema_name", "table_name", "columns"]
            }
        },
        {
            "name": "delete_table",
            "description": "Delete a table from Unity Catalog",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "full_table_name": {
                        "type": "string",
                        "description": "Full table name (catalog.schema.table format)"
                    }
                },
                "required": ["full_table_name"]
            }
        },
    ]
