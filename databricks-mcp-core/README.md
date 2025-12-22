# Databricks MCP Core

High-level, AI-assistant-friendly Python functions for building Databricks projects.

## Overview

The `databricks-mcp-core` package contains reusable, opinionated functions for interacting with Databricks platform components. It is organized by product line for scalability:

- **unity_catalog/** - Unity Catalog operations (catalogs, schemas, tables)
- **compute/** - Compute and execution operations
- **spark_declarative_pipelines/** - SDP operations (coming soon)
- **agent_bricks/** - Agent Bricks operations (coming soon)
- **dabs/** - DAB generation (coming soon)

## Installation

```bash
pip install -e .
```

## Usage

```python
from databricks_mcp_core.client import DatabricksClient
from databricks_mcp_core.unity_catalog import catalogs, schemas, tables

# Initialize client
client = DatabricksClient(
    host="https://your-workspace.databricks.com",
    token="your-token"
)

# Or use environment variables: DATABRICKS_HOST, DATABRICKS_TOKEN
client = DatabricksClient()

# List catalogs
all_catalogs = catalogs.list_catalogs(client)
for catalog in all_catalogs:
    print(catalog["name"])

# Create a schema
schema = schemas.create_schema(
    client,
    catalog_name="main",
    schema_name="my_schema",
    comment="Example schema"
)

# Create a table
table = tables.create_table(
    client,
    catalog_name="main",
    schema_name="my_schema",
    table_name="my_table",
    columns=[
        {"name": "id", "type_name": "INT"},
        {"name": "value", "type_name": "STRING"}
    ]
)
```

## Architecture

This is a pure Python library with no dependencies on MCP protocol code. It can be used standalone in notebooks, scripts, or other Python projects.

For MCP server functionality, see the `databricks-mcp-server` package.

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black databricks_mcp_core/
```
