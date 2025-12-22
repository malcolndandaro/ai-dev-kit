# Databricks MCP Server

MCP (Model Context Protocol) server that exposes Databricks operations as AI-friendly tools.

## Overview

This is a thin wrapper around `databricks-mcp-core` that implements the MCP protocol over FastAPI with Server-Sent Events (SSE) transport.

## Available Tools

### Compute Operations (4 tools)
- `create_context` - Create execution context on cluster
- `execute_command_with_context` - Execute code with state persistence
- `destroy_context` - Clean up execution context
- `databricks_command` - One-off command execution

### Unity Catalog Operations (11 tools)
- **Catalogs:** `list_catalogs`, `get_catalog`
- **Schemas:** `list_schemas`, `get_schema`, `create_schema`, `update_schema`, `delete_schema`
- **Tables:** `list_tables`, `get_table`, `create_table`, `delete_table`

## Installation

```bash
# Install both core and server packages
pip install -e ../databricks-mcp-core
pip install -e .
```

## Configuration

Three authentication methods (in priority order):

### 1. Databricks CLI Profile (Recommended)

Use your existing `~/.databrickscfg` profile:

```bash
export DATABRICKS_CONFIG_PROFILE=ai-strat
```

Or in your `.env` file:

```
DATABRICKS_CONFIG_PROFILE=ai-strat
```

### 2. Environment Variables

```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
```

### 3. .env File

```
DATABRICKS_HOST=https://your-workspace.databricks.com
DATABRICKS_TOKEN=dapi...
```

## Running the Server

```bash
# Development mode
python -m databricks_mcp_server.server

# Production mode with uvicorn
uvicorn databricks_mcp_server.server:app --host 0.0.0.0 --port 8000
```

## MCP Endpoints

- `GET /sse` - SSE endpoint for MCP communication
- `POST /message` - JSON-RPC message handling
- `GET /` - Health check

## Usage with Claude Code

Configure in your Claude Code MCP settings:

```json
{
  "mcpServers": {
    "databricks": {
      "url": "http://localhost:8000/sse",
      "transport": "sse"
    }
  }
}
```

## Architecture

```
databricks-mcp-server/
├── server.py              # FastAPI app + MCP protocol
└── tools/
    ├── unity_catalog.py   # UC tool wrappers
    └── compute.py         # Compute tool wrappers
```

Each tool wrapper:
1. Imports functions from `databricks-mcp-core`
2. Wraps them with MCP response formatting
3. Provides JSON schema definitions
