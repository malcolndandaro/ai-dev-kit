# App Refactoring: stdio MCP Client

## Overview

Refactored `databricks-mcp-app` to use the MCP stdio server via subprocess communication, creating architectural consistency with the `databricks-mcp-server`.

## Changes Made

### 1. Created MCP stdio Client (`databricks-mcp-app/mcp/client.py`)

New `MCPStdioClient` class that:
- Starts `databricks-mcp-server` as a subprocess
- Communicates via stdin/stdout using JSON-RPC 2.0
- Manages process lifecycle (start, stop, graceful shutdown)
- Handles request/response queuing with threading
- Provides methods: `initialize()`, `list_tools()`, `call_tool()`

**Key Features:**
- Line-buffered stdio communication
- Per-request response queuing
- 30-second request timeout
- Proper error handling and logging
- Context manager support (`with MCPStdioClient()`)

### 2. Updated Orchestrator (`databricks-mcp-app/llm/orchestrator.py`)

**Before:**
- Imported tools directly from `databricks-mcp-core` and `databricks-mcp-server.tools`
- Used `tools/registry.py` to get handlers
- Called Python functions directly

**After:**
- Starts MCP stdio server subprocess on initialization
- Fetches tools via `mcp_client.list_tools()`
- Converts MCP format to OpenAI format for LLM
- Executes tools via `mcp_client.call_tool()`

**Changes:**
- `_load_tools()`: Now starts MCP client and fetches tools
- `_convert_mcp_to_openai_format()`: Converts tool definitions
- `_execute_tool()`: Calls MCP client instead of direct handlers

### 3. Removed Direct Dependencies

- **Deleted:** `databricks-mcp-app/tools/` directory (including `registry.py`)
- **No Changes Needed:** `requirements.txt` (never had direct databricks-mcp-core dependency)

### 4. Updated App API (`databricks-mcp-app/app.py`)

- Enhanced `/api/tools` endpoint to handle compute tools category
- Better error handling for missing tools

## Architecture

### Before (Bundled Tools)

```
databricks-mcp-app
├── app.py (FastAPI)
├── llm/orchestrator.py
└── tools/registry.py ──┐
                        ├──> databricks-mcp-core (direct import)
                        └──> databricks-mcp-server.tools (direct import)
```

### After (MCP stdio Client)

```
databricks-mcp-app
├── app.py (FastAPI)
├── llm/orchestrator.py
└── mcp/client.py ──> stdio subprocess ──> databricks-mcp-server
                                            ├── stdio_server.py
                                            ├── message_handler.py
                                            └── databricks-mcp-core
```

## Configuration

### Environment Variables

```bash
# Optional: Custom MCP server command
MCP_SERVER_COMMAND="python -m databricks_mcp_server.stdio_server"

# Or with uv
MCP_SERVER_COMMAND="uv run python -m databricks_mcp_server.stdio_server"

# Existing variables (unchanged)
LLM_ENDPOINT="databricks-claude-sonnet-4-5"
DATABRICKS_CONFIG_PROFILE="your-profile"
```

Default: `"python -m databricks_mcp_server.stdio_server"`

## Benefits

1. **Architectural Consistency**
   - Both Claude Code and the app use the same MCP server
   - Single source of truth for tools

2. **Standard Protocol**
   - Uses official MCP protocol (JSON-RPC 2.0 over stdio)
   - Follows Model Context Protocol best practices

3. **Easier Maintenance**
   - Update tools in one place (`databricks-mcp-server`)
   - App doesn't need to know implementation details

4. **Separation of Concerns**
   - App: UI, sessions, LLM orchestration
   - Server: Databricks operations, tool execution

5. **Future Flexibility**
   - Can easily switch to HTTP transport for remote deployment
   - Can share one server across multiple app instances

## Testing

To test the refactored app:

```bash
cd databricks-mcp-app

# Ensure databricks-mcp-server is installed
cd ../databricks-mcp-server
pip install -e .
cd ../databricks-mcp-app

# Start the app
uvicorn app:app --host 0.0.0.0 --port 8080

# Test via web UI
open http://localhost:8080

# Or test via API
curl -X POST http://localhost:8080/api/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "List all catalogs"}'
```

## What Stayed the Same

- **SSE for UI Streaming:** The app still uses Server-Sent Events for streaming tool execution updates to the web UI (unrelated to MCP protocol)
- **OpenAI Format:** Still converts tools to OpenAI format for the LLM (Databricks Model Serving uses OpenAI-compatible API)
- **API Endpoints:** All existing endpoints work the same
- **Frontend:** No changes to the Vue.js UI

## Rollback

If needed, rollback by:
1. Restore `databricks-mcp-app/tools/` from git history
2. Revert changes to `llm/orchestrator.py`
3. Remove `databricks-mcp-app/mcp/` directory

```bash
git checkout HEAD~1 -- databricks-mcp-app/tools/
git checkout HEAD~1 -- databricks-mcp-app/llm/orchestrator.py
rm -rf databricks-mcp-app/mcp/
```

## Files Changed

**New:**
- `databricks-mcp-app/mcp/__init__.py`
- `databricks-mcp-app/mcp/client.py`
- `REFACTORING.md` (this file)

**Modified:**
- `databricks-mcp-app/llm/orchestrator.py`
- `databricks-mcp-app/app.py`

**Deleted:**
- `databricks-mcp-app/tools/` (entire directory)

## Next Steps

1. **Test thoroughly** with existing functionality
2. **Add health checks** for MCP subprocess
3. **Add metrics/logging** for MCP communication
4. **Consider auto-restart** if MCP server crashes
5. **Document deployment** with containerization
