# Databricks MCP Server

A simple [FastMCP](https://github.com/jlowin/fastmcp) server that exposes Databricks operations as MCP tools for AI assistants like Claude Code.

## Quick Start

### Step 1: Clone the repository

```bash
git clone https://github.com/databricks-solutions/ai-dev-kit.git
cd ai-dev-kit
```

### Step 2: Install the packages

```bash
# Install the core library
cd databricks-tools-core
uv pip install -e .

# Install the MCP server
cd ../databricks-mcp-server
uv pip install -e .
```

### Step 3: Configure Databricks authentication

```bash
# Option 1: Environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"

# Option 2: Use a profile from ~/.databrickscfg
export DATABRICKS_CONFIG_PROFILE="your-profile"
```

### Step 4: Add MCP server to Claude Code

Add to your project's `.claude/mcp.json` (create the file if it doesn't exist):

```json
{
  "mcpServers": {
    "databricks": {
      "command": "uv",
      "args": ["run", "python", "-m", "databricks_mcp_server.server"],
      "cwd": "/path/to/ai-dev-kit/databricks-mcp-server",
      "defer_loading": true
    }
  }
}
```

**Replace `/path/to/ai-dev-kit`** with the actual path where you cloned the repo.

**Note:** `"defer_loading": true` improves startup time by not loading all tools upfront.

#### Advanced Configuration

You can pass additional arguments to configure the server:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "uv",
      "args": [
        "run", "python", "-m", "databricks_mcp_server.server",
        "--profile", "my-profile",
        "--cluster-id", "0123-456789-abcdef",
        "--warehouse-id", "abc123def456",
        "--serverless", "True"
      ],
      "cwd": "/path/to/ai-dev-kit/databricks-mcp-server",
      "defer_loading": true
    }
  }
}
```

| Argument | Description |
|----------|-------------|
| `--profile` | Databricks config profile from `~/.databrickscfg` |
| `--cluster-id` | Default cluster ID for compute operations |
| `--warehouse-id` | Default warehouse ID for SQL operations |
| `--serverless` | Set true to use serverless compute instead of cluster (does not apply to Databricks SQL) |

When `--cluster-id` or `--warehouse-id` are configured, they become the default values for all tool calls, eliminating the need to pass them every time. You may set `--serverless` to "True" to override need for cluster-id.

### Step 5 (Recommended): Install Databricks skills

The MCP server works best with **Databricks skills** that teach Claude best practices:

```bash
# In your project directory (not ai-dev-kit)
cd /path/to/your/project
curl -sSL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/databricks-skills/install_skills.sh | bash
```

### Step 6: Start Claude Code

```bash
cd /path/to/your/project
claude
```

Claude now has both:
- **Skills** (knowledge) - patterns and best practices in `.claude/skills/`
- **MCP Tools** (actions) - Databricks operations via the MCP server

## Manual Setup from Another Project

If you're setting up the MCP server from a different project (not in the `ai-dev-kit` repository), follow these steps:

### Option 1: Install from Git (Recommended)

Install both packages directly from the GitHub repository:

```bash
# Install databricks-tools-core
uv pip install git+https://github.com/databricks-solutions/ai-dev-kit.git#subdirectory=databricks-tools-core

# Install databricks-mcp-server
uv pip install git+https://github.com/databricks-solutions/ai-dev-kit.git#subdirectory=databricks-mcp-server
```

### Option 2: Clone and Install Locally

If you want to use editable installs for development:

```bash
# Clone the repository
git clone https://github.com/databricks-solutions/ai-dev-kit.git
cd ai-dev-kit

# Install both packages in editable mode
uv pip install -e databricks-tools-core
uv pip install -e databricks-mcp-server
```

### Option 3: Use the Setup Script

If you've cloned the repository, use the provided setup script:

```bash
cd databricks-mcp-server
bash setup.sh
```

The script will:
- Check for `uv` (install from https://astral.sh/uv/install.sh if needed)
- Create a virtual environment
- Install both packages in editable mode
- Verify the installation

### Configure MCP Server

After installation, add the MCP server config to your default location or a project specific location, such as `mcp.json` for Claude Code or `.cursor/mcp.json` for Cursor:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "/absolute/path/to/databricks-mcp-server/.venv/bin/python",
      "args": ["/absolute/path/to/databricks-mcp-server/run_server.py"]
    }
  }
}
```

### Configure Authentication

Set up Databricks authentication using environment variables or a config profile:

```bash
# Option 1: Environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"

# Option 2: Use a profile from ~/.databrickscfg
export DATABRICKS_CONFIG_PROFILE="your-profile"
```

Or pass authentication via command arguments in `mcp.json`:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "/absolute/path/to/databricks-mcp-server/.venv/bin/python",
      "args": ["/absolute/path/to/databricks-mcp-server/run_server.py",
        "--profile", "my-profile",
        "--cluster-id", "0123-456789-abcdef",
        "--warehouse-id", "abc123def456"
      ]
    }
  }
}
```

### Verify Installation

Test that the server can be imported:

```bash
python -c "import databricks_mcp_server; print('✓ MCP server installed')"
```

## Available Tools

### SQL Operations

| Tool | Description |
|------|-------------|
| `execute_sql` | Execute a SQL query on a Databricks SQL Warehouse |
| `execute_sql_multi` | Execute multiple SQL statements with parallel execution |
| `list_warehouses` | List all SQL warehouses in the workspace |
| `get_best_warehouse` | Get the ID of the best available warehouse |
| `get_table_details` | Get table schema and statistics |

### Compute

| Tool | Description |
|------|-------------|
| `execute_databricks_command` | Execute code on a Databricks cluster |
| `run_python_file_on_databricks` | Run a local Python file on a cluster |
| `list_clusters` | List clusters in the workspace |
| `get_best_cluster` | Get the ID of the best available cluster |

### File Operations

| Tool | Description |
|------|-------------|
| `upload_folder` | Upload a local folder to Databricks workspace (parallel) |
| `upload_file` | Upload a single file to workspace |

### Spark Declarative Pipelines (SDP)

| Tool | Description |
|------|-------------|
| `create_pipeline` | Create a new SDP pipeline |
| `get_pipeline` | Get pipeline details and configuration |
| `update_pipeline` | Update pipeline configuration |
| `delete_pipeline` | Delete a pipeline |
| `start_update` | Start pipeline run or dry-run validation |
| `get_update` | Get update status (QUEUED, RUNNING, COMPLETED, FAILED) |
| `stop_pipeline` | Stop a running pipeline |
| `get_pipeline_events` | Get error messages for debugging |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Claude Code                              │
│                                                              │
│  Skills (knowledge)          MCP Tools (actions)            │
│  └── .claude/skills/         └── .claude/mcp.json           │
│      ├── sdp-writer              └── databricks server      │
│      ├── dabs-writer                                        │
│      └── ...                                                │
└──────────────────────────────┬──────────────────────────────┘
                               │ MCP Protocol (stdio)
                               ▼
┌─────────────────────────────────────────────────────────────┐
│              databricks-mcp-server (FastMCP)                 │
│                                                              │
│  tools/sql.py ──────┐                                       │
│  tools/compute.py ──┼──► @mcp.tool decorators               │
│  tools/file.py ─────┤                                       │
│  tools/pipelines.py ┘                                       │
└──────────────────────────────┬──────────────────────────────┘
                               │ Python imports
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                   databricks-tools-core                        │
│                                                              │
│  sql/         compute/       file/         pipelines/       │
│  └── execute  └── run_code   └── upload    └── create/run   │
└──────────────────────────────┬──────────────────────────────┘
                               │ Databricks SDK
                               ▼
                    ┌─────────────────────┐
                    │  Databricks         │
                    │  Workspace          │
                    └─────────────────────┘
```

## Development

The server is intentionally simple - each tool file just imports functions from `databricks-tools-core` and decorates them with `@mcp.tool`.

To add a new tool:

1. Add the function to `databricks-tools-core`
2. Create a wrapper in `databricks_mcp_server/tools/`
3. Import it in `server.py`

Example:

```python
# tools/my_module.py
from databricks_tools_core.my_module import my_function as _my_function
from ..server import mcp

@mcp.tool
def my_function(arg1: str, arg2: int = 10) -> dict:
    """Tool description shown to the AI."""
    return _my_function(arg1=arg1, arg2=arg2)
```

## License

© Databricks, Inc. See [LICENSE.md](../LICENSE.md).
