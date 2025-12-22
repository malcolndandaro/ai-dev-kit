# Databricks AI Dev Kit

MCP (Model Context Protocol) server for building Databricks projects with AI coding assistants like Claude Code and Cursor.

## Overview

High-level, AI-assistant-friendly tools for Databricks operations. Organized by product line for scalability.

## Architecture

```
databricks-mcp-core/              # Pure Python library
├── unity_catalog/                # Catalogs, schemas, tables
├── compute/                      # Execution contexts
├── spark_declarative_pipelines/  # Pipeline management & workspace files
├── agent_bricks/                 # Agent Bricks (future)
└── dabs/                        # DAB generation (future)

databricks-mcp-server/            # MCP protocol wrapper
├── server.py                    # FastAPI + SSE
└── tools/                       # MCP tool wrappers
```

## Installation

```bash
# Install packages
pip install -e databricks-mcp-core
pip install -e databricks-mcp-server

# Set profile
export DATABRICKS_CONFIG_PROFILE=your-profile

# Run server
python -m databricks_mcp_server.server
```

## Available Tools (30)

**Unity Catalog (11):**
- Catalogs: list_catalogs, get_catalog
- Schemas: list_schemas, get_schema, create_schema, update_schema, delete_schema
- Tables: list_tables, get_table, create_table, delete_table

**Compute (4):**
- create_context, execute_command_with_context, destroy_context, databricks_command

**Spark Declarative Pipelines (15):**
- Pipeline Management (9): create_pipeline, get_pipeline, update_pipeline_config, delete_pipeline, start_pipeline_update, validate_pipeline, get_pipeline_update_status, stop_pipeline, get_pipeline_events
- Workspace Files (6): list_pipeline_files, get_pipeline_file_status, read_pipeline_file, write_pipeline_file, create_pipeline_directory, delete_pipeline_path

## Usage with Claude Code

Add to your MCP settings:

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

Then ask Claude to interact with your Databricks workspace!

## Documentation

- [databricks-mcp-core README](databricks-mcp-core/README.md) - Core package details
- [databricks-mcp-server README](databricks-mcp-server/README.md) - Server configuration

## License

© 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].

## 📄 Third-Party Package Licenses

&copy; 2025 Databricks, Inc. All rights reserved. The source in this project is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| Package | License | Copyright |
|---------|---------|-----------|
| [requests](https://github.com/psf/requests) | Apache License 2.0 | Copyright Kenneth Reitz |
| [pydantic](https://github.com/pydantic/pydantic) | MIT License | Copyright (c) 2017 Samuel Colvin |
| [fastapi](https://github.com/tiangolo/fastapi) | MIT License | Copyright (c) 2018 Sebastián Ramírez |
| [uvicorn](https://github.com/encode/uvicorn) | BSD 3-Clause License | Copyright © 2017-present, Encode OSS Ltd. |
| [sse-starlette](https://github.com/sysid/sse-starlette) | BSD 3-Clause License | Copyright (c) 2020, sysid |
| [python-dotenv](https://github.com/theskumar/python-dotenv) | BSD 3-Clause License | Copyright (c) 2014, Saurabh Kumar |
