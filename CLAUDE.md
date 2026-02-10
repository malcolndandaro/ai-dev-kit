<!-- ai-dev-kit:start -->
# AI Dev Kit - Databricks Development Environment

This project provides Databricks skills and an MCP server for Claude Code. The user works with Databricks daily.

## IMPORTANT: Tool Preference Order

YOU MUST follow this priority when helping with Databricks tasks:

1. **Skills first** - Check if an installed skill matches the task before using general knowledge. Skills contain verified patterns, MCP tool usage examples, and Databricks-specific workflows that are more accurate than training data.
2. **MCP tools second** - Use `mcp__databricks__*` tools for executing actions (SQL, pipelines, jobs, dashboards). These handle auth, workspace context, and audit logging automatically.
3. **General knowledge last** - Only fall back to training data when no skill or MCP tool covers the topic.

Why: Skills and MCP tools reflect current Databricks APIs and patterns. Training data may be outdated or use deprecated approaches.

## Skill-to-Task Routing

| When the user wants to... | Load this skill | Key MCP tools |
|---|---|---|
| Build data pipelines (SDP/DLT) | `spark-declarative-pipelines` | `create_or_update_pipeline`, `start_update`, `get_pipeline_events` |
| Create or manage Jobs | `databricks-jobs` | `create_job`, `run_job_now`, `wait_for_run`, `get_run_output` |
| Build AI/BI dashboards | `aibi-dashboards` | `create_or_update_dashboard`, `publish_dashboard` |
| Build AI agents (KA, Genie, MAS) | `agent-bricks` | `create_or_update_ka`, `create_or_update_genie`, `create_or_update_mas` |
| Deploy with Asset Bundles | `asset-bundles` | N/A (uses `databricks bundle` CLI) |
| Generate synthetic data | `synthetic-data-generation` | `execute_sql` for Spark-based generation |
| Work with Unity Catalog | `databricks-unity-catalog` | `get_catalogs`, `get_tables`, `manage_uc_objects` |
| Deploy ML models or agents | `model-serving` | `get_serving_endpoint_status`, `query_serving_endpoint` |
| Evaluate GenAI agents | `mlflow-evaluation` | `execute_sql` for trace queries |
| Build RAG / semantic search | `vector-search` | `create_vector_search_index`, `query_vector_search_index` |
| Build Python apps (Dash/Streamlit) | `databricks-app-python` | `upload_folder`, `upload_file` |
| Build full-stack apps (FastAPI+React) | `databricks-app-apx` | `upload_folder`, `upload_file` |
| Set up Databricks auth/config | `databricks-config` | N/A (uses `databricks` CLI) |
| Use Genie for NL-to-SQL | `databricks-genie` | `ask_genie`, `ask_genie_followup` |
| Work with Lakebase (PostgreSQL) | `lakebase-provisioned` | `create_database_instance`, `generate_database_credential` |
| Generate PDFs for RAG testing | `unstructured-pdf-generation` | `upload_to_volume` |
| Build streaming pipelines | `spark-structured-streaming` | `execute_sql`, `create_or_update_pipeline` |
| Use Databricks SDK/CLI/REST API | `databricks-python-sdk` | All tools (skill teaches SDK patterns) |
| Look up Databricks docs | `databricks-docs` | N/A (reference skill) |

## MCP Tool Quick Reference

- **SQL**: `execute_sql`, `execute_sql_multi`, `get_table_details` - run queries and inspect tables
- **Compute**: `execute_databricks_command`, `run_python_file_on_databricks` - run code on clusters
- **Files**: `upload_file`, `upload_folder`, `upload_to_volume`, `download_from_volume` - file operations
- **Pipelines**: `create_or_update_pipeline`, `start_update`, `get_pipeline_events` - SDP lifecycle
- **Jobs**: `create_job`, `run_job_now`, `wait_for_run`, `get_run_output` - job orchestration
- **Agents**: `create_or_update_ka`, `create_or_update_genie`, `create_or_update_mas` - AI agents
- **Dashboards**: `create_or_update_dashboard`, `publish_dashboard` - AI/BI dashboards
- **Unity Catalog**: `get_catalogs`, `get_schemas`, `get_tables`, `manage_uc_objects` - governance

## Workflow

1. Before implementing, load the relevant skill to get verified patterns and examples
2. Use MCP tools for all Databricks operations instead of shell commands with `databricks` CLI
3. Validate SQL queries with `execute_sql` before embedding them in pipelines or dashboards
4. When the user asks to create resources, prefer `create_or_update_*` tools (idempotent) over plain `create_*`
<!-- ai-dev-kit:end -->
