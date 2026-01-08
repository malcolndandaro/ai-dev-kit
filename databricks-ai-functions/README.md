Databricks AI Functions
=======================

AI-powered builder functions for Databricks:
- SDP pipelines (natural language → pipeline)
- Jobs (natural language → job config)
- Workflows (natural language → multi-task)

Status: Initial version with `sdp.build_pipeline`.

Install (editable)
------------------

```bash
cd /Users/cal.reynolds/Downloads/skunkworks/ai-dev-kit/databricks-ai-functions
uv pip install -e .
```

Usage
-----

Python:
```python
from databricks_ai_functions.sdp import build_pipeline

result = build_pipeline("Create a pipeline from customers to customers_summary")
print(result["pipeline_url"])
```

MCP (Claude/Cursor):
- Tool: `databricks_build_sdp_pipeline`
- Requires `databricks-mcp-server` installed and configured in Claude/Cursor.

UC SQL:
```sql
SELECT main.ai_functions.build_sdp_pipeline('Create pipeline from orders to daily_sales') AS result;
```

Defaults and Configuration
--------------------------
- Workspace upload root: `/Workspace/Shared/ai-dev-kit/pipelines`
- Default skills path: `databricks-skills/sdp` (local repo)
- Default model: `databricks-gpt-5-2`

Development
-----------
Editable install:
```bash
cd /Users/cal.reynolds/Downloads/skunkworks/ai-dev-kit/databricks-ai-functions
uv pip install -e .[test]
pytest -q
```


