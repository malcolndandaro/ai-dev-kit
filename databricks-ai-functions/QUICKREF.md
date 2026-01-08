# Quick Reference: Pipeline Builder AI Agent Tool

## Deploy (Run This First)

**Notebook**: https://fe-ai-strategy.cloud.databricks.com/workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions/deploy_pipeline_builder_endpoint

Click **"Run All"** → Wait 5-10 mins → ✅ Done!

---

## Use the Tool

### Python SDK

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
response = w.serving_endpoints.query(
    name="pipeline-builder-agent-tool",
    dataframe_records=[{
        "user_request": "Create pipeline from samples.tpch.customer target: main.ai_dev_kit",
        "model": "databricks-gpt-5-2"
    }]
)

print(response.predictions[0]["response"])
```

### LangChain Tool

```python
from langchain.tools import Tool
from databricks.sdk import WorkspaceClient

def build_pipeline(request: str) -> str:
    w = WorkspaceClient()
    resp = w.serving_endpoints.query(
        name="pipeline-builder-agent-tool",
        dataframe_records=[{"user_request": request, "model": "databricks-gpt-5-2"}]
    )
    return resp.predictions[0]["response"]

tool = Tool(
    name="build_sdp_pipeline",
    description="Build Spark Declarative Pipeline from natural language",
    func=build_pipeline
)

# Add to your agent's tools
agent = create_agent(tools=[tool, ...])
```

### REST API

```bash
curl -X POST \
  https://fe-ai-strategy.cloud.databricks.com/serving-endpoints/pipeline-builder-agent-tool/invocations \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "dataframe_records": [{
      "user_request": "Create pipeline from samples.nyctaxi.trips target: main.ai_dev_kit",
      "model": "databricks-gpt-5-2"
    }]
  }'
```

---

## Monitor

**Endpoint UI**: https://fe-ai-strategy.cloud.databricks.com/ml/endpoints/pipeline-builder-agent-tool

**Model**: https://fe-ai-strategy.cloud.databricks.com/explore/data/models/main/ai_functions/pipeline_builder

---

## Example Requests

### Simple Pipeline
```json
{
  "user_request": "Create pipeline from samples.tpch.customer target: main.ai_dev_kit"
}
```

### Bronze-Silver-Gold
```json
{
  "user_request": "Create bronze-silver-gold pipeline from samples.nyctaxi.trips. Bronze loads raw, silver filters valid trips, gold aggregates by date. Target: main.ai_dev_kit"
}
```

### SCD Type 2
```json
{
  "user_request": "Create SCD Type 2 pipeline tracking customer changes from samples.tpch.customer target: main.ai_dev_kit"
}
```

---

## Response Format

```json
{
  "response": "{
    \"status\": \"success\",
    \"pipeline_id\": \"abc-123-def\",
    \"pipeline_url\": \"https://...\",
    \"pipeline_name\": \"customer_pipeline\",
    \"catalog\": \"main\",
    \"schema\": \"ai_dev_kit\",
    \"message\": \"Pipeline created and update started\"
  }"
}
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Endpoint not found | Run deployment notebook first |
| Model not ready | Wait ~5 mins, check endpoint UI |
| Permission error | Ensure USE CATALOG, CREATE TABLE permissions |
| Prediction fails | Check endpoint logs in UI |

---

## Full Documentation

- **Setup Guide**: `MODEL_SERVING_SETUP.md`
- **Integration Guide**: `AI_AGENT_TOOL_SERVING_ENDPOINT.md`
- **Summary**: `DEPLOYMENT_SUMMARY.md`

