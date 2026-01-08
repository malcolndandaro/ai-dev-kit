# Pipeline Builder - Model Serving Endpoint Setup

## ✅ What Was Done

We created a solution to make `build_pipeline` callable as an AI agent tool using **Databricks Model Serving Endpoints**.

### Why This Approach?

Unity Catalog Functions have limitations:
- ❌ Cannot use custom Python packages with `ENVIRONMENT` clause (hangs/fails)
- ❌ Cannot call `WorkspaceClient()` (no auth context in serverless)
- ❌ Cannot access workspace files reliably

Model Serving Endpoints provide:
- ✅ Full Databricks SDK access
- ✅ Custom package loading (via workspace paths in model code)
- ✅ Native integration with AI agent frameworks
- ✅ Monitoring, logging, and scalability
- ✅ REST API for any client

## 📦 Files Created

1. **`deploy_endpoint_notebook.py`** → Databricks notebook for deployment
   - Registers `PipelineBuilderModel` to Unity Catalog
   - Creates serving endpoint `pipeline-builder-agent-tool`
   - Tests the endpoint

2. **`AI_AGENT_TOOL_SERVING_ENDPOINT.md`** → Complete usage documentation
   - Integration examples (REST, SDK, LangChain, OpenAI)
   - Request/response formats
   - Monitoring and troubleshooting

3. **Helper scripts** (not needed for final solution):
   - `create_serving_endpoint.py` - initial attempt (had UC sync issues)
   - `deploy_endpoint.py` - simplified version (same issue)

## 🚀 Deployment Steps

### 1. Upload and Run Notebook (MUST RUN IN DATABRICKS)

The notebook has been uploaded to:
```
/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions/deploy_pipeline_builder_endpoint
```

**Open it here**: https://fe-ai-strategy.cloud.databricks.com/workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions/deploy_pipeline_builder_endpoint

**Run all cells** to:
1. Define `PipelineBuilderModel` (MLflow PyFunc model)
2. Register to UC as `main.ai_functions.pipeline_builder`
3. Wait for model version to be `READY`
4. Create endpoint `pipeline-builder-agent-tool`
5. Test with a sample request

> **Why in Databricks?** Model registration to Unity Catalog from local environments causes models to get stuck in `PENDING_REGISTRATION`. Running in Databricks ensures proper sync.

### 2. Verify Deployment

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Check model
model = w.model_versions.list(full_name="main.ai_functions.pipeline_builder")
for v in model:
    print(f"Version {v.version}: {v.status.value}")

# Check endpoint
endpoint = w.serving_endpoints.get("pipeline-builder-agent-tool")
print(f"Endpoint state: {endpoint.state.ready}")
```

### 3. Test the Endpoint

```python
response = w.serving_endpoints.query(
    name="pipeline-builder-agent-tool",
    dataframe_records=[{
        "user_request": "Create a pipeline from samples.tpch.customer target: main.ai_dev_kit",
        "model": "databricks-gpt-5-2"
    }]
)

print(response.predictions[0]["response"])
```

## 🤖 Using as an AI Agent Tool

### Simple Example (Databricks SDK)

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

response = w.serving_endpoints.query(
    name="pipeline-builder-agent-tool",
    dataframe_records=[{
        "user_request": "Create bronze-silver-gold pipeline from samples.nyctaxi.trips target: main.ai_dev_kit",
        "model": "databricks-gpt-5-2"
    }]
)

result = response.predictions[0]["response"]
print(result)  # JSON with pipeline_id, pipeline_url, etc.
```

### LangChain Integration

```python
from langchain.tools import Tool
from databricks.sdk import WorkspaceClient

def build_pipeline_tool(user_request: str) -> str:
    w = WorkspaceClient()
    response = w.serving_endpoints.query(
        name="pipeline-builder-agent-tool",
        dataframe_records=[{"user_request": user_request, "model": "databricks-gpt-5-2"}]
    )
    return response.predictions[0]["response"]

tool = Tool(
    name="build_sdp_pipeline",
    description="Build Spark Declarative Pipeline from natural language",
    func=build_pipeline_tool
)

# Use in your agent
agent = create_agent(tools=[tool, ...])
```

## 📊 Monitoring

**Endpoint UI**: https://fe-ai-strategy.cloud.databricks.com/ml/endpoints/pipeline-builder-agent-tool

View:
- Request/response logs
- Latency metrics
- Error rates
- Model version serving

## 🎯 What This Enables

AI agents can now:
1. Receive natural language requests like "Create a customer analytics pipeline"
2. Call the `pipeline-builder-agent-tool` endpoint
3. Get back a deployed, running Spark Declarative Pipeline with:
   - Pipeline ID and URL
   - Bronze, Silver, Gold layers (as requested)
   - Proper table references and dependencies
   - Started update/refresh

## 📝 Complete Documentation

See **`AI_AGENT_TOOL_SERVING_ENDPOINT.md`** for:
- Full API reference
- Integration examples (REST, SDK, LangChain, OpenAI)
- Request/response schemas
- Troubleshooting guide
- Architecture diagram

## ⚡ Next Steps

1. **Run the deployment notebook in Databricks** (link above)
2. Wait for endpoint to be `READY` (~5-10 minutes)
3. Test with sample requests
4. Integrate into your AI agent

---

**Status**: ✅ Ready to deploy (just run the notebook!)

**Endpoint Name**: `pipeline-builder-agent-tool`

**Model**: `main.ai_functions.pipeline_builder`

**Access**: REST API, Databricks SDK, LangChain, OpenAI function calling

