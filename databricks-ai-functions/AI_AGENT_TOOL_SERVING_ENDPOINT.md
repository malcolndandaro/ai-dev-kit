# Using Pipeline Builder as an AI Agent Tool

## Overview

The `build_pipeline` function has been deployed as a **Databricks Model Serving Endpoint** that can be called by AI agents to dynamically create Spark Declarative Pipelines from natural language.

## Deployment

### Step 1: Deploy the Endpoint

Run the notebook in Databricks:

**Notebook Path**: `/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions/deploy_pipeline_builder_endpoint`

**Direct Link**: https://fe-ai-strategy.cloud.databricks.com/workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions/deploy_pipeline_builder_endpoint

The notebook will:
1. ✅ Register `PipelineBuilderModel` to Unity Catalog (`main.ai_functions.pipeline_builder`)
2. ✅ Create serving endpoint `pipeline-builder-agent-tool`
3. ✅ Test the endpoint with a sample request

### Why Run in Databricks?

Model registration to Unity Catalog **must** run from within Databricks for proper synchronization. Local registration causes models to get stuck in `PENDING_REGISTRATION` status.

## Using the Endpoint

### Endpoint Details

- **Name**: `pipeline-builder-agent-tool`
- **URL**: `https://fe-ai-strategy.cloud.databricks.com/serving-endpoints/pipeline-builder-agent-tool/invocations`
- **Model**: `main.ai_functions.pipeline_builder`
- **Workload**: Small (scales to zero when idle)

### Input Format

```json
{
  "dataframe_records": [{
    "user_request": "<natural language description of pipeline>",
    "model": "databricks-gpt-5-2"
  }]
}
```

#### Parameters

- **user_request** (string, required): Natural language description of the pipeline to build
  - Include source table names (catalog.schema.table)
  - Specify transformations (bronze, silver, gold)
  - Mention target location with "target: catalog.schema"
  
- **model** (string, optional): LLM model endpoint name (default: "databricks-gpt-5-2")

### Output Format

```json
{
  "response": "{
    \"status\": \"success\",
    \"pipeline_id\": \"abc-123\",
    \"pipeline_url\": \"https://...\",
    \"pipeline_name\": \"customer_analytics_pipeline\",
    \"catalog\": \"main\",
    \"schema\": \"ai_dev_kit\",
    \"message\": \"Pipeline created and update started\"
  }"
}
```

## Integration Examples

### 1. REST API (Direct)

```python
import requests
import os

response = requests.post(
    "https://fe-ai-strategy.cloud.databricks.com/serving-endpoints/pipeline-builder-agent-tool/invocations",
    headers={
        "Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"
    },
    json={
        "dataframe_records": [{
            "user_request": """
                Create a customer analytics pipeline from samples.tpch.customer:
                1. Bronze: Load raw customer data
                2. Silver: Filter valid customers, add nation info
                3. Gold: Aggregate by market segment
                Target: main.ai_dev_kit
            """,
            "model": "databricks-gpt-5-2"
        }]
    }
)

result = response.json()
print(result)
```

### 2. Databricks SDK

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

response = w.serving_endpoints.query(
    name="pipeline-builder-agent-tool",
    dataframe_records=[{
        "user_request": "Create pipeline from samples.nyctaxi.trips target: main.ai_dev_kit",
        "model": "databricks-gpt-5-2"
    }]
)

print(response)
```

### 3. LangChain / AI Agents

```python
from databricks_langchain import ChatDatabricks
from langchain.agents import create_structured_chat_agent, AgentExecutor
from langchain.tools import Tool

# Define the tool
def build_pipeline_tool(user_request: str) -> str:
    """
    Build a Spark Declarative Pipeline from natural language.
    
    Args:
        user_request: Natural language description of the pipeline
    
    Returns:
        JSON string with pipeline details and URL
    """
    from databricks.sdk import WorkspaceClient
    import json
    
    w = WorkspaceClient()
    response = w.serving_endpoints.query(
        name="pipeline-builder-agent-tool",
        dataframe_records=[{
            "user_request": user_request,
            "model": "databricks-gpt-5-2"
        }]
    )
    
    return response.predictions[0]["response"]

# Register as a tool
pipeline_tool = Tool(
    name="build_sdp_pipeline",
    description="""
    Build a Spark Declarative Pipeline from natural language description.
    Use this when user asks to create data pipelines, ETL workflows, or 
    bronze-silver-gold architectures. Requires source table names and 
    optionally a target catalog.schema.
    """,
    func=build_pipeline_tool
)

# Create agent with the tool
llm = ChatDatabricks(endpoint="databricks-gpt-5-2")
agent = create_structured_chat_agent(
    llm=llm,
    tools=[pipeline_tool],
    # ... other agent config
)

executor = AgentExecutor(agent=agent, tools=[pipeline_tool], verbose=True)

# Use it
result = executor.invoke({
    "input": "Create a data pipeline from samples.tpch.customer to main.ai_dev_kit"
})
```

### 4. OpenAI-Compatible Function Calling

```python
import openai

# Define as a function
functions = [{
    "name": "build_sdp_pipeline",
    "description": "Build a Spark Declarative Pipeline from natural language",
    "parameters": {
        "type": "object",
        "properties": {
            "user_request": {
                "type": "string",
                "description": "Natural language description of the pipeline including source tables and transformations"
            }
        },
        "required": ["user_request"]
    }
}]

# In your function handler
def execute_function(function_name, arguments):
    if function_name == "build_sdp_pipeline":
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        
        response = w.serving_endpoints.query(
            name="pipeline-builder-agent-tool",
            dataframe_records=[{
                "user_request": arguments["user_request"],
                "model": "databricks-gpt-5-2"
            }]
        )
        
        return response.predictions[0]["response"]
```

## Example Requests

### Basic Pipeline

```json
{
  "dataframe_records": [{
    "user_request": "Create a pipeline from samples.tpch.customer target: main.ai_dev_kit",
    "model": "databricks-gpt-5-2"
  }]
}
```

### Multi-Layer Pipeline

```json
{
  "dataframe_records": [{
    "user_request": "Create a bronze-silver-gold pipeline from samples.nyctaxi.trips. Bronze loads raw data, silver filters valid trips and adds datetime features, gold aggregates by date and location. Target: main.ai_dev_kit",
    "model": "databricks-gpt-5-2"
  }]
}
```

### SCD Type 2 Pipeline

```json
{
  "dataframe_records": [{
    "user_request": "Create an SCD Type 2 pipeline for customer dimension. Track changes to customer name, address, and phone. Use samples.tpch.customer as source. Target: main.ai_dev_kit",
    "model": "databricks-gpt-5-2"
  }]
}
```

## Monitoring

### Check Endpoint Status

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
endpoint = w.serving_endpoints.get("pipeline-builder-agent-tool")

print(f"State: {endpoint.state.ready}")
print(f"Config: {endpoint.state.config_update}")
```

### View Endpoint Logs

Navigate to: https://fe-ai-strategy.cloud.databricks.com/ml/endpoints/pipeline-builder-agent-tool

Click on "Logs" tab to see:
- Request/response logs
- Model prediction logs
- Errors and exceptions

## Architecture

```
AI Agent (LangChain, OpenAI, etc.)
    ↓
    ↓ [Function Call / Tool Use]
    ↓
Model Serving Endpoint (pipeline-builder-agent-tool)
    ↓
    ↓ [Executes PipelineBuilderModel.predict()]
    ↓
build_pipeline() function
    ↓
    ↓ [Calls Databricks SDK]
    ↓
- databricks_tools_core (create/update pipeline)
- databricks_ai_functions (LLM for SQL generation)
    ↓
    ↓ [Creates/Updates]
    ↓
Spark Declarative Pipeline (SDP)
```

## Troubleshooting

### Model Not Found

If you get "RegisteredModel ... does not exist":
1. Run the deployment notebook in Databricks
2. Wait for model version to reach `READY` status
3. Check: `w.model_versions.list(full_name="main.ai_functions.pipeline_builder")`

### Endpoint Not Ready

If endpoint is in `UPDATING` state:
1. Wait a few minutes for deployment
2. Check status: `w.serving_endpoints.get("pipeline-builder-agent-tool")`
3. View logs in the UI for errors

### Permission Errors

Ensure the serving endpoint's service principal or user has:
- `USE CATALOG` on target catalog
- `USE SCHEMA` and `CREATE TABLE` on target schema
- `EXECUTE` on LLM endpoint (e.g., `databricks-gpt-5-2`)
- `EXECUTE` on UC functions if using them

### Prediction Errors

Check the endpoint logs for:
- Module import errors → Verify workspace paths in model code
- Build pipeline failures → Check source table permissions
- LLM errors → Verify LLM endpoint is accessible

## Next Steps

1. ✅ Deploy the endpoint (run notebook in Databricks)
2. ✅ Test with sample requests
3. ✅ Integrate into your AI agent framework
4. ✅ Monitor usage and errors
5. ✅ Scale endpoint as needed (change workload size)

---

**Questions?** Check:
- Endpoint UI: https://fe-ai-strategy.cloud.databricks.com/ml/endpoints/pipeline-builder-agent-tool
- Model Registry: https://fe-ai-strategy.cloud.databricks.com/explore/data/models/main/ai_functions/pipeline_builder
- Logs & Metrics in endpoint UI

