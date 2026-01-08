# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Pipeline Builder as Agent Tool (Agent Framework)
# MAGIC 
# MAGIC Using Databricks Agent Framework - simpler and faster than custom model serving

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.31.0 mlflow>=2.14.0 langchain>=0.1.0 langchain-community databricks-agents --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the Tool Function

# COMMAND ----------

import sys
sys.path.extend([
    '/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-tools-core',
    '/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions',
])

# COMMAND ----------

def build_sdp_pipeline_tool(user_request: str, model: str = "databricks-gpt-5-2") -> str:
    """
    Build a Spark Declarative Pipeline from natural language description.
    
    Args:
        user_request: Natural language description of the pipeline to build.
                     Include source tables and target location (e.g., "target: main.schema")
        model: LLM model endpoint to use (default: databricks-gpt-5-2)
    
    Returns:
        JSON string with pipeline details including pipeline_id, pipeline_url, and status
    """
    from databricks_ai_functions.sdp.build_pipeline import build_pipeline
    import json
    import re
    
    # Extract catalog/schema
    catalog, schema = "main", "default"
    m = re.search(r'(?:target|to|into):\s*(\w+)\.(\w+)', user_request, re.I)
    if m:
        catalog, schema = m.groups()
    
    try:
        result = build_pipeline(
            user_request=user_request,
            catalog=catalog,
            schema=schema,
            model=model,
            start_run=True,
        )
        return json.dumps(result, indent=2)
    except Exception as e:
        import traceback
        error = {
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc(),
        }
        return json.dumps(error, indent=2)

# COMMAND ----------

# Test it locally
print("🧪 Testing tool locally...")
result = build_sdp_pipeline_tool(
    "Create a simple pipeline from samples.tpch.customer target: main.ai_dev_kit"
)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register as Agent

# COMMAND ----------

import mlflow
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain.tools import Tool
from langchain_community.chat_models import ChatDatabricks

mlflow.set_registry_uri("databricks-uc")

# Define the tool
pipeline_tool = Tool(
    name="build_sdp_pipeline",
    description="""Build a Spark Declarative Pipeline (SDP) from natural language.
    Use this when user asks to create data pipelines, ETL workflows, or bronze-silver-gold architectures.
    Provide: source table names (catalog.schema.table) and optionally target location.""",
    func=build_sdp_pipeline_tool
)

# COMMAND ----------

# Create agent with the tool
llm = ChatDatabricks(endpoint="databricks-gpt-5-2", max_tokens=4000)

from langchain import hub
prompt = hub.pull("hwchase17/openai-functions-agent")

agent = create_tool_calling_agent(llm, [pipeline_tool], prompt)
agent_executor = AgentExecutor(
    agent=agent,
    tools=[pipeline_tool],
    verbose=True,
    handle_parsing_errors=True
)

# COMMAND ----------

# Log the agent to MLflow
with mlflow.start_run(run_name="pipeline_builder_agent"):
    mlflow.langchain.log_model(
        agent_executor,
        artifact_path="agent",
        registered_model_name="main.ai_functions.pipeline_builder_agent"
    )
    run_id = mlflow.active_run().info.run_id

print(f"✅ Agent logged: runs:/{run_id}/agent")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Agent

# COMMAND ----------

from databricks import agents

# Deploy the agent
deployment = agents.deploy(
    model_name="main.ai_functions.pipeline_builder_agent",
    model_version=1,
    endpoint_name="pipeline-builder-agent-tool"
)

print(f"✅ Agent deployed!")
print(f"🔗 https://fe-ai-strategy.cloud.databricks.com/ml/endpoints/pipeline-builder-agent-tool")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Deployed Agent

# COMMAND ----------

import time
print("Waiting for agent to be ready...")
time.sleep(30)

# Test query
response = agents.query(
    endpoint_name="pipeline-builder-agent-tool",
    inputs={
        "messages": [{
            "role": "user",
            "content": "Create a pipeline from samples.tpch.customer target: main.ai_dev_kit"
        }]
    }
)

print("✅ Response:")
print(response)

