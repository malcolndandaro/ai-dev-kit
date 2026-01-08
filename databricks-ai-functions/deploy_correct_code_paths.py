# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Pipeline Builder - Correct code_paths Approach

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.31.0 mlflow>=2.0.0 --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from mlflow.pyfunc import PythonModel
import pandas as pd
import json
import shutil
import os

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy Workspace Code to Local Filesystem

# COMMAND ----------

# Create local copies of the packages
print("📦 Copying packages to local filesystem...")

# Copy databricks-tools-core
if os.path.exists("/tmp/databricks_tools_core"):
    shutil.rmtree("/tmp/databricks_tools_core")
shutil.copytree(
    "/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-tools-core/databricks_tools_core",
    "/tmp/databricks_tools_core"
)
print("✅ Copied databricks_tools_core")

# Copy databricks-ai-functions
if os.path.exists("/tmp/databricks_ai_functions"):
    shutil.rmtree("/tmp/databricks_ai_functions")
shutil.copytree(
    "/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions/databricks_ai_functions",
    "/tmp/databricks_ai_functions"
)
print("✅ Copied databricks_ai_functions")

# Copy databricks-skills (CRITICAL for LLM prompts!)
if os.path.exists("/tmp/databricks_skills"):
    shutil.rmtree("/tmp/databricks_skills")
shutil.copytree(
    "/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-skills",
    "/tmp/databricks_skills"
)
print("✅ Copied databricks_skills (SCD patterns, etc.)")

print(f"\n📁 Local packages ready at:")
print(f"   /tmp/databricks_tools_core")
print(f"   /tmp/databricks_ai_functions")
print(f"   /tmp/databricks_skills")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Model

# COMMAND ----------

class PipelineBuilderModel(PythonModel):
    """MLflow model with properly bundled code."""
    
    def load_context(self, context):
        """Load dependencies when model is initialized."""
        print("✅ PipelineBuilderModel loaded")
    
    def predict(self, context, model_input):
        """Handle inference requests."""
        # Code is already in sys.path via MLflow's code_paths mechanism
        from databricks_ai_functions.sdp.build_pipeline import build_pipeline
        import re
        
        # Parse input
        if isinstance(model_input, pd.DataFrame):
            user_request = model_input["user_request"].iloc[0]
            model = model_input.get("model", pd.Series(["databricks-gpt-5-2"])).iloc[0]
        else:
            user_request = model_input.get("user_request", "")
            model = model_input.get("model", "databricks-gpt-5-2")
        
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
            return pd.DataFrame([{"response": json.dumps(result, indent=2)}])
        except Exception as e:
            import traceback
            error = {
                "status": "error",
                "message": str(e),
                "traceback": traceback.format_exc(),
            }
            return pd.DataFrame([{"response": json.dumps(error, indent=2)}])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Model with code_paths

# COMMAND ----------

model_name = "main.ai_functions.pipeline_builder"

with mlflow.start_run(run_name="pipeline_builder_bundled") as run:
    input_example = pd.DataFrame({
        "user_request": ["Create a pipeline from samples.tpch.customer target: main.ai_dev_kit"],
        "model": ["databricks-gpt-5-2"]
    })
    
    output_example = pd.DataFrame([{
        "response": json.dumps({
            "status": "success",
            "pipeline_id": "example-123",
            "pipeline_url": "https://...",
            "message": "Pipeline created"
        })
    }])
    
    signature = mlflow.models.infer_signature(input_example, output_example)
    
    # Use code_paths with LOCAL filesystem paths (this is the key!)
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=PipelineBuilderModel(),
        registered_model_name=model_name,
        signature=signature,
        input_example=input_example,
        code_paths=[
            "/tmp/databricks_tools_core",
            "/tmp/databricks_ai_functions",
            "/tmp/databricks_skills"  # CRITICAL: Include skills!
        ],
        pip_requirements=[
            "databricks-sdk>=0.31.0",
            "mlflow>=2.0.0",
            "pandas>=1.0.0",
        ],
    )
    run_id = run.info.run_id

print(f"✅ Model registered: {model_name}")
print(f"   Run ID: {run_id}")
print(f"   Code bundled via code_paths from local filesystem!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Model

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import time

w = WorkspaceClient()

print("Waiting for model version to be READY...")
for i in range(60):
    versions = list(w.model_versions.list(full_name=model_name))
    latest = max(versions, key=lambda v: int(v.version))
    
    print(f"  Version {latest.version}: {latest.status.value}")
    if latest.status.value == "READY":
        print(f"✅ Model version {latest.version} is READY!")
        model_version = latest.version
        break
    time.sleep(2)
else:
    raise Exception("Timeout waiting for model to be READY")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Endpoint

# COMMAND ----------

from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

endpoint_name = "pipeline-builder-agent-tool"

# Delete existing
try:
    w.serving_endpoints.delete(endpoint_name)
    print(f"🗑️  Deleted existing endpoint")
    time.sleep(10)
except:
    print(f"No existing endpoint to delete")

# Get token for environment variables
try:
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    print(f"✅ Got token from notebook context")
except:
    token = w.config.token
    print(f"✅ Got token from WorkspaceClient config")

# Create new
endpoint = w.serving_endpoints.create(
    name=endpoint_name,
    config=EndpointCoreConfigInput(
        served_entities=[
            ServedEntityInput(
                entity_name=model_name,
                entity_version=model_version,
                scale_to_zero_enabled=True,
                workload_size="Small",
                environment_vars={
                    "DATABRICKS_HOST": w.config.host,
                    "DATABRICKS_TOKEN": token
                }
            )
        ],
        name=endpoint_name
    )
)
print(f"✅ Endpoint created: {endpoint_name}")
print(f"   With DATABRICKS_HOST and DATABRICKS_TOKEN env vars")
print(f"🔗 https://fe-ai-strategy.cloud.databricks.com/ml/endpoints/{endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait and Test

# COMMAND ----------

print("⏳ Waiting for endpoint to be ready...")
for i in range(40):  # 10 minutes max
    ep = w.serving_endpoints.get(endpoint_name)
    state = ep.state.ready if ep.state else "UNKNOWN"
    config_state = ep.state.config_update if ep.state else "UNKNOWN"
    
    print(f"  {i*15}s: State={state}, Config={config_state}")
    
    if state == "READY":
        print(f"✅ Endpoint is READY!")
        break
    time.sleep(15)
else:
    print(f"⚠️  Timeout after 10 minutes")
    print(f"   Check UI: https://fe-ai-strategy.cloud.databricks.com/ml/endpoints/{endpoint_name}")

# COMMAND ----------

# Test
test_input = {
    "dataframe_records": [{
        "user_request": "Create a test pipeline from samples.tpch.customer target: main.ai_dev_kit",
        "model": "databricks-gpt-5-2"
    }]
}

print("🧪 Testing endpoint...")
try:
    response = w.serving_endpoints.query(endpoint_name, dataframe_records=test_input["dataframe_records"])
    print("\n✅ SUCCESS!")
    print(response)
except Exception as e:
    print(f"\n❌ Error: {e}")
    print(f"\nCheck logs: https://fe-ai-strategy.cloud.databricks.com/ml/endpoints/{endpoint_name}")

