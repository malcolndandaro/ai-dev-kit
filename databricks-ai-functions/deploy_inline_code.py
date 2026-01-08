# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Pipeline Builder - Inline Code Approach

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.31.0 mlflow>=2.0.0 --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from mlflow.pyfunc import PythonModel
import pandas as pd
import json
import sys

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

class PipelineBuilderModel(PythonModel):
    """MLflow model with inlined code - no external dependencies."""
    
    def load_context(self, context):
        """Load dependencies when model is initialized."""
        print("✅ PipelineBuilderModel loaded")
    
    def predict(self, context, model_input):
        """Handle inference requests."""
        import sys
        import os
        
        # Add workspace paths
        workspace_paths = [
            '/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-tools-core',
            '/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions',
        ]
        for path in workspace_paths:
            if path not in sys.path:
                sys.path.insert(0, path)
        
        # Now import (from workspace)
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

model_name = "main.ai_functions.pipeline_builder"

# COMMAND ----------

with mlflow.start_run(run_name="pipeline_builder_inline") as run:
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
    
    # Log model with ONLY databricks-sdk as dependency
    # Code will be loaded from workspace paths at runtime
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=PipelineBuilderModel(),
        registered_model_name=model_name,
        signature=signature,
        input_example=input_example,
        pip_requirements=[
            "databricks-sdk>=0.31.0",
            "mlflow>=2.0.0",
            "pandas>=1.0.0",
        ],
    )
    run_id = run.info.run_id

print(f"✅ Model registered: {model_name}")
print(f"   Run ID: {run_id}")
print(f"   No custom wheels - uses workspace paths!")

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
    print(f"No existing endpoint")

# Create new
endpoint = w.serving_endpoints.create(
    name=endpoint_name,
    config=EndpointCoreConfigInput(
        served_entities=[
            ServedEntityInput(
                entity_name=model_name,
                entity_version=model_version,
                scale_to_zero_enabled=True,
                workload_size="Small"
            )
        ],
        name=endpoint_name
    )
)
print(f"✅ Endpoint created: {endpoint_name}")
print(f"🔗 https://fe-ai-strategy.cloud.databricks.com/ml/endpoints/{endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait and Test

# COMMAND ----------

print("⏳ Waiting for endpoint (should be faster without custom wheels)...")
for i in range(40):  # 40 * 15s = 10 minutes max
    ep = w.serving_endpoints.get(endpoint_name)
    state = ep.state.ready if ep.state else "UNKNOWN"
    print(f"  {i*15}s: {state}")
    
    if state == "READY":
        print(f"✅ Endpoint is READY!")
        break
    time.sleep(15)
else:
    print(f"⚠️  Still not ready after 10 minutes")

# COMMAND ----------

# Test
test_input = {
    "dataframe_records": [{
        "user_request": "Create a test pipeline from samples.tpch.customer target: main.ai_dev_kit",
        "model": "databricks-gpt-5-2"
    }]
}

print("🧪 Testing endpoint...")
response = w.serving_endpoints.query(endpoint_name, dataframe_records=test_input["dataframe_records"])
print(response)

