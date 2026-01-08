# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Pipeline Builder - Build Wheels First

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.31.0 mlflow>=2.0.0 build wheel --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Build wheels for our custom packages
import subprocess
import os

print("📦 Building wheels for custom packages...")

# Build databricks-tools-core wheel
print("\n1. Building databricks-tools-core...")
result = subprocess.run(
    ["python", "-m", "build", "--wheel", "--outdir", "/tmp/wheels"],
    cwd="/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-tools-core",
    capture_output=True,
    text=True
)
print(result.stdout)
if result.returncode != 0:
    print(f"Error: {result.stderr}")

# Build databricks-ai-functions wheel
print("\n2. Building databricks-ai-functions...")
result = subprocess.run(
    ["python", "-m", "build", "--wheel", "--outdir", "/tmp/wheels"],
    cwd="/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions",
    capture_output=True,
    text=True
)
print(result.stdout)
if result.returncode != 0:
    print(f"Error: {result.stderr}")

# List the wheels
print("\n✅ Wheels created:")
for f in os.listdir("/tmp/wheels"):
    print(f"   - {f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Model with Wheels

# COMMAND ----------

import mlflow
from mlflow.pyfunc import PythonModel
import pandas as pd
import json
import glob

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

class PipelineBuilderModel(PythonModel):
    """MLflow model that wraps build_pipeline."""
    
    def load_context(self, context):
        """Load dependencies when model is initialized."""
        print("✅ PipelineBuilderModel loaded")
    
    def predict(self, context, model_input):
        """Handle inference requests."""
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
                "catalog": catalog,
                "schema": schema,
            }
            return pd.DataFrame([{"response": json.dumps(error, indent=2)}])

# COMMAND ----------

model_name = "main.ai_functions.pipeline_builder"

# Get the wheel file paths
wheel_files = glob.glob("/tmp/wheels/*.whl")
print(f"Found wheels: {wheel_files}")

# COMMAND ----------

with mlflow.start_run(run_name="pipeline_builder_with_wheels") as run:
    # Create input/output examples for signature
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
    
    # Include wheels as pip requirements
    pip_reqs = [
        "databricks-sdk>=0.31.0",
        "mlflow>=2.0.0",
        "pandas>=1.0.0",
    ] + wheel_files  # Add the wheel files
    
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=PipelineBuilderModel(),
        registered_model_name=model_name,
        signature=signature,
        input_example=input_example,
        pip_requirements=pip_reqs,
    )
    run_id = run.info.run_id

print(f"✅ Model registered: {model_name}")
print(f"   Run ID: {run_id}")
print(f"   Wheels included: {len(wheel_files)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Model to be READY

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
# MAGIC ## Delete Existing Endpoint (Clean Slate)

# COMMAND ----------

from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

endpoint_name = "pipeline-builder-agent-tool"

# Delete existing endpoint to start fresh
try:
    w.serving_endpoints.delete(endpoint_name)
    print(f"🗑️  Deleted existing endpoint: {endpoint_name}")
    print(f"   Waiting 10 seconds...")
    import time
    time.sleep(10)
except Exception as e:
    if "does not exist" in str(e) or "RESOURCE_DOES_NOT_EXIST" in str(e):
        print(f"No existing endpoint to delete")
    else:
        print(f"Note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Fresh Serving Endpoint

# COMMAND ----------

# Create new endpoint
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
print(f"🔗 Endpoint URL: https://fe-ai-strategy.cloud.databricks.com/ml/endpoints/{endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Endpoint

# COMMAND ----------

print("Waiting for endpoint to be ready...")
for i in range(60):
    ep = w.serving_endpoints.get(endpoint_name)
    state = ep.state.ready if ep.state else "UNKNOWN"
    print(f"  State: {state}")
    if state == "READY":
        print(f"✅ Endpoint is READY!")
        break
    time.sleep(5)

# COMMAND ----------

# Test query
test_input = {
    "dataframe_records": [{
        "user_request": "Create a simple test pipeline from samples.tpch.customer target: main.ai_dev_kit",
        "model": "databricks-gpt-5-2"
    }]
}

print("🧪 Testing endpoint...")
response = w.serving_endpoints.query(endpoint_name, dataframe_records=test_input["dataframe_records"])
print("\n✅ Response:")
print(response)

