# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy with Wheels from UC Volume (Simplest Approach)

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.31.0 mlflow>=2.0.0 build wheel --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from mlflow.pyfunc import PythonModel
import pandas as pd
import json
import subprocess
import os

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Wheels and Upload to UC Volume

# COMMAND ----------

# Ensure volume exists
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

catalog, schema = "main", "ai_functions"
volume_name = "packages"

try:
    w.volumes.read(f"{catalog}.{schema}.{volume_name}")
    print(f"✅ Volume exists: {catalog}.{schema}.{volume_name}")
except:
    print(f"Creating volume...")
    w.volumes.create(catalog_name=catalog, schema_name=schema, name=volume_name, volume_type="MANAGED")
    print(f"✅ Created volume: {catalog}.{schema}.{volume_name}")

# COMMAND ----------

# Build wheels
print("📦 Building wheels...")

# databricks-tools-core
subprocess.run(
    ["python", "-m", "build", "--wheel", "--outdir", "/tmp/wheels"],
    cwd="/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-tools-core",
    check=True
)

# databricks-ai-functions
subprocess.run(
    ["python", "-m", "build", "--wheel", "--outdir", "/tmp/wheels"],
    cwd="/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions",
    check=True
)

wheels = [f for f in os.listdir("/tmp/wheels") if f.endswith(".whl")]
print(f"✅ Built {len(wheels)} wheels")

# COMMAND ----------

# Upload to UC Volume
import shutil

volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}"

for wheel in wheels:
    src = f"/tmp/wheels/{wheel}"
    dst = f"{volume_path}/{wheel}"
    shutil.copy(src, dst)
    print(f"✅ Uploaded: {wheel}")

print(f"\n📦 Wheels in volume: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Model (No Code Bundling!)

# COMMAND ----------

class PipelineBuilderModel(PythonModel):
    """Simple model - packages installed from volume."""
    
    def load_context(self, context):
        print("✅ PipelineBuilderModel loaded")
    
    def predict(self, context, model_input):
        # Packages already installed from volume
        from databricks_ai_functions.sdp.build_pipeline import build_pipeline
        import re
        
        if isinstance(model_input, pd.DataFrame):
            user_request = model_input["user_request"].iloc[0]
            model = model_input.get("model", pd.Series(["databricks-gpt-5-2"])).iloc[0]
        else:
            user_request = model_input.get("user_request", "")
            model = model_input.get("model", "databricks-gpt-5-2")
        
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
            return pd.DataFrame([{"response": json.dumps({
                "status": "error",
                "message": str(e),
                "traceback": traceback.format_exc(),
            }, indent=2)}])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Register Model with Volume Wheels

# COMMAND ----------

model_name = "main.ai_functions.pipeline_builder"

# Get wheel paths from volume
wheel_files = [f"{volume_path}/{w}" for w in wheels]
print(f"Wheel paths: {wheel_files}")

# COMMAND ----------

with mlflow.start_run(run_name="pipeline_builder_volume_wheels") as run:
    input_example = pd.DataFrame({
        "user_request": ["Create pipeline from samples.tpch.customer target: main.ai_dev_kit"],
        "model": ["databricks-gpt-5-2"]
    })
    
    output_example = pd.DataFrame([{
        "response": json.dumps({"status": "success", "pipeline_id": "example-123"})
    }])
    
    signature = mlflow.models.infer_signature(input_example, output_example)
    
    # Reference wheels from UC Volume in pip_requirements
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
        ] + wheel_files,  # Add volume wheel paths
    )
    run_id = run.info.run_id

print(f"✅ Model registered: {model_name}")
print(f"   With wheels from: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Wait for Model

# COMMAND ----------

import time

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
    raise Exception("Timeout")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Endpoint

# COMMAND ----------

from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

endpoint_name = "pipeline-builder-agent-tool"

# Delete existing
try:
    w.serving_endpoints.delete(endpoint_name)
    print(f"🗑️  Deleted existing endpoint")
    time.sleep(10)
except:
    pass

# Get token
try:
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
except:
    token = w.config.token

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
print(f"🔗 https://fe-ai-strategy.cloud.databricks.com/ml/endpoints/{endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Wait and Test

# COMMAND ----------

print("⏳ Waiting for endpoint...")
for i in range(40):
    ep = w.serving_endpoints.get(endpoint_name)
    state = ep.state.ready if ep.state else "UNKNOWN"
    print(f"  {i*15}s: {state}")
    
    if state == "READY":
        print(f"✅ Ready!")
        break
    time.sleep(15)

# COMMAND ----------

# Test
test_input = {"dataframe_records": [{
    "user_request": "generate a cdc sdp pipeline with iceberg on top of main.ai_dev_kit.customers. append 5 to the end of any tables you create",
    "model": "databricks-gpt-5-2"
}]}

print("🧪 Testing...")
response = w.serving_endpoints.query(endpoint_name, dataframe_records=test_input["dataframe_records"])
print(response)

