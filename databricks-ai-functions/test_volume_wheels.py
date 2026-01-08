# Databricks notebook source
# MAGIC %md
# MAGIC # Test Model with UC Volume Wheels (Before Deploying)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Wheels and Upload to UC Volume

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

catalog, schema = "main", "ai_functions"
volume_name = "packages"

# Ensure volume exists
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
print(f"✅ Built {len(wheels)} wheels:")
for w in wheels:
    size = os.path.getsize(f"/tmp/wheels/{w}") / 1024
    print(f"   {w}: {size:.1f} KB")

# COMMAND ----------

# Upload to UC Volume
import shutil

volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}"

for wheel in wheels:
    src = f"/tmp/wheels/{wheel}"
    dst = f"{volume_path}/{wheel}"
    shutil.copy(src, dst)
    print(f"✅ Uploaded: {wheel}")

print(f"\n📦 Wheels available at: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Model

# COMMAND ----------

class PipelineBuilderModel(PythonModel):
    """Model using packages from UC Volume."""
    
    def load_context(self, context):
        print("✅ PipelineBuilderModel loaded")
    
    def predict(self, context, model_input):
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
# MAGIC ## Step 3: Log Model (No Registration)

# COMMAND ----------

# Get wheel paths
wheel_files = [f"{volume_path}/{whl}" for whl in wheels]
print(f"Wheel paths for pip_requirements:")
for wf in wheel_files:
    print(f"   {wf}")

# COMMAND ----------

with mlflow.start_run(run_name="test_volume_wheels") as run:
    input_example = pd.DataFrame({
        "user_request": ["Create pipeline from samples.tpch.customer target: main.ai_dev_kit"],
        "model": ["databricks-gpt-5-2"]
    })
    
    output_example = pd.DataFrame([{
        "response": json.dumps({"status": "success", "pipeline_id": "example-123"})
    }])
    
    signature = mlflow.models.infer_signature(input_example, output_example)
    
    # Log with volume wheels
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=PipelineBuilderModel(),
        signature=signature,
        input_example=input_example,
        pip_requirements=[
            "databricks-sdk>=0.31.0",
            "mlflow>=2.0.0",
            "pandas>=1.0.0",
        ] + wheel_files,
    )
    
    model_uri = f"runs:/{run.info.run_id}/model"
    run_id = run.info.run_id

print(f"✅ Model logged: {model_uri}")
print(f"   With wheels from UC Volume")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Load and Test

# COMMAND ----------

print("🔄 Loading model from MLflow...")
loaded_model = mlflow.pyfunc.load_model(model_uri)
print("✅ Model loaded!")

# COMMAND ----------

# Set up auth
try:
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
except:
    token = w.config.token

os.environ["DATABRICKS_HOST"] = w.config.host
os.environ["DATABRICKS_TOKEN"] = token

print(f"✅ Auth configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Test with CDC Request

# COMMAND ----------

test_input = pd.DataFrame({
    "user_request": ["generate a cdc sdp pipeline with iceberg on top of main.ai_dev_kit.customers. append 6 to the end of any tables you create"],
    "model": ["databricks-gpt-5-2"]
})

print("🧪 Testing CDC request...")
print("="*60)
print(f"Request: {test_input['user_request'].iloc[0]}")
print("\n💡 Should generate: CREATE FLOW ... AUTO CDC INTO syntax")

try:
    result = loaded_model.predict(test_input)
    print("\n✅ SUCCESS!")
    print("\nResult:")
    print(result["response"].iloc[0])
    
    # Parse and check
    response_json = json.loads(result["response"].iloc[0])
    
    if response_json.get("status") == "success":
        print("\n" + "="*60)
        print("✅ MODEL WORKS! Check if SQL has CDC syntax:")
        print("="*60)
        print(f"\nPipeline URL: {response_json.get('pipeline_url')}")
        print(f"\n👉 Open pipeline and verify SQL files have:")
        print(f"   CREATE FLOW ... AUTO CDC INTO")
        print(f"   NOT: CREATE STREAMING TABLE")
    else:
        print(f"\n⚠️  Pipeline had error: {response_json.get('message')}")
        
except Exception as e:
    print(f"\n❌ FAILED: {e}")
    import traceback
    print(traceback.format_exc())

