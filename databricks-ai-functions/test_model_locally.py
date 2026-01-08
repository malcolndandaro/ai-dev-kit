# Databricks notebook source
# MAGIC %md
# MAGIC # Test Model Locally BEFORE Deploying

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Copy Code to Local Filesystem

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Model (Same as Deployment)

# COMMAND ----------

class PipelineBuilderModel(PythonModel):
    """MLflow model with properly bundled code."""
    
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
            }
            return pd.DataFrame([{"response": json.dumps(error, indent=2)}])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Log Model (BUT DON'T REGISTER)

# COMMAND ----------

with mlflow.start_run(run_name="test_local") as run:
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
    
    # Log WITHOUT registering (no registered_model_name)
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=PipelineBuilderModel(),
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
    
    model_uri = f"runs:/{run.info.run_id}/model"
    run_id = run.info.run_id

print(f"✅ Model logged (not registered)")
print(f"   Model URI: {model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Skills Were Bundled

# COMMAND ----------

print("🔍 Checking if skills were bundled...")
import os

# Check if skills directory exists in the model
skills_check_paths = [
    "/tmp/databricks_skills/sdp/scd-query-patterns.md",
    "/tmp/databricks_skills/sdp/SKILL.md"
]

for path in skills_check_paths:
    if os.path.exists(path):
        size = os.path.getsize(path) / 1024
        print(f"✅ Found: {path} ({size:.1f} KB)")
    else:
        print(f"❌ MISSING: {path}")

print("\n💡 After log_model, check model artifacts...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Load Model and Test Locally

# COMMAND ----------

print("🔄 Loading model from MLflow...")
loaded_model = mlflow.pyfunc.load_model(model_uri)
print("✅ Model loaded!")

# Check if skills are in the loaded model's code directory
import sys
print(f"\n🔍 Python path includes:")
for p in sys.path:
    if 'databricks_skills' in p or 'mlflow' in p:
        print(f"   {p}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test Prediction (With Auth Simulation)

# COMMAND ----------

import os

# Simulate model serving environment variables
print("🔐 Setting up auth environment (simulating model serving)...")

# Get current workspace config
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Get token from notebook context (dbutils)
try:
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    print(f"✅ Got token from notebook context")
except:
    # Fallback to w.config.token if available
    token = w.config.token
    print(f"✅ Got token from WorkspaceClient config")

os.environ["DATABRICKS_HOST"] = w.config.host
os.environ["DATABRICKS_TOKEN"] = token

print(f"✅ DATABRICKS_HOST: {os.environ['DATABRICKS_HOST']}")
print(f"✅ DATABRICKS_TOKEN: {'*' * 20} (hidden)")

# COMMAND ----------

test_input = pd.DataFrame({
    "user_request": ["generate a cdc sdp pipeline with iceberg on top of main.ai_dev_kit.customers. append 3 to the end of any tables you create"],
    "model": ["databricks-gpt-5-2"]
})

print("🧪 Testing CDC/SCD Type 2 request...")
print("="*60)
print(f"\nUser request: {test_input['user_request'].iloc[0]}")
print(f"\n💡 Expected: Should generate CREATE FLOW ... AUTO CDC INTO syntax")
print(f"   NOT: Regular CREATE STREAMING TABLE")

try:
    result = loaded_model.predict(test_input)
    print("\n✅ SUCCESS! Model works locally with auth!")
    print("\nResult:")
    print(result)
    
    # Parse the response
    response_json = json.loads(result["response"].iloc[0])
    print(f"\n📊 Pipeline Status: {response_json.get('status')}")
    
    if response_json.get('status') == 'error':
        print(f"⚠️  Pipeline build had an error:")
        print(f"   Message: {response_json.get('message')}")
        if 'auth' in response_json.get('message', '').lower():
            print(f"\n❌ AUTH ERROR DETECTED!")
            print(f"   The auth fix didn't work - check llm.py, build_pipeline.py, skills.py")
        print(f"\n⚠️  FIX THIS BEFORE DEPLOYING")
    else:
        print(f"   Pipeline ID: {response_json.get('pipeline_id')}")
        print(f"   Pipeline URL: {response_json.get('pipeline_url')}")
        
        print("\n" + "="*60)
        print("✅ MODEL IS READY TO DEPLOY!")
        print("="*60)
        print("\nNext step: Run the deploy_correct_code_paths notebook")
    
except Exception as e:
    print("\n❌ FAILED! Fix this before deploying!")
    print(f"\nError: {e}")
    import traceback
    print(f"\nTraceback:\n{traceback.format_exc()}")
    
    print("\n" + "="*60)
    print("⚠️  DO NOT DEPLOY - FIX THE ERROR ABOVE FIRST")
    print("="*60)

