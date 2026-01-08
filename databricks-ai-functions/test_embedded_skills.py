# Databricks notebook source
# MAGIC %md
# MAGIC # Test with Embedded Skills (Simplest)

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.31.0 mlflow>=2.0.0 build --quiet

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
# MAGIC ## Step 1: Build Wheel (with embedded skills)

# COMMAND ----------

print("📦 Building databricks-ai-functions wheel (with embedded skills)...")

subprocess.run(
    ["python", "-m", "build", "--wheel", "--outdir", "/tmp/wheels"],
    cwd="/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions",
    check=True
)

wheels = [f for f in os.listdir("/tmp/wheels") if f.startswith("databricks_ai_functions")]
print(f"✅ Built: {wheels[0]}")

wheel_path = f"/tmp/wheels/{wheels[0]}"
size = os.path.getsize(wheel_path) / 1024
print(f"   Size: {size:.1f} KB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Install and Test Locally

# COMMAND ----------

# Install the wheel
subprocess.run(["pip", "install", "--force-reinstall", wheel_path], check=True)
print("✅ Wheel installed")

# Also need tools-core
subprocess.run(
    ["pip", "install", "-e", "/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-tools-core"],
    check=True
)
print("✅ databricks-tools-core installed")

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Test imports
from databricks_ai_functions.sdp.build_pipeline import build_pipeline
import os

# Set up auth
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

try:
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
except:
    token = w.config.token

os.environ["DATABRICKS_HOST"] = w.config.host
os.environ["DATABRICKS_TOKEN"] = token

print("✅ Imports and auth configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Test CDC Request

# COMMAND ----------

print("🧪 Testing CDC request directly...")
print("="*60)

try:
    result = build_pipeline(
        user_request="generate a cdc sdp pipeline with iceberg on top of main.ai_dev_kit.customers. append 7 to the end of any tables you create",
        catalog="main",
        schema="ai_dev_kit",
        model="databricks-gpt-5-2",
        start_run=True
    )
    
    print("\n✅ SUCCESS!")
    print(json.dumps(result, indent=2))
    
    print("\n" + "="*60)
    print("👉 Check pipeline for CDC syntax:")
    print(f"   {result['pipeline_url']}")
    print("="*60)
    
except Exception as e:
    print(f"\n❌ FAILED: {e}")
    import traceback
    print(traceback.format_exc())

