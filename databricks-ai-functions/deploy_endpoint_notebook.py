# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Pipeline Builder as Model Serving Endpoint

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

# COMMAND ----------

# Add project paths
sys.path.extend([
    '/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-tools-core',
    '/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions',
])

# COMMAND ----------

class PipelineBuilderModel(PythonModel):
    """MLflow model that wraps build_pipeline."""
    
    def load_context(self, context):
        """Load dependencies when model is initialized."""
        print("✅ PipelineBuilderModel loaded")
    
    def predict(self, context, model_input):
        """Handle inference requests."""
        # Import here - code is bundled with model via code_paths
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

# MAGIC %md
# MAGIC ## Register Model to UC

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
# Use default notebook experiment (required for Git folders)
# mlflow.set_experiment() is not needed - it will use the notebook's experiment

model_name = "main.ai_functions.pipeline_builder"

# COMMAND ----------

with mlflow.start_run(run_name="pipeline_builder_serving") as run:
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
    
    # Infer signature from examples
    signature = mlflow.models.infer_signature(input_example, output_example)
    
    # Bundle the code as artifacts
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=PipelineBuilderModel(),
        registered_model_name=model_name,
        signature=signature,
        input_example=input_example,
        code_paths=[
            "/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-tools-core/databricks_tools_core",
            "/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions/databricks_ai_functions"
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Serving Endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput, ServedEntitySpec
import time

w = WorkspaceClient()

# Wait for model to be READY
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

# Create or update endpoint
endpoint_name = "pipeline-builder-agent-tool"

try:
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
    
except Exception as e:
    if "already exists" in str(e):
        print(f"Endpoint exists, updating...")
        w.serving_endpoints.update_config(
            name=endpoint_name,
            served_entities=[
                ServedEntityInput(
                    entity_name=model_name,
                    entity_version=model_version,
                    scale_to_zero_enabled=True,
                    workload_size="Small"
                )
            ]
        )
        print(f"✅ Endpoint updated!")
    else:
        print(f"❌ Error: {e}")
        raise

print(f"\n🔗 Endpoint URL: https://fe-ai-strategy.cloud.databricks.com/ml/endpoints/{endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Endpoint

# COMMAND ----------

print("Waiting for endpoint to be ready...")
for i in range(60):
    ep = w.serving_endpoints.get(endpoint_name)
    print(f"  State: {ep.state.ready}")
    if ep.state.ready == "READY":
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
print(response)

