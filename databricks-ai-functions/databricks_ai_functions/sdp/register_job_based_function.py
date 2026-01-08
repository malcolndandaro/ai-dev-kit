"""
Alternative: UC Function that triggers a Databricks Job.

Since ENVIRONMENT dependencies aren't working, this approach:
1. UC Function receives the request
2. Triggers a Databricks Job that has access to workspace code
3. Returns the job run ID immediately
4. Agent can check status via another function
"""

from databricks.sdk import WorkspaceClient


def create_job_based_function():
    """
    Create a UC function that triggers a job to build pipelines.
    This works around the ENVIRONMENT dependency limitations.
    """
    w = WorkspaceClient()
    
    # First, create a job that runs build_pipeline
    print("Step 1: Creating Databricks Job for build_pipeline...")
    
    job = w.jobs.create(
        name="AI Agent - Build SDP Pipeline",
        tasks=[{
            "task_key": "build_pipeline",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions/build_pipeline_notebook",
                "base_parameters": {}
            },
            "new_cluster": {
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 0,
                "spark_conf": {
                    "spark.databricks.cluster.profile": "singleNode",
                    "spark.master": "local[*]"
                },
                "custom_tags": {
                    "ResourceClass": "SingleNode"
                }
            }
        }],
        parameters=[
            {"name": "user_request", "default": ""},
            {"name": "model", "default": "databricks-gpt-5-2"}
        ]
    )
    
    job_id = job.job_id
    print(f"✅ Job created: {job_id}")
    
    # Now create UC function that triggers this job
    print("\nStep 2: Creating UC Function that triggers the job...")
    
    sql = f"""
CREATE OR REPLACE FUNCTION main.ai_functions.build_sdp_pipeline(
  user_request STRING,
  model STRING
)
RETURNS STRING
COMMENT 'Build SDP pipeline by triggering a Databricks Job'
LANGUAGE PYTHON
AS $$
from databricks.sdk import WorkspaceClient
import json

w = WorkspaceClient()

# Trigger the job
run = w.jobs.run_now(
    job_id={job_id},
    notebook_params={{
        "user_request": user_request,
        "model": model
    }}
)

return json.dumps({{
    "status": "job_triggered",
    "run_id": run.run_id,
    "message": "Pipeline build job started. Check status with run_id.",
    "run_url": f"https://fe-ai-strategy.cloud.databricks.com/#job/{job_id}/run/{{run.run_id}}"
}}, indent=2)
$$
"""
    
    whs = list(w.warehouses.list())
    wh = next((w for w in whs if w.state.value == "RUNNING"), whs[0])
    
    result = w.statement_execution.execute_statement(
        warehouse_id=wh.id,
        statement=sql,
        wait_timeout="30s"
    )
    
    print(f"✅ UC Function created!")
    print(f"\n📝 Now create the notebook at:")
    print(f"   /Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions/build_pipeline_notebook")
    print(f"\n   With this code:")
    print('''
# Databricks notebook source
# MAGIC %pip install databricks-sdk>=0.31.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
import sys
sys.path.extend([
    '/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-tools-core',
    '/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions',
])

# COMMAND ----------
from databricks_ai_functions.sdp.build_pipeline import build_pipeline
import json

user_request = dbutils.widgets.get("user_request")
model = dbutils.widgets.get("model")

result = build_pipeline(
    user_request=user_request,
    catalog="main",  # Extract from request if needed
    schema="default",
    model=model,
    start_run=True
)

print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
''')
    
    return job_id


if __name__ == "__main__":
    job_id = create_job_based_function()
    print(f"\n✅ Setup complete!")
    print(f"   Job ID: {job_id}")
    print(f"   Function: main.ai_functions.build_sdp_pipeline")

