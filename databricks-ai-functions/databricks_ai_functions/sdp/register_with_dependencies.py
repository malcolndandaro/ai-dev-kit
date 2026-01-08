"""
Register build_pipeline as a Unity Catalog function with custom dependencies.

This version registers the function with the ENVIRONMENT clause to load
custom wheels from UC Volumes.
"""

from unitycatalog.ai.core.databricks import DatabricksFunctionClient


def register_tool_with_dependencies(
    catalog: str = "main",
    schema: str = "ai_functions",
    replace: bool = True
):
    """
    Register build_sdp_pipeline with custom package dependencies.
    
    This uses SQL CREATE FUNCTION with ENVIRONMENT clause to load:
    - databricks_tools_core from UC volume
    - databricks_ai_functions from UC volume
    """
    print(f"Registering UC function with dependencies: {catalog}.{schema}.build_sdp_pipeline")
    
    client = DatabricksFunctionClient()
    
    # Create function with SQL body that includes ENVIRONMENT clause
    sql_body = f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.build_sdp_pipeline(
  user_request STRING COMMENT 'Natural language description of the pipeline to build',
  model STRING COMMENT 'LLM model endpoint name (e.g., databricks-gpt-5-2)'
)
RETURNS STRING
COMMENT 'Build a Spark Declarative Pipeline from natural language. Returns JSON with pipeline details.'
LANGUAGE PYTHON
ENVIRONMENT (
  dependencies = '[
    "databricks-sdk>=0.31.0",
    "/Volumes/main/ai_functions/packages/databricks_tools_core-0.1.0-py3-none-any.whl",
    "/Volumes/main/ai_functions/packages/databricks_ai_functions-0.1.0-py3-none-any.whl"
  ]',
  environment_version = 'None'
)
AS $$
# Import inside function (required for UC functions)
from databricks_ai_functions.sdp.build_pipeline import build_pipeline
import json
import re

# Extract catalog and schema from user request
catalog = "main"
schema = "default"

target_match = re.search(
    r'(?:target|to|into):\\s*(\\w+)\\.(\\w+)',
    user_request,
    re.IGNORECASE
)
if target_match:
    catalog = target_match.group(1)
    schema = target_match.group(2)

# Call build_pipeline
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
    error_result = {{
        "status": "error",
        "pipeline_id": "unknown",
        "pipeline_url": "",
        "message": f"Failed to build pipeline: {{str(e)}}",
        "catalog": catalog,
        "schema": schema,
    }}
    return json.dumps(error_result, indent=2)
$$
"""
    
    # Register using create_function with SQL body
    try:
        function_info = client.create_function(sql_function_body=sql_body)
        print(f"\\n✅ Successfully registered UC function with dependencies!")
        print(f"   Function name: {catalog}.{schema}.build_sdp_pipeline")
        print(f"   Dependencies loaded from: /Volumes/main/ai_functions/packages/")
        return function_info
    except Exception as e:
        print(f"\\n❌ Failed to register: {e}")
        raise


if __name__ == "__main__":
    import sys
    
    catalog = sys.argv[1] if len(sys.argv) > 1 else "main"
    schema = sys.argv[2] if len(sys.argv) > 2 else "ai_functions"
    
    register_tool_with_dependencies(catalog=catalog, schema=schema)
    
    print(f"\\n🎉 Ready to use in AI agents!")
    print(f"\\n   from databricks_langchain import UCFunctionToolkit")
    print(f"   toolkit = UCFunctionToolkit(function_names=['{catalog}.{schema}.build_sdp_pipeline'])")

