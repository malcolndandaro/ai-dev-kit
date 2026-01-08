from databricks.sdk import WorkspaceClient


def register_as_uc_function(
    catalog: str = "main",
    schema: str = "ai_functions",
    function_name: str = "build_sdp_pipeline",
) -> None:
    """
    Register build_pipeline as a Databricks UC Function.
    """
    w = WorkspaceClient()

    routine_definition = (
        "from databricks_ai_functions.sdp import build_pipeline\n"
        "import json\n"
        "def entry(user_request, catalog, schema):\n"
        "    return json.dumps(build_pipeline(user_request, catalog, schema))\n"
        "return entry(user_request, catalog, schema)\n"
    )

    w.functions.create(
        catalog=catalog,
        schema=schema,
        name=function_name,
        input_params=[
            {"name": "user_request", "type_text": "STRING"},
            {"name": "catalog", "type_text": "STRING"},
            {"name": "schema", "type_text": "STRING"},
        ],
        data_type="STRING",
        routine_body="PYTHON",
        routine_definition=routine_definition,
        is_sql_uwf=False,
    )



