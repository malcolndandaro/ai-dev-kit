from typing import Optional, Dict, Any

from ..server import mcp

from databricks_ai_functions.sdp import build_pipeline as _build_pipeline


@mcp.tool
def databricks_build_sdp_pipeline(
    user_request: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    model: str = "databricks-gpt-5-2",
) -> Dict[str, Any]:
    """
    AI-powered SDP pipeline builder. Creates a Databricks pipeline
    from natural language description.
    """
    return _build_pipeline(
        user_request=user_request,
        catalog=catalog,
        schema=schema,
        model=model,
    )


