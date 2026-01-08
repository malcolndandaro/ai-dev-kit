from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole


def _extract_text_content(response: object) -> str:
    """
    Normalize content from Databricks serving_endpoints.query responses.
    Supports both SDK objects and raw dict formats.
    """
    # SDK object with .choices
    if hasattr(response, "choices"):
        try:
            return response.choices[0].message.content or ""
        except Exception:
            pass

    # Dict-like structure
    try:
        if isinstance(response, dict):
            return (
                response.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
            )
    except Exception:
        pass

    # Fallback string
    return str(response or "")


def _strip_fences(text: str) -> str:
    """Remove ```json ... ``` or generic ``` ... ``` fences if present."""
    content = text.strip()
    if "```json" in content:
        try:
            return content.split("```json", 1)[1].split("```", 1)[0].strip()
        except Exception:
            return content
    if "```" in content:
        try:
            return content.split("```", 1)[1].split("```", 1)[0].strip()
        except Exception:
            return content
    return content


def call_llm(
    system: str,
    user_prompt: str,
    model: str = "databricks-gpt-5-2",
    temperature: float = 0.0,
    max_tokens: int = 4000,
    seed: Optional[int] = None,
) -> str:
    """
    Shared LLM calling logic using Databricks Serving Endpoints.

    Returns the response content as plain text (code fences removed if present).
    """
    w = WorkspaceClient()
    
    # Build query params - only include seed if provided and supported
    query_params = {
        "name": model,
        "messages": [
            ChatMessage(role=ChatMessageRole.SYSTEM, content=system),
            ChatMessage(role=ChatMessageRole.USER, content=user_prompt),
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    
    # Only add seed if provided (some SDK versions may not support it)
    if seed is not None:
        try:
            response = w.serving_endpoints.query(**query_params, seed=seed)
        except TypeError:
            # Fallback if seed parameter is not supported
            response = w.serving_endpoints.query(**query_params)
    else:
        response = w.serving_endpoints.query(**query_params)
    
    raw = _extract_text_content(response)
    return _strip_fences(raw)


