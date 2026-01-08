import base64
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import (
    UpdateInfoState,
    FileLibrary,
    PipelineLibrary,
)

from system_prompt import get_system_prompt

import subprocess


def _print_result_line(obj: Dict[str, Any]) -> None:
    # Single-line JSON for easy scraping from run output
    print(json.dumps(obj, separators=(",", ":")))


def _call_claude_via_dbx_fm(question: str, endpoint_name: str) -> Optional[Dict[str, Any]]:
    """
    Call a Databricks Foundation Model serving endpoint for Anthropic to get JSON config.
    Expects the endpoint to support chat-style 'messages'.
    """
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        return None

    url = f"{host}/api/2.0/serving-endpoints/{endpoint_name}/invocations"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    sys_prompt = get_system_prompt()
    payload = {
        "messages": [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": question},
        ],
        "max_tokens": 800,
        "temperature": 0.2,
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        # Try common Anthropic-style response shapes
        text_parts: List[str] = []
        if isinstance(data, dict):
            if "output" in data and isinstance(data["output"], dict):
                # Some endpoints wrap result here
                content = data["output"].get("content")
                if isinstance(content, list):
                    for block in content:
                        if isinstance(block, dict) and block.get("type") == "output_text":
                            text_parts.append(block.get("text", ""))
            elif "choices" in data and isinstance(data["choices"], list) and data["choices"]:
                # OpenAI-like proxy
                message = data["choices"][0].get("message", {})
                text_parts.append(message.get("content", ""))
            elif "predictions" in data and data["predictions"]:
                # Fallback
                text_parts.append(str(data["predictions"][0]))

        text = "\n".join([t for t in text_parts if t]).strip()
        if not text:
            return None

        # Expect strict JSON in response
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Try to extract JSON block if extra text present
            start = text.find("{")
            end = text.rfind("}")
            if start != -1 and end != -1 and end > start:
                snippet = text[start : end + 1]
                try:
                    return json.loads(snippet)
                except Exception:
                    return None
        return None
    except Exception:
        return None


def _libraries_from_paths(paths: List[str]) -> List[PipelineLibrary]:
    return [PipelineLibrary(file=FileLibrary(path=p)) for p in paths]

def _maybe_start_mcp_server() -> Optional[subprocess.Popen]:
    """
    Optionally start the Databricks MCP server via stdio if requested.
    This is useful when running a local agent that expects an MCP server.
    """
    use_mcp = os.environ.get("ENABLE_MCP_SERVER", "false").lower() == "true"
    if not use_mcp:
        return None
    try:
        # Start in the background; the runner itself does not consume it directly.
        # Tools remain available for any local agent process that may connect.
        proc = subprocess.Popen(
            [sys.executable, "-m", "databricks_mcp_server.server"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
        )
        return proc
    except Exception:
        return None


def _find_pipeline_by_name(w: WorkspaceClient, name: str) -> Optional[str]:
    for p in w.pipelines.list_pipelines(filter=f"name LIKE '{name}'"):
        if p.name == name:
            return p.pipeline_id
    return None


def _create_or_update_pipeline(
    w: WorkspaceClient,
    name: str,
    root_path: str,
    catalog: str,
    schema: str,
    workspace_file_paths: List[str],
) -> str:
    pipeline_id = _find_pipeline_by_name(w, name)
    if pipeline_id:
        w.pipelines.update(
            pipeline_id=pipeline_id,
            name=name,
            root_path=root_path,
            catalog=catalog,
            schema=schema,
            libraries=_libraries_from_paths(workspace_file_paths),
        )
        return pipeline_id
    resp = w.pipelines.create(
        name=name,
        root_path=root_path,
        catalog=catalog,
        schema=schema,
        libraries=_libraries_from_paths(workspace_file_paths),
        continuous=False,
        serverless=True,
    )
    return resp.pipeline_id


def _start_update(w: WorkspaceClient, pipeline_id: str, full_refresh: bool = True) -> str:
    resp = w.pipelines.start_update(
        pipeline_id=pipeline_id,
        full_refresh=full_refresh,
    )
    return resp.update_id


def _build_service_url(w: WorkspaceClient, pipeline_id: str) -> str:
    host = w.config.host.rstrip("/")
    # UI URL format may evolve; provide a reasonable deep-link
    return f"{host}/pipelines/{pipeline_id}"


def _load_payload_from_argv() -> Dict[str, Any]:
    if len(sys.argv) < 2:
        return {}
    try:
        raw = base64.b64decode(sys.argv[1]).decode("utf-8")
        return json.loads(raw)
    except Exception:
        return {}


def main() -> None:
    payload = _load_payload_from_argv()
    question = payload.get("question", "")
    claude_endpoint = payload.get("claude_endpoint") or os.environ.get("CLAUDE_ENDPOINT_NAME")

    mcp_proc = _maybe_start_mcp_server()

    w = WorkspaceClient()

    # Gather config
    cfg = {
        "pipelineName": payload.get("pipelineName"),
        "catalog": payload.get("catalog"),
        "schema": payload.get("schema"),
        "root_path": payload.get("root_path") or "/Workspace",
        "workspace_file_paths": payload.get("workspace_file_paths") or [],
    }

    # If incomplete, try to ask Claude (via DBX FM) to produce config JSON
    needs_llm = not all([cfg.get("pipelineName"), cfg.get("catalog"), cfg.get("schema")]) or not cfg["workspace_file_paths"]
    if needs_llm and question and claude_endpoint:
        llm_cfg = _call_claude_via_dbx_fm(question, claude_endpoint) or {}
        cfg["pipelineName"] = cfg["pipelineName"] or llm_cfg.get("pipelineName")
        cfg["catalog"] = cfg["catalog"] or llm_cfg.get("catalog")
        cfg["schema"] = cfg["schema"] or llm_cfg.get("schema")
        cfg["root_path"] = cfg["root_path"] or llm_cfg.get("root_path") or "/Workspace"
        if not cfg["workspace_file_paths"]:
            wfp = llm_cfg.get("workspace_file_paths")
            if isinstance(wfp, list):
                cfg["workspace_file_paths"] = [str(x) for x in wfp if isinstance(x, str)]

    # Validate minimal config
    for key in ["pipelineName", "catalog", "schema", "workspace_file_paths"]:
        if not cfg.get(key):
            _print_result_line({"error": f"Missing required config: {key}"})
            return

    try:
        pipeline_id = _create_or_update_pipeline(
            w=w,
            name=cfg["pipelineName"],
            root_path=cfg["root_path"],
            catalog=cfg["catalog"],
            schema=cfg["schema"],
            workspace_file_paths=cfg["workspace_file_paths"],
        )

        update_id = _start_update(w, pipeline_id=pipeline_id, full_refresh=True)
        service_url = _build_service_url(w, pipeline_id)

        _print_result_line(
            {
                "service_url": service_url,
                "pipeline_id": pipeline_id,
                "update_id": update_id,
                "message": "Pipeline created/updated and run started.",
            }
        )
    except Exception as e:
        _print_result_line({"error": str(e)})
    finally:
        if mcp_proc:
            try:
                mcp_proc.terminate()
            except Exception:
                pass


if __name__ == "__main__":
    main()


