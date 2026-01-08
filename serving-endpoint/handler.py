import base64
import json
import os
from typing import Any, Dict

from databricks.sdk import WorkspaceClient


def _error(message: str) -> Dict[str, Any]:
    return {"error": message}


def _build_task_from_payload(payload: Dict[str, Any], encoded_args: str) -> Dict[str, Any]:
    """
    Build a Jobs task spec from payload. Prefer task_overrides if provided.
    """
    # Full override
    if "task_overrides" in payload and isinstance(payload["task_overrides"], dict):
        return payload["task_overrides"]

    runner_path = payload.get("runner_path")
    if not runner_path:
        raise ValueError("runner_path is required in payload to locate job_runner.py in Workspace")

    task: Dict[str, Any] = {
        "task_key": "agent_runner",
        "python_task": {
            "python_file": runner_path,
            "parameters": [encoded_args],
        },
    }

    # Compute: either existing_cluster_id or new_cluster must be present unless using task_overrides
    existing_cluster_id = payload.get("existing_cluster_id")
    new_cluster = payload.get("new_cluster")
    if existing_cluster_id:
        task["existing_cluster_id"] = existing_cluster_id
    elif new_cluster:
        task["new_cluster"] = new_cluster
    else:
        raise ValueError(
            "Either existing_cluster_id or new_cluster must be provided (or use task_overrides)."
        )

    return task


def predict(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Model Serving entrypoint.

    Accepts a payload, submits a one-time Jobs run to execute `job_runner.py`,
    and returns the run URL immediately.
    """
    try:
        if not isinstance(payload, dict):
            return _error("Invalid payload: expected JSON object")

        # Encode the original payload for the job runner (as a single arg)
        encoded_args = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")

        w = WorkspaceClient()

        task = _build_task_from_payload(payload, encoded_args)

        submit_body = {
            "run_name": payload.get("run_name", "serving-endpoint agent build run"),
            "tasks": [task],
        }

        # Use raw API to avoid tight coupling with SDK task dataclasses here
        submit_resp = w.api_client.do(
            "POST",
            "/api/2.1/jobs/runs/submit",
            body=submit_body,
        )
        run_id = submit_resp.get("run_id")
        if not run_id:
            return _error(f"Failed to submit job: {submit_resp}")

        # Fetch run_page_url
        run_resp = w.api_client.do(
            "GET",
            "/api/2.1/jobs/runs/get",
            query={"run_id": run_id},
        )
        run_url = run_resp.get("run_page_url") or f"{w.config.host}/jobs/runs/{run_id}"

        return {
            "run_id": run_id,
            "run_url": run_url,
        }

    except Exception as e:
        return _error(str(e))


