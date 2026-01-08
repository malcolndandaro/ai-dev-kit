### Serving endpoint (isolated) - Claude + Jobs + SDP

This folder contains an isolated implementation to expose a custom Model Serving endpoint that:
- Receives a question/payload
- Submits a Databricks Jobs one-time run that executes a runner
- The runner uses Claude via the Databricks Foundation Model endpoint to derive or validate configuration, then creates/updates an SDP pipeline
- The serving handler immediately returns the Jobs run URL; the runner prints final `{ service_url, pipeline_id, update_id }` JSON to stdout

### Files
- `handler.py`: Model Serving `predict(payload: dict) -> dict` that submits a Jobs run and returns `{ run_id, run_url }`
- `job_runner.py`: Job task entrypoint that calls Claude (via DBX FM) to generate/confirm pipeline config and then creates/updates an SDP pipeline
- `system_prompt.py`: Minimal Databricks-focused system prompt used by the runner
- `requirements.txt`: Minimal dependencies for the serving container

### Expected payload (to the serving endpoint)
Minimal shape:
```json
{
  "question": "Generate synthetic clickstream and build an SDP pipeline to bronze/silver.",
  "runner_path": "/Workspace/Repos/<repo-path>/serving-endpoint/job_runner.py",
  "claude_endpoint": "anthropic/claude-3-5-sonnet",
  "existing_cluster_id": "<optional-cluster-id>",
  "new_cluster": { "spark_version": "...", "node_type_id": "...", "num_workers": 1 },
  "catalog": "main",
  "schema": "demo",
  "pipelineName": "sdp_clickstream_demo",
  "root_path": "/Workspace/Repos/<repo-path>",
  "workspace_file_paths": ["/Workspace/Repos/<repo-path>/pipelines/bronze.sql"]
}
```

Notes:
- Either `existing_cluster_id` or `new_cluster` must be provided for Jobs compute (unless you override `task_overrides` below).
- If `catalog/schema/pipelineName/workspace_file_paths` are omitted, the runner will attempt to call Claude via DBX FM (`claude_endpoint`) to produce a JSON config.
- Optional advanced override:
  - `task_overrides`: a raw Jobs `tasks[0]` spec that will be forwarded as-is, allowing full control over compute/task type. When provided, it replaces the default Python task spec in `handler.py`.

### Output (serving handler)
```json
{ "run_id": 12345, "run_url": "https://<workspace>/jobs/runs/12345" }
```

### Output (runner, to stdout)
The runner prints a single line JSON on success:
```json
{ "service_url": "https://<workspace>/pipelines/<pipeline_id>", "pipeline_id": "<id>", "update_id": "<optional>", "message": "..." }
```

### Deploy
1) Upload or sync this `serving-endpoint/` folder into your workspace (e.g., a Repo).
2) Create a Jobs run (one-time) that invokes:
   - Task type: Python task
   - Python file: the `runner_path` you pass to the serving handler (e.g., `/Workspace/Repos/.../serving-endpoint/job_runner.py`)
   - Provide cluster via `existing_cluster_id` or define a `new_cluster`
3) Create a custom Python Model Serving endpoint:
   - Entry point module: `serving-endpoint/handler.py`
   - Function: `predict`
   - Requirements file: `serving-endpoint/requirements.txt`
4) Grant the endpoint identity permission to submit Jobs and manage Pipelines.

### Dependencies
- `databricks-sdk` is required in both the serving container and the job cluster environment.
- Claude via Databricks FM: set `claude_endpoint` in the payload or `CLAUDE_ENDPOINT_NAME` in the job environment.
- If you want to rely on local copies of pipeline helpers in this repo rather than duplicating API calls, attach built wheels of `databricks-mcp-core` and `databricks-mcp-server` as Workspace Libraries to both the job and serving endpoint. The runner will attempt to import them; if unavailable, it falls back to direct SDK calls.

### Quick test
1) After deploying, send a request to the serving endpoint body like:
```json
{
  "question": "Create a small synthetic clickstream dataset and an SDP pipeline writing bronze & silver.",
  "runner_path": "/Workspace/Repos/<repo>/serving-endpoint/job_runner.py",
  "claude_endpoint": "anthropic/claude-3-5-sonnet",
  "existing_cluster_id": "<cluster-id>",
  "catalog": "main",
  "schema": "demo",
  "pipelineName": "sdp_clickstream_demo",
  "root_path": "/Workspace/Repos/<repo>",
  "workspace_file_paths": ["/Workspace/Repos/<repo>/pipelines/bronze.sql"]
}
```
2) Open the returned `run_url` to monitor. The runner prints final JSON with `service_url`.


