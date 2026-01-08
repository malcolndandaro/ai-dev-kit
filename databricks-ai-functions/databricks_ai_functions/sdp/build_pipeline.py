import json
import os
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional

from databricks_tools_core.spark_declarative_pipelines.pipelines import (
    create_or_update_pipeline,
)
from databricks_tools_core.file import upload_folder
from databricks.sdk import WorkspaceClient

from databricks_ai_functions._shared.llm import call_llm
from databricks_ai_functions._shared.skills import load_skills, retrieve_relevant_skills


DEFAULT_WORKSPACE_ROOT = "/Workspace/Shared/ai-dev-kit/pipelines"


def _generate_pipeline_plan(
    user_request: str,
    skills: List[Dict[str, str]],
    catalog: Optional[str],
    schema: Optional[str],
    model: str,
) -> Dict[str, Any]:
    """Prompt LLM to produce pipeline plan JSON."""
    system = (
        "You are a Databricks engineer. Generate Spark Declarative Pipelines. Output ONLY valid JSON."
    )

    location_hint = ""
    if catalog or schema:
        location_hint = f"(target: catalog={catalog or ''}, schema={schema or ''})"

    user_prompt = f"""User request: {user_request} {location_hint}

Skills: {json.dumps(skills, indent=2)}

Output JSON:
{{
  "name": "descriptive_pipeline_name",
  "catalog": "main",
  "schema": "target_schema",
  "files": [
    {{"rel_path": "transformations/01_bronze_source.sql", "language": "SQL", "content": "CREATE OR REFRESH MATERIALIZED VIEW bronze_table AS SELECT * FROM source_table"}},
    {{"rel_path": "transformations/02_silver_transform.sql", "language": "SQL", "content": "CREATE OR REFRESH MATERIALIZED VIEW silver_table AS SELECT * FROM bronze_table"}},
    {{"rel_path": "transformations/03_gold_aggregate.sql", "language": "SQL", "content": "CREATE OR REFRESH MATERIALIZED VIEW gold_table AS SELECT * FROM silver_table"}}
  ],
  "start_run": true,
  "full_refresh": true
}}

CRITICAL REQUIREMENTS:
- catalog and schema: CAREFULLY READ THE USER REQUEST FOR TARGET LOCATION
  * If user says "main.ai_dev_kit" or mentions fully qualified sources, infer matching target
  * ONLY use schema="default" if the user explicitly says "default" or doesn't mention any schema
  * When in doubt, extract schema from the source table path
- name: Use a descriptive pipeline name based on the user's request
- files: Create actual SDP SQL with proper syntax (use MATERIALIZED VIEW for batch or STREAMING TABLE for streaming)
- rel_path: MUST start with "transformations/" and use numeric prefixes (01_, 02_, 03_) to define execution order
- Do NOT include root_path or storage fields
- Set start_run to true and full_refresh to true
- Avoid conflicts between output table names and source names

SQL SYNTAX RULES FOR SDP:
- DO NOT use USE CATALOG or USE SCHEMA statements
- Use fully qualified names for reads: catalog.schema.table
- STREAMING: use CREATE STREAMING TABLE for streaming datasets
- OUTPUT table/view names: simple names without catalog/schema prefix

FOLDER STRUCTURE:
- {{"workspace_root"}}/{{"pipeline_name"}}/transformations/*.sql
- rel_path values MUST begin with "transformations/"

Output JSON ONLY."""

    content = call_llm(system=system, user_prompt=user_prompt, model=model, temperature=0.0, max_tokens=4000)
    plan = json.loads(content)

    # Ensure rel_path prefix
    for f in plan.get("files", []):
        rel_path = f.get("rel_path", "")
        if not rel_path.startswith("transformations/"):
            f["rel_path"] = f"transformations/{rel_path.lstrip('/')}"

    # Override catalog/schema if provided explicitly
    if catalog:
        plan["catalog"] = catalog
    if schema:
        plan["schema"] = schema

    return plan


def _materialize_local_files(plan: Dict[str, Any]) -> Path:
    """
    Create local temp folder with transformations files matching rel_path.
    Returns the local folder path containing the pipeline root.
    """
    tmp_dir = Path(tempfile.mkdtemp(prefix="sdp_build_"))
    pipeline_root = tmp_dir / plan["name"]
    transformations_dir = pipeline_root / "transformations"
    transformations_dir.mkdir(parents=True, exist_ok=True)

    for f in plan.get("files", []):
        target = pipeline_root / f["rel_path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f.get("content", ""), encoding="utf-8")

    return pipeline_root


def build_pipeline(
    user_request: str,
    catalog: str | None = None,
    schema: str | None = None,
    skills_path: str | None = None,
    model: str = "databricks-gpt-5-2",
    start_run: bool = True,
) -> Dict[str, Any]:
    """
    AI-powered SDP pipeline builder.

    Takes natural language request and creates a running SDP pipeline.
    """
    # 1. Load and retrieve relevant skills
    skills_dir = skills_path or "databricks-skills/sdp"
    all_skills = load_skills(skills_dir)
    relevant_skills = retrieve_relevant_skills(user_request, all_skills, model, k=6)

    # 2. Generate pipeline configuration with LLM
    plan = _generate_pipeline_plan(
        user_request=user_request,
        skills=relevant_skills,
        catalog=catalog,
        schema=schema,
        model=model,
    )

    name = plan["name"]
    target_catalog = plan["catalog"]
    target_schema = plan["schema"]

    # 3. Create local files
    local_pipeline_root = _materialize_local_files(plan)

    # 4. Upload to workspace
    workspace_root = f"{DEFAULT_WORKSPACE_ROOT}/{name}"
    upload_result = upload_folder(
        local_folder=str(local_pipeline_root),
        workspace_folder=workspace_root,
        max_workers=8,
        overwrite=True,
    )
    # Optional: could surface upload_result stats in return payload if needed

    # 5. Build workspace_file_paths list from plan['files']
    workspace_file_paths = [
        f"{workspace_root}/{f['rel_path']}" for f in plan.get("files", [])
    ]

    # 6. Create or update pipeline via tools-core
    run_result = create_or_update_pipeline(
        name=name,
        root_path=workspace_root,
        catalog=target_catalog,
        schema=target_schema,
        workspace_file_paths=workspace_file_paths,
        start_run=start_run,
        wait_for_completion=False,
        full_refresh=True,
    )

    # 7. Return result
    w = WorkspaceClient()
    pipeline_url = f"{w.config.host}#joblist/pipelines/{run_result.pipeline_id}"

    return {
        "status": "success" if run_result.success else "error",
        "pipeline_id": run_result.pipeline_id,
        "pipeline_url": pipeline_url,
        "update_id": run_result.update_id,
        "created": run_result.created,
        "message": run_result.message,
    }


