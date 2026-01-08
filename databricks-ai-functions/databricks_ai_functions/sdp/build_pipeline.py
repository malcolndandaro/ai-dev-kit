import json
import os
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional

from databricks_tools_core.spark_declarative_pipelines.pipelines import (
    create_or_update_pipeline,
)
from databricks_tools_core.file import upload_folder
from databricks_tools_core.unity_catalog.tables import get_table
from databricks.sdk import WorkspaceClient

from databricks_ai_functions._shared.llm import call_llm
from databricks_ai_functions._shared.skills import load_skills, retrieve_relevant_skills


DEFAULT_WORKSPACE_ROOT = "/Workspace/Shared/ai-dev-kit/pipelines"


def _extract_source_tables(user_request: str) -> List[str]:
    """
    Extract potential source table names from user request.
    Looks for patterns like catalog.schema.table or mentions of "from X".
    """
    import re
    
    # Pattern for 3-part names: word.word.word
    pattern = r'\b([a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*)\b'
    matches = re.findall(pattern, user_request.lower())
    
    return list(set(matches))  # Remove duplicates


def _get_source_schema_info(table_names: List[str]) -> str:
    """
    Fetch schema information for source tables and format for LLM.
    Returns a string with table schemas or empty string if none found.
    """
    if not table_names:
        return ""
    
    schema_info = []
    for table_name in table_names:
        try:
            table_info = get_table(table_name)
            if table_info and table_info.columns:
                cols = [f"{col.name} {col.type_text}" for col in table_info.columns[:20]]  # Limit to first 20 columns
                schema_info.append(f"\nSource table: {table_name}")
                schema_info.append(f"Columns: {', '.join(cols)}")
                if len(table_info.columns) > 20:
                    schema_info.append(f"... and {len(table_info.columns) - 20} more columns")
        except Exception as e:
            # If we can't fetch schema, just continue - LLM will work without it
            continue
    
    return "\n".join(schema_info) if schema_info else ""


def _generate_pipeline_plan(
    user_request: str,
    skills: List[Dict[str, str]],
    catalog: Optional[str],
    schema: Optional[str],
    model: str,
) -> Dict[str, Any]:
    """Prompt LLM to produce pipeline plan JSON."""
    
    # Extract and fetch source table schemas
    source_tables = _extract_source_tables(user_request)
    schema_info = _get_source_schema_info(source_tables)
    
    system = (
        "You are a Databricks engineer expert in Spark Declarative Pipelines (SDP). "
        "Generate complete pipeline configurations. Output ONLY valid JSON."
    )

    location_hint = ""
    if catalog or schema:
        location_hint = f"(target: catalog={catalog or ''}, schema={schema or ''})"

    schema_section = ""
    if schema_info:
        schema_section = f"\n\nSOURCE TABLE SCHEMAS:\n{schema_info}\n\nUse the EXACT column names from these schemas."

    skills_section = "\n\n".join([f"# {s['path']}\n{s['snippet']}" for s in skills])

    user_prompt = f"""User request: {user_request} {location_hint}{schema_section}

REFERENCE DOCUMENTATION (follow these exact patterns):
{skills_section}

Generate a pipeline configuration as JSON:
{{
  "name": "descriptive_pipeline_name",
  "catalog": "main",
  "schema": "target_schema",
  "files": [
    {{"rel_path": "transformations/01_bronze.sql", "language": "SQL", "content": "CREATE OR REFRESH STREAMING TABLE bronze_data AS SELECT * FROM STREAM catalog.schema.source"}},
    {{"rel_path": "transformations/02_silver.sql", "language": "SQL", "content": "CREATE OR REFRESH STREAMING TABLE silver_data AS SELECT * FROM STREAM bronze_data WHERE col IS NOT NULL"}}
  ]
}}

CRITICAL RULES:

1. TABLE REFERENCES:
   - External sources: catalog.schema.table (3-part, NOT 4-part)
   - Pipeline tables: simple names ONLY (bronze_data, not catalog.schema.bronze_data)

2. STREAMING KEYWORD:
   - **CRITICAL**: For CREATE OR REFRESH STREAMING TABLE, ALWAYS use "FROM STREAM source_table"
   - This applies to ALL sources - both external tables AND pipeline tables
   - External: FROM STREAM catalog.schema.table
   - Pipeline: FROM STREAM bronze_data
   - Without STREAM keyword, you get "CREATE_APPEND_ONCE_FLOW_FROM_BATCH_QUERY_NOT_ALLOWED" error

3. COLUMN NAMES:
   - Use EXACT column names from schemas provided above

4. SYNTAX PATTERNS:
   - Follow the EXACT syntax patterns from REFERENCE DOCUMENTATION above
   - Use complete examples - don't omit any required clauses
   - For AUTO CDC: COLUMNS clause MUST have EXCEPT - use "COLUMNS * EXCEPT (_rescued_data)" not just "COLUMNS *"

5. FILE STRUCTURE:
   - rel_path MUST start with "transformations/"
   - Use numeric prefixes (01_, 02_, 03_)

Output ONLY valid JSON."""

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
    if skills_path is None:
        # Skills are now embedded in the package at: databricks_ai_functions/skills/
        module_dir = Path(__file__).parent
        package_skills_path = module_dir.parent / "skills" / "sdp"
        
        if package_skills_path.exists():
            # Use embedded skills (always available)
            skills_path = str(package_skills_path)
            print(f"🔍 DEBUG: Using EMBEDDED skills from package: {skills_path}")
        else:
            # Fallback to workspace (for local dev)
            project_root = module_dir.parent.parent.parent
            skills_path = str(project_root / "databricks-skills" / "sdp")
            print(f"🔍 DEBUG: Using WORKSPACE skills (fallback): {skills_path}")
    
    print(f"🔍 DEBUG: Loading skills from: {skills_path}")
    all_skills = load_skills(skills_path)
    print(f"🔍 DEBUG: Loaded {len(all_skills)} skill documents")
    for skill in all_skills[:3]:
        print(f"   - {skill.get('path', 'unknown')}: {len(skill.get('text', ''))} chars")
    
    relevant_skills = retrieve_relevant_skills(user_request, all_skills, model, k=6)
    print(f"🔍 DEBUG: Retrieved {len(relevant_skills)} relevant skills")
    for skill in relevant_skills[:2]:
        print(f"   - {skill.get('path', 'unknown')}")


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
    import os
    
    # Use environment variables if available (for model serving)
    workspace_client_kwargs = {}
    if os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_TOKEN"):
        workspace_client_kwargs = {
            "host": os.getenv("DATABRICKS_HOST"),
            "token": os.getenv("DATABRICKS_TOKEN")
        }
    
    w = WorkspaceClient(**workspace_client_kwargs)
    pipeline_url = f"{w.config.host}#joblist/pipelines/{run_result.pipeline_id}"

    return {
        "status": "success" if run_result.success else "error",
        "pipeline_id": run_result.pipeline_id,
        "pipeline_url": pipeline_url,
        "update_id": run_result.update_id,
        "created": run_result.created,
        "message": run_result.message,
    }


