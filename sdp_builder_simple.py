"""
Simple SDP Builder - Fully Self-Contained

Just run this on Databricks. No external dependencies needed.
"""

import os
import re
import json
import base64
import time
from typing import List, Dict, Any, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType, Language, ImportFormat
from databricks.sdk.service.pipelines import (
    PipelineLibrary,
    FileLibrary,
    UpdateInfoState,
)
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole


class SDPBuilder:
    """Simple RAG-based pipeline builder"""

    def __init__(self, skills_path: Optional[str] = None, output_dir: Optional[str] = None):
        self.w = WorkspaceClient()
        user = self.w.current_user.me().user_name

        self.skills_path = skills_path or f"/Workspace/Users/{user}/ai-dev-kit/skills"
        self.output_dir = output_dir or f"/Workspace/Users/{user}/ai-dev-kit/pipelines"
        self.skills = self._load_skills()

    def _load_skills(self) -> List[Dict[str, str]]:
        """Load skill markdown files"""
        items = []
        try:
            for item in self.w.workspace.list(self.skills_path, recursive=True):
                if item.path and item.path.endswith(".md") and item.object_type != ObjectType.DIRECTORY:
                    try:
                        content = self.w.workspace.download(item.path).read().decode('utf-8')
                        items.append({"path": item.path, "text": content})
                    except Exception as e:
                        print(f"Warning: {e}")
        except Exception as e:
            print(f"No skills found at {self.skills_path}: {e}")

        print(f"Loaded {len(items)} skill files")
        return items

    def retrieve_skills(self, query: str, k: int = 6) -> List[Dict[str, str]]:
        """Get relevant skills"""
        def score(text: str, query: str) -> int:
            q = re.findall(r"[a-zA-Z0-9_]+", query.lower())
            t = text.lower()
            return sum(3 for w in q if w in t)

        if not self.skills:
            return []

        ranked = sorted(self.skills, key=lambda x: score(x["text"], query), reverse=True)
        return [{"path": s["path"], "snippet": s["text"][:2500]} for s in ranked[:k]]

    def generate_plan(self, user_request: str, model: str = "databricks-gpt-5-2") -> Dict[str, Any]:
        """Generate pipeline plan using LLM"""
        skills = self.retrieve_skills(user_request, k=6)

        system = "You are a Databricks engineer. Generate Spark Declarative Pipelines. Output ONLY valid JSON."

        user_prompt = f"""User request: {user_request}

Skills: {json.dumps(skills, indent=2)}

Output JSON:
{{
  "name": "descriptive_pipeline_name",
  "catalog": "main",
  "schema": "target_schema",
  "files": [
    {{"rel_path": "pipeline.sql", "language": "SQL", "content": "CREATE OR REFRESH MATERIALIZED VIEW target_table AS SELECT * FROM source_table"}}
  ],
  "start_run": true,
  "full_refresh": true
}}

CRITICAL REQUIREMENTS:
- catalog and schema: **CAREFULLY READ THE USER REQUEST FOR TARGET LOCATION**
  * If user says "main.ai_dev_kit" or "same catalog.schema" where source is "main.ai_dev_kit.X", use catalog="main", schema="ai_dev_kit"
  * Look for phrases like "put it in main.ai_dev_kit", "output to main.ai_dev_kit", "same catalog/schema"
  * ONLY use schema="default" if the user explicitly says "default" or doesn't mention any schema
  * When in doubt, extract schema from the source table path (e.g., if source is "main.ai_dev_kit.customers", output should go to catalog="main", schema="ai_dev_kit")
- name: Use a descriptive pipeline name based on the user's request
- files: Create actual DLT SQL with proper syntax (use MATERIALIZED VIEW for batch or STREAMING TABLE for streaming)
- rel_path: Use descriptive filenames like "gold_customers_mv.sql" or "bronze_ingestion.sql"
- Do NOT include root_path or storage fields
- Set start_run to true and full_refresh to true
- Make sure table/view names in SQL don't conflict with source table names

All files go in: {self.output_dir}
Output JSON ONLY."""

        response = self.w.serving_endpoints.query(
            name=model,
            messages=[
                ChatMessage(role=ChatMessageRole.SYSTEM, content=system),
                ChatMessage(role=ChatMessageRole.USER, content=user_prompt)
            ],
            temperature=0.0,
            max_tokens=4000
        )

        # Handle different response formats
        if hasattr(response, 'choices'):
            content = response.choices[0].message.content
        elif isinstance(response, dict):
            content = response.get('choices', [{}])[0].get('message', {}).get('content', '')
        else:
            content = str(response)

        # Parse JSON
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()

        plan = json.loads(content)

        # Add full paths - ensure they match exactly what files list has
        plan["workspace_file_paths"] = [
            f"{self.output_dir}/{f['rel_path']}" for f in plan["files"]
        ]

        # Debug: print the plan
        print(f"Generated plan: {json.dumps(plan, indent=2)}")

        return plan

    def execute_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the plan"""

        # Create output directory
        self.w.workspace.mkdirs(self.output_dir)

        # Write files
        print(f"Writing {len(plan['files'])} files...")
        written_paths = []
        for f in plan["files"]:
            path = f"{self.output_dir}/{f['rel_path']}"

            # Create parent dirs
            parent = "/".join(path.split("/")[:-1])
            try:
                self.w.workspace.mkdirs(parent)
            except:
                pass

            # Write file - DLT requires SQL notebooks (not source files)
            lang_map = {"PYTHON": Language.PYTHON, "SQL": Language.SQL, "SCALA": Language.SCALA}
            lang = lang_map.get(f.get("language", "SQL").upper(), Language.SQL)

            content_b64 = base64.b64encode(f["content"].encode("utf-8")).decode("utf-8")

            # DLT requires SQL files to be in DBC (notebook) format
            self.w.workspace.import_(
                path=path,
                content=content_b64,
                language=lang,
                format=ImportFormat.DBC,  # Use DBC format for notebooks
                overwrite=True
            )

            written_paths.append(path)
            print(f"  ✓ {path}")

        # Verify workspace_file_paths matches what we wrote
        print(f"\nExpected paths: {plan['workspace_file_paths']}")
        print(f"Written paths: {written_paths}")

        # Update plan with actual written paths (in case there's a mismatch)
        plan["workspace_file_paths"] = written_paths

        # Verify files exist
        print("\nVerifying files exist:")
        for path in written_paths:
            try:
                status = self.w.workspace.get_status(path)
                print(f"  ✓ {path} exists (type: {status.object_type})")
            except Exception as e:
                print(f"  ✗ {path} ERROR: {e}")

        # Create pipeline
        print(f"Creating pipeline: {plan['name']}")

        libraries = [PipelineLibrary(file=FileLibrary(path=p)) for p in plan["workspace_file_paths"]]
        print(f"\nLibraries to register:")
        for lib in libraries:
            print(f"  - {lib.file.path}")

        # Check if pipeline exists
        pipeline_id = None
        try:
            for p in self.w.pipelines.list_pipelines():
                if p.name == plan["name"]:
                    pipeline_id = p.pipeline_id
                    break
        except:
            pass

        if pipeline_id:
            # Update existing
            print(f"  Updating existing pipeline: {pipeline_id}")
            self.w.pipelines.update(
                pipeline_id=pipeline_id,
                name=plan["name"],
                catalog=plan["catalog"],
                target=plan["schema"],
                libraries=libraries,
                serverless=True
            )
        else:
            # Create new
            print(f"  Creating new pipeline")
            response = self.w.pipelines.create(
                name=plan["name"],
                catalog=plan["catalog"],
                target=plan["schema"],
                libraries=libraries,
                serverless=True
            )
            pipeline_id = response.pipeline_id

        # Start run if requested
        update_id = None
        should_start = plan.get("start_run", True)  # Default to True
        print(f"  Should start run: {should_start}")
        if should_start:
            print("  Starting pipeline run...")
            try:
                update = self.w.pipelines.start_update(
                    pipeline_id=pipeline_id,
                    full_refresh=plan.get("full_refresh", True)
                )
                update_id = update.update_id if hasattr(update, 'update_id') else None
            except Exception as e:
                print(f"  Warning starting update: {e}")

        return {
            "pipeline_id": pipeline_id,
            "pipeline_url": f"{self.w.config.host}#joblist/pipelines/{pipeline_id}",
            "update_id": update_id,
            "status": "success"
        }

    def build(self, user_request: str, model: str = "databricks-gpt-5-2") -> Dict[str, Any]:
        """Main entry point"""
        try:
            print(f"Building pipeline for: {user_request[:100]}...")
            plan = self.generate_plan(user_request, model)
            result = self.execute_plan(plan)

            return {
                "status": "success",
                "plan": plan,
                "result": result
            }
        except Exception as e:
            import traceback
            return {
                "status": "error",
                "error": str(e),
                "traceback": traceback.format_exc()
            }


def build_sdp_pipeline(user_request: str, **kwargs) -> Dict[str, Any]:
    """Simple function interface"""
    builder = SDPBuilder(
        skills_path=kwargs.get("skills_path"),
        output_dir=kwargs.get("output_dir")
    )
    return builder.build(user_request, kwargs.get("model", "databricks-gpt-5-2"))


# Alias for compatibility
build_sdp = build_sdp_pipeline


if __name__ == "__main__":
    result = build_sdp_pipeline("Create a pipeline that reads from a source table and writes to a target table")
    print(json.dumps(result, indent=2))
