"""
Simple SDP Builder - Fully Self-Contained

Just run this on Databricks. No external dependencies needed.
"""

import json
import base64
import time
from typing import List, Dict, Any, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType, Language
from databricks.sdk.service.pipelines import (
    PipelineLibrary,
    FileLibrary,
    NotebookLibrary,
)
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole


class SDPBuilder:
    """Simple RAG-based pipeline builder"""

    def __init__(
        self, skills_path: Optional[str] = None,
        output_dir: Optional[str] = None
    ):
        self.w = WorkspaceClient()

        default_skills = (
            "/Workspace/Users/cal.reynolds@databricks.com/"
            "ai-dev-kit/databricks-skills/sdp"
        )
        default_output = (
            "/Workspace/Users/cal.reynolds@databricks.com/"
            "ai-dev-kit/pipelines"
        )
        self.skills_path = skills_path or default_skills
        self.output_dir = output_dir or default_output
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

    def retrieve_skills(self, query: str, model: str = "databricks-gpt-5-2", k: int = 6) -> List[Dict[str, str]]:
        """Use LLM to select relevant skills"""
        if not self.skills:
            return []

        # Create a summary of available skills for the LLM
        skills_summary = []
        for idx, skill in enumerate(self.skills):
            # Extract title/first line as a preview
            first_lines = skill["text"].split('\n')[:3]
            preview = ' '.join(first_lines).strip()[:200]
            skills_summary.append({
                "id": idx,
                "path": skill["path"],
                "preview": preview
            })

        system = "You are a Databricks expert. Select the most relevant skill documentation for the user's request."

        user_prompt = f"""User request: {query}

Available skills:
{json.dumps(skills_summary, indent=2)}

Select the {k} most relevant skills for this request. Output ONLY a JSON array of skill IDs.
Example: [0, 2, 5, 7, 9, 11]

Consider:
- Which skills contain relevant tables, transformations, or patterns?
- Which skills match the data processing needs?
- Which skills are most applicable to the request?

Output JSON array only:"""

        try:
            response = self.w.serving_endpoints.query(
                name=model,
                messages=[
                    ChatMessage(role=ChatMessageRole.SYSTEM, content=system),
                    ChatMessage(role=ChatMessageRole.USER, content=user_prompt)
                ],
                temperature=0.1,
                max_tokens=500
            )

            # Handle different response formats
            if hasattr(response, 'choices'):
                content = response.choices[0].message.content
            elif isinstance(response, dict):
                content = response.get('choices', [{}])[0].get('message', {}).get('content', '')
            else:
                content = str(response)

            # Parse JSON
            content = content.strip()
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()

            selected_ids = json.loads(content)
            
            # Return the selected skills with full content
            selected_skills = []
            for skill_id in selected_ids[:k]:
                if 0 <= skill_id < len(self.skills):
                    selected_skills.append({
                        "path": self.skills[skill_id]["path"],
                        "snippet": self.skills[skill_id]["text"][:2500]
                    })
            
            print(f"LLM selected {len(selected_skills)} relevant skills")
            return selected_skills

        except Exception as e:
            print(f"Warning: LLM skill selection failed ({e}), falling back to first {k} skills")
            # Fallback to first k skills if LLM fails
            return [{"path": s["path"], "snippet": s["text"][:2500]} for s in self.skills[:k]]

    def generate_plan(self, user_request: str, model: str = "databricks-gpt-5-2") -> Dict[str, Any]:
        """Generate pipeline plan using LLM"""
        skills = self.retrieve_skills(user_request, model=model, k=6)

        system = "You are a Databricks engineer. Generate Spark Declarative Pipelines. Output ONLY valid JSON."

        user_prompt = f"""User request: {user_request}

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
- catalog and schema: **CAREFULLY READ THE USER REQUEST FOR TARGET LOCATION**
  * If user says "main.ai_dev_kit" or "same catalog.schema" where source is "main.ai_dev_kit.X", use catalog="main", schema="ai_dev_kit"
  * Look for phrases like "put it in main.ai_dev_kit", "output to main.ai_dev_kit", "same catalog/schema"
  * ONLY use schema="default" if the user explicitly says "default" or doesn't mention any schema
  * When in doubt, extract schema from the source table path (e.g., if source is "main.ai_dev_kit.customers", output should go to catalog="main", schema="ai_dev_kit")
- name: Use a descriptive pipeline name based on the user's request
- files: Create actual SDP SQL with proper syntax (use MATERIALIZED VIEW for batch or STREAMING TABLE for streaming)
- rel_path: **MUST start with "transformations/"** - e.g., "transformations/01_bronze_customers.sql", "transformations/02_silver_customers.sql"
  * **CRITICAL**: Use numeric prefixes (01_, 02_, 03_) to define execution order for bronze → silver → gold pipelines
  * Files are executed in alphabetical order, so prefix with numbers to ensure dependencies are met
  * Example order: 01_bronze_*, 02_silver_*, 03_gold_*
- Do NOT include root_path or storage fields
- Set start_run to true and full_refresh to true
- Make sure table/view names in SQL don't conflict with source table names
- **DEPENDENCIES**: If a file depends on tables created in another file, ensure proper ordering with numeric prefixes

**CRITICAL SQL SYNTAX RULES FOR SDP (Spark Declarative Pipelines):**
- **DO NOT use USE CATALOG or USE SCHEMA statements** - the pipeline's catalog and target schema settings handle this automatically
- When reading from Unity Catalog tables, use fully qualified names: catalog.schema.table
- For cloud files: Use cloud_files('/path', 'format') or read_files('/path')
- For streaming: Use STREAMING TABLE instead of MATERIALIZED VIEW
- For output tables/views: Use simple names (no catalog/schema prefix) - they will be created in the pipeline's target location
- Example SQL file content:
  ```
  CREATE OR REFRESH MATERIALIZED VIEW my_view 
  AS SELECT * FROM main.ai_dev_kit.customers;
  ```
- The pipeline configuration (catalog + target schema) determines where output tables are created

**FOLDER STRUCTURE:**
- Files will be organized in a pipeline folder: {self.output_dir}/{{pipeline_name}}/transformations/
- All SQL files MUST have rel_path starting with "transformations/"
- This matches Databricks SDP expected structure

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

        # Add full paths with pipeline folder structure
        # Structure: {output_dir}/{pipeline_name}/{rel_path}
        # Where rel_path already includes "transformations/" prefix
        plan["workspace_file_paths"] = [
            f"{self.output_dir}/{plan['name']}/{f['rel_path']}" for f in plan["files"]
        ]

        # Debug: print the plan
        print(f"Generated plan: {json.dumps(plan, indent=2)}")

        return plan

    def execute_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the plan"""

        # Create pipeline folder structure: {output_dir}/{pipeline_name}/transformations/
        pipeline_root = f"{self.output_dir}/{plan['name']}"
        print(f"Creating pipeline folder structure: {pipeline_root}")
        self.w.workspace.mkdirs(pipeline_root)
        
        # Create transformations subfolder
        transformations_dir = f"{pipeline_root}/transformations"
        self.w.workspace.mkdirs(transformations_dir)
        print(f"  ✓ Created transformations folder: {transformations_dir}")

        # Write files
        print(f"\nWriting {len(plan['files'])} files...")
        written_paths = []
        for f in plan["files"]:
            # Build full path: {output_dir}/{pipeline_name}/{rel_path}
            # rel_path already includes "transformations/" prefix
            path = f"{pipeline_root}/{f['rel_path']}"

            # Create parent dirs (in case there are nested folders)
            parent = "/".join(path.split("/")[:-1])
            try:
                self.w.workspace.mkdirs(parent)
            except Exception:
                pass

            # Write file - SDP accepts SQL/Python notebooks
            # When no format is specified, workspace.import_() creates a notebook
            # The .sql extension in the path is just naming -
            # it's stored as a SQL notebook
            lang_map = {
                "PYTHON": Language.PYTHON,
                "SQL": Language.SQL,
                "SCALA": Language.SCALA
            }
            lang = lang_map.get(f.get("language", "SQL").upper(), Language.SQL)

            content_b64 = base64.b64encode(
                f["content"].encode("utf-8")
            ).decode("utf-8")

            # Delete if exists first to avoid conflicts
            try:
                self.w.workspace.delete(path)
                time.sleep(0.5)  # Brief pause after delete
            except Exception:
                pass
            
            # Import as a SQL/Python notebook (no format = notebook)
            # This is what SDP expects for FileLibrary references
            self.w.workspace.import_(
                path=path,
                content=content_b64,
                language=lang
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
        
        # Give the workspace time to fully sync and register files
        # SDP pipelines need files to be fully accessible before pipeline creation
        print("\nWaiting for workspace to sync (5 seconds)...")
        time.sleep(5)
        
        # Verify one more time
        print("Final verification:")
        for path in written_paths:
            try:
                status = self.w.workspace.get_status(path)
                print(f"  ✓ {path} confirmed (type: {status.object_type})")
            except Exception as e:
                print(f"  ✗ {path} STILL NOT FOUND: {e}")
                raise Exception(f"File {path} not accessible after creation")

        # Create pipeline
        print(f"Creating pipeline: {plan['name']}")

        # Reference individual files in the transformations folder
        # SDP needs explicit file references, not folder references
        libraries = [
            PipelineLibrary(notebook=NotebookLibrary(path=p))
            for p in plan["workspace_file_paths"]
        ]
        print(f"\nLibraries to register:")
        for lib in libraries:
            print(f"  - {lib.notebook.path}")

        # Check if pipeline exists
        pipeline_id = None
        try:
            for p in self.w.pipelines.list_pipelines():
                if p.name == plan["name"]:
                    pipeline_id = p.pipeline_id
                    break
        except Exception:
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
                serverless=True,
                continuous=False  # Triggered mode - runs once and stops
            )
        else:
            # Create new
            print(f"  Creating new pipeline")
            response = self.w.pipelines.create(
                name=plan["name"],
                catalog=plan["catalog"],
                target=plan["schema"],
                libraries=libraries,
                serverless=True,
                continuous=False  # Triggered mode - runs once and stops
            )
            pipeline_id = response.pipeline_id

        # Give the pipeline a moment to fully initialize and register libraries
        print("  Waiting for pipeline to initialize...")
        time.sleep(3)

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
