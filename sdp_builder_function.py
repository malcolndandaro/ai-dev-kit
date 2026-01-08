"""
SDP Builder - Standalone Databricks Function for RAG-based Pipeline Generation

This is a self-contained script that can be deployed as a Databricks Function.
Just upload this file to your Databricks workspace and register it.

Usage:
    1. Upload to workspace: /Workspace/Users/<your-email>/sdp_builder_function.py
    2. In a notebook:
       %run /Workspace/Users/<your-email>/sdp_builder_function
       result = build_sdp_pipeline("Create a pipeline that...")
    3. Or call via SQL after registering as a function
"""

import os
import re
import json
from typing import List, Dict, Any, Optional

# Import Databricks SDK and the MCP core library
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType

# Import from the ai-dev-kit repo (assumes it's in sys.path or REPO_ROOT)
import sys

# Auto-detect repo root or use workspace path
REPO_ROOT = os.environ.get("REPO_ROOT", "/Workspace/Repos/cal.reynolds@databricks.com/ai-dev-kit")
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from databricks_mcp_core.spark_declarative_pipelines.pipelines import (
    create_or_update_pipeline,
    PipelineRunResult
)
from databricks_mcp_core.spark_declarative_pipelines.workspace_files import (
    write_file,
    create_directory
)


class SDPBuilder:
    """RAG-based builder for Spark Declarative Pipelines

    This class:
    1. Loads skill markdown files from Databricks workspace
    2. Retrieves relevant skills using keyword-based scoring
    3. Generates pipeline plans using Foundation Models or Model Serving
    4. Executes plans by creating workspace files and registering pipelines
    """

    def __init__(
        self,
        workspace_client: Optional[WorkspaceClient] = None,
        skills_path: Optional[str] = None,
        base_workspace_dir: Optional[str] = None
    ):
        """
        Initialize SDP Builder

        Args:
            workspace_client: Optional WorkspaceClient. If None, creates new one.
            skills_path: Workspace path containing skill .md files
            base_workspace_dir: Base directory for generated pipeline files
        """
        self.w = workspace_client or WorkspaceClient()

        # Get current user for default paths
        try:
            current_user = self.w.current_user.me().user_name
        except Exception:
            current_user = "default_user"

        # Set paths with defaults
        self.skills_path = skills_path or f"/Workspace/Users/{current_user}/ai-dev-kit/databricks-skills/sdp"
        self.base_workspace_dir = base_workspace_dir or f"/Workspace/Users/{current_user}/ai-dev-kit/generated-pipelines"

        # Load skills on initialization
        self.skills = self._load_skills()

    def _load_skills(self) -> List[Dict[str, str]]:
        """Load all markdown skill files from workspace"""
        items = []
        try:
            # Check if directory exists
            try:
                self.w.workspace.get_status(self.skills_path)
            except Exception:
                print(f"Warning: Skills directory not found: {self.skills_path}")
                print(f"Create it with: databricks workspace mkdirs {self.skills_path}")
                return items

            # List files in workspace recursively
            for item in self.w.workspace.list(self.skills_path, recursive=True):
                if item.path and item.path.endswith(".md") and item.object_type != ObjectType.DIRECTORY:
                    try:
                        content = self.w.workspace.download(item.path).read().decode('utf-8')
                        items.append({"path": item.path, "text": content})
                        print(f"Loaded skill: {item.path}")
                    except Exception as e:
                        print(f"Warning: Could not load {item.path}: {e}")
        except Exception as e:
            print(f"Warning: Could not list skills directory {self.skills_path}: {e}")

        print(f"Loaded {len(items)} skill files")
        return items

    def _score(self, text: str, query: str) -> int:
        """Simple keyword scoring for skill retrieval"""
        q = re.findall(r"[a-zA-Z0-9_]+", query.lower())
        t = text.lower()
        return sum(3 for w in q if w in t)

    def retrieve_skills(
        self,
        query: str,
        k: int = 6,
        max_chars_each: int = 2500
    ) -> List[Dict[str, str]]:
        """Retrieve top-k relevant skills based on query"""
        if not self.skills:
            print("Warning: No skills loaded. Pipeline generation may be less effective.")
            return []

        ranked = sorted(self.skills, key=lambda x: self._score(x["text"], query), reverse=True)
        top = []
        for item in ranked[:k]:
            snippet = item["text"][:max_chars_each]
            top.append({"path": item["path"], "snippet": snippet})
        return top

    def generate_plan(
        self,
        user_request: str,
        model_endpoint: str = "databricks-meta-llama-3-1-70b-instruct",
        skill_snips: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        Generate pipeline plan using LLM

        Args:
            user_request: User's natural language request
            model_endpoint: Model serving endpoint name or Foundation Model name
            skill_snips: Optional pre-retrieved skills

        Returns:
            Dict containing pipeline plan with files, config, etc.
        """
        if skill_snips is None:
            skill_snips = self.retrieve_skills(user_request, k=6)

        plan_schema = """
{
  "name": "pipeline_name",
  "catalog": "catalog_name",
  "schema": "schema_name",
  "root_path": "dbfs:/pipelines/...",
  "workspace_base_dir": "/Workspace/Users/.../pipelines/...",
  "files": [
    {"rel_path": "pipelines/foo.sql", "language": "SQL", "content": "SELECT ..."},
    {"rel_path": "pipelines/helpers.py", "language": "PYTHON", "content": "def ..."}
  ],
  "workspace_file_paths": [
    "/Workspace/.../pipelines/foo.sql",
    "/Workspace/.../pipelines/helpers.py"
  ],
  "start_run": true,
  "wait_for_completion": false,
  "full_refresh": true
}
"""

        system_prompt = """You are an expert Databricks engineer who generates Spark Declarative Pipelines.
Follow best practices from the provided skills/examples.
Output ONLY valid JSON matching the schema - no markdown, no prose, no explanations."""

        user_prompt = f"""User request:
{user_request}

Relevant SDP skills and best practices:
{json.dumps(skill_snips, indent=2)}

Constraints:
- All files MUST be under workspace_base_dir: {self.base_workspace_dir}
- workspace_file_paths MUST match files list after full path expansion
- Use Unity Catalog, serverless compute, and clean naming
- Prefer minimal file set needed
- Output JSON ONLY (no markdown code blocks)

JSON schema:
{plan_schema}
"""

        # Call model via Foundation Model API or Model Serving
        try:
            response = self.w.serving_endpoints.query(
                name=model_endpoint,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,
                max_tokens=4000
            )

            # Extract JSON from response
            content = response.choices[0].message.content

            # Try to parse JSON (may need to extract from markdown code blocks)
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()

            plan = json.loads(content)

            # Ensure workspace_base_dir is set
            plan["workspace_base_dir"] = plan.get("workspace_base_dir") or self.base_workspace_dir

            # Validate plan structure
            required_fields = ["name", "catalog", "schema", "root_path", "files", "workspace_file_paths"]
            missing = [f for f in required_fields if f not in plan]
            if missing:
                raise ValueError(f"Generated plan missing required fields: {missing}")

            return plan

        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON from LLM response: {e}\nContent: {content}")
        except Exception as e:
            raise ValueError(f"Failed to generate plan with LLM: {e}")

    def execute_plan(self, plan: Dict[str, Any]) -> PipelineRunResult:
        """Execute the generated pipeline plan"""
        base_dir = plan["workspace_base_dir"]

        # Create base directory
        print(f"Creating base directory: {base_dir}")
        try:
            create_directory(base_dir)
        except Exception as e:
            print(f"Warning creating base dir: {e}")

        # Write all files to workspace
        print(f"Writing {len(plan['files'])} files to workspace...")
        for f in plan["files"]:
            abs_path = f"{base_dir}/{f['rel_path']}"
            parent = "/".join(abs_path.split("/")[:-1])

            # Ensure parent directory exists
            try:
                create_directory(parent)
            except Exception as e:
                print(f"Warning creating parent dir {parent}: {e}")

            # Write file
            print(f"  Writing: {abs_path}")
            write_file(
                path=abs_path,
                content=f["content"],
                language=f.get("language", "SQL"),
                overwrite=True
            )

        # Create/update pipeline
        print(f"Creating/updating pipeline: {plan['name']}")
        result = create_or_update_pipeline(
            name=plan["name"],
            root_path=plan["root_path"],
            catalog=plan["catalog"],
            schema=plan["schema"],
            workspace_file_paths=plan["workspace_file_paths"],
            start_run=plan.get("start_run", True),
            wait_for_completion=plan.get("wait_for_completion", False),
            full_refresh=plan.get("full_refresh", True)
        )

        return result

    def build_pipeline(
        self,
        user_request: str,
        model_endpoint: str = "databricks-meta-llama-3-1-70b-instruct"
    ) -> Dict[str, Any]:
        """
        End-to-end: generate and execute pipeline from natural language request
        """
        try:
            # 1. Retrieve relevant skills
            print(f"Retrieving relevant skills for: {user_request[:100]}...")
            skill_snips = self.retrieve_skills(user_request, k=6)

            # 2. Generate plan using LLM
            print(f"Generating pipeline plan using model: {model_endpoint}")
            plan = self.generate_plan(user_request, model_endpoint, skill_snips)

            # 3. Execute plan
            print("Executing pipeline plan...")
            result = self.execute_plan(plan)

            return {
                "status": "success",
                "plan": plan,
                "execution_result": result.to_dict()
            }
        except Exception as e:
            print(f"Error building pipeline: {e}")
            return {
                "status": "error",
                "error": str(e)
            }


# ============================================================================
# Public API Functions
# ============================================================================

def build_sdp_pipeline(
    user_request: str,
    skills_path: Optional[str] = None,
    base_workspace_dir: Optional[str] = None,
    model_endpoint: str = "databricks-meta-llama-3-1-70b-instruct"
) -> Dict[str, Any]:
    """
    Main entry point for building SDP pipelines

    This function can be called from:
    - Notebooks: %run then call build_sdp_pipeline(...)
    - Python scripts on Databricks
    - Registered as a SQL function
    - Agent tools

    Args:
        user_request: Natural language description of pipeline
        skills_path: Optional custom skills directory path
        base_workspace_dir: Optional custom output directory
        model_endpoint: Model endpoint name (default: llama-3.1-70b)

    Returns:
        Dict with status, plan, and execution_result

    Example:
        >>> result = build_sdp_pipeline(
        ...     "Create a pipeline that reads CSV files and writes to Delta"
        ... )
        >>> print(result['execution_result']['pipeline_url'])
    """
    builder = SDPBuilder(
        workspace_client=None,
        skills_path=skills_path,
        base_workspace_dir=base_workspace_dir
    )

    return builder.build_pipeline(user_request, model_endpoint)


# Backwards compatibility with test.py
def build_sdp(user_request: str) -> Dict[str, Any]:
    """Legacy interface matching test.py signature"""
    return build_sdp_pipeline(user_request)


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # Example: Build a simple pipeline
    result = build_sdp_pipeline(
        user_request="Create a pipeline that reads from a source table, applies data quality checks, and writes to a target table in Unity Catalog",
        model_endpoint="databricks-meta-llama-3-1-70b-instruct"
    )

    print("\n" + "="*80)
    print("RESULT:")
    print("="*80)
    print(json.dumps(result, indent=2))

    if result["status"] == "success":
        exec_result = result["execution_result"]
        print(f"\nPipeline created successfully!")
        print(f"Pipeline ID: {exec_result['pipeline_id']}")
        print(f"Pipeline URL: {exec_result.get('pipeline_url', 'N/A')}")
        if exec_result.get('update_id'):
            print(f"Update ID: {exec_result['update_id']}")
            print(f"State: {exec_result.get('state', 'UNKNOWN')}")
    else:
        print(f"\nError: {result['error']}")
