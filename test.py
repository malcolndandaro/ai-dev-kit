import os, re, json
from typing import List, Dict, Any, Tuple
import sys

REPO_ROOT = "/Workspace/Repos/cal.reynolds@databricks.com/ai-dev-kit"  # exact path
sys.path.insert(0, REPO_ROOT)
# ---- your existing core imports ----
from databricks_mcp_core.databricks_mcp_core.spark_declarative_pipelines.pipelines import create_or_update_pipeline
from databricks_mcp_core.databricks_mcp_core.spark_declarative_pipelines.workspace_files import write_file, create_directory

# ============ CONFIG ============
SDP_SKILLS_DIR = "/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-skills/sdp"

BASE_WORKSPACE_DIR = "/Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit"  # adjust

# ============ 1) LOAD SKILLS ============
def load_md_files(root: str) -> List[Dict[str, str]]:
    items = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.endswith(".md"):
                path = os.path.join(dirpath, fn)
                with open(path, "r", encoding="utf-8") as f:
                    items.append({"path": path, "text": f.read()})
    return items

SKILLS = load_md_files(SDP_SKILLS_DIR)

# ============ 2) RETRIEVE RELEVANT SKILLS ============
def score(text: str, query: str) -> int:
    q = re.findall(r"[a-zA-Z0-9_]+", query.lower())
    t = text.lower()
    return sum(3 for w in q if w in t)

def retrieve_skills(query: str, k: int = 6, max_chars_each: int = 2500) -> List[Dict[str, str]]:
    ranked = sorted(SKILLS, key=lambda x: score(x["text"], query), reverse=True)
    top = []
    for item in ranked[:k]:
        snippet = item["text"][:max_chars_each]
        top.append({"path": item["path"], "snippet": snippet})
    return top

# ============ 3) LLM PLAN ============
PLAN_SCHEMA = """
Return STRICT JSON:
{
  "name": "...",
  "catalog": "...",
  "schema": "...",
  "root_path": "...",
  "workspace_base_dir": "...",
  "files": [
    {"rel_path": "pipelines/foo.sql", "language": "SQL", "content": "..."},
    {"rel_path": "pipelines/helpers.py", "language": "PYTHON", "content": "..."}
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

def call_llm_for_plan(user_request: str, skill_snips: List[Dict[str, str]]) -> Dict[str, Any]:
    # Replace this with: DBRX endpoint call OR Claude call OR whatever you already use.
    # IMPORTANT: force JSON-only output.
    prompt = {
        "system": "You generate Spark Declarative Pipelines on Databricks. Follow best practices. Output JSON only.",
        "user": f"""
User request:
{user_request}

Relevant SDP skills / best practices:
{json.dumps(skill_snips, indent=2)}

Constraints:
- Files MUST be written under workspace_base_dir
- workspace_file_paths MUST match the files list after expansion
- Prefer minimal file set needed to satisfy request
- Use Unity Catalog, serverless, and clean naming
- Output JSON ONLY. No prose.

JSON schema:
{PLAN_SCHEMA}
"""
    }

    # TODO: implement your model call here and parse JSON
    raise NotImplementedError("Wire this to your LLM")

# ============ 4) EXECUTE ============
def execute_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    base_dir = plan["workspace_base_dir"]
    create_directory(base_dir)

    # Write files
    for f in plan["files"]:
        abs_path = f"{base_dir}/{f['rel_path']}"
        # ensure parent dirs exist (simple approach: mkdir each parent)
        parent = "/".join(abs_path.split("/")[:-1])
        create_directory(parent)
        write_file(path=abs_path, content=f["content"], language=f.get("language","SQL"), overwrite=True)

    # Create/update pipeline
    result = create_or_update_pipeline(
        name=plan["name"],
        root_path=plan["root_path"],
        catalog=plan["catalog"],
        schema=plan["schema"],
        workspace_file_paths=plan["workspace_file_paths"],
        start_run=bool(plan.get("start_run", True)),
        wait_for_completion=bool(plan.get("wait_for_completion", False)),
        full_refresh=bool(plan.get("full_refresh", True)),
    )
    return result.to_dict()

def build_sdp(user_request: str) -> Dict[str, Any]:
    skill_snips = retrieve_skills(user_request, k=6)
    # set base dir so LLM doesn't invent it
    # (you can also pass BASE_WORKSPACE_DIR in the prompt and force it)
    plan = call_llm_for_plan(user_request, skill_snips)
    plan["workspace_base_dir"] = plan.get("workspace_base_dir") or BASE_WORKSPACE_DIR
    return execute_plan(plan)