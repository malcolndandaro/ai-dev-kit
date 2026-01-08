import json
import os
from pathlib import Path
from typing import List, Dict, Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType

from .llm import call_llm


def _is_workspace_path(path: str) -> bool:
    return path.startswith("/Workspace/")


def load_skills(skills_path: str) -> List[Dict[str, str]]:
    """
    Load markdown skills from either local filesystem or Databricks Workspace.
    Returns a list of dicts: {\"path\": str, \"text\": str}
    """
    items: List[Dict[str, str]] = []

    if _is_workspace_path(skills_path):
        w = WorkspaceClient()
        try:
            for item in w.workspace.list(skills_path, recursive=True):
                if (
                    item.path
                    and item.path.endswith(".md")
                    and item.object_type != ObjectType.DIRECTORY
                ):
                    try:
                        content = (
                            w.workspace.download(item.path).read().decode("utf-8")
                        )
                        items.append({"path": item.path, "text": content})
                    except Exception:
                        # Skip unreadable files
                        continue
        except Exception:
            # Path may not exist; return empty list
            return items
        return items

    # Local filesystem
    root = Path(skills_path).resolve()
    if not root.exists():
        return items

    for p in root.rglob("*.md"):
        try:
            items.append({"path": str(p), "text": p.read_text(encoding="utf-8")})
        except Exception:
            continue

    return items


def retrieve_relevant_skills(
    query: str,
    skills: List[Dict[str, str]],
    model: str,
    k: int = 6,
) -> List[Dict[str, str]]:
    """
    Use LLM to select the most relevant skills. Falls back to first k on failure.
    Returns list of {\"path\": str, \"snippet\": str}.
    """
    if not skills:
        return []

    skills_summary = []
    for idx, skill in enumerate(skills):
        first_lines = skill["text"].split("\n")[:3]
        preview = " ".join(first_lines).strip()[:200]
        skills_summary.append({"id": idx, "path": skill["path"], "preview": preview})

    system = "You are a Databricks expert. Select the most relevant skill documentation for the user's request."
    user_prompt = (
        f"User request: {query}\n\n"
        f"Available skills:\n{json.dumps(skills_summary, indent=2)}\n\n"
        f"Select the {k} most relevant skills for this request. "
        "Output ONLY a JSON array of skill IDs.\n"
        "Example: [0, 2, 5, 7, 9, 11]\n\n"
        "Consider:\n"
        "- Which skills contain relevant tables, transformations, or patterns?\n"
        "- Which skills match the data processing needs?\n"
        "- Which skills are most applicable to the request?\n\n"
        "Output JSON array only:"
    )

    try:
        content = call_llm(system=system, user_prompt=user_prompt, model=model, temperature=0.1, max_tokens=500)
        selected_ids = json.loads(content)
        selected: List[Dict[str, str]] = []
        for skill_id in selected_ids[:k]:
            if 0 <= skill_id < len(skills):
                selected.append(
                    {
                        "path": skills[skill_id]["path"],
                        "snippet": skills[skill_id]["text"][:2500],
                    }
                )
        return selected
    except Exception:
        # Fallback to first k
        return [
            {"path": s["path"], "snippet": s["text"][:2500]} for s in skills[:k]
        ]


