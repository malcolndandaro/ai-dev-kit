"""
Claude Agent SDK integration for Databricks workflows - Simplified Version.

This uses the query() API from claude-agent-sdk which is simpler
and doesn't require managing ClaudeSDKClient instances.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional


class AgentSDKManager:
    """
    Simplified manager for Claude skills and tools discovery.

    Note: The full Agent SDK integration requires more complex async handling.
    This version provides skills discovery and tool listing without the full SDK.
    """

    def __init__(self, workspace_path: Optional[str] = None):
        """
        Initialize the manager.

        Args:
            workspace_path: Path to workspace for file operations (default: current dir)
        """
        self.workspace_path = workspace_path or os.getcwd()
        self.skills_cache: Optional[List[Dict[str, Any]]] = None

    async def initialize_session(
        self,
        session_id: str,
        event_callback: Optional[Any] = None
    ) -> None:
        """Placeholder for session initialization."""
        pass

    async def process_message(
        self,
        session_id: str,
        message: str,
        event_callback: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Process message - returns error indicating SDK not fully integrated.

        TODO: Implement full Agent SDK integration using query() API
        """
        return {
            "response": "Agent SDK integration in progress. The full query() integration requires:\n"
                       "1. Setting up async generators for streaming\n"
                       "2. Proper hook configuration\n"
                       "3. MCP server connection\n\n"
                       "For now, the app provides skills and tools discovery.",
            "tools_used": [],
            "error": False,
            "history": []
        }

    async def get_available_skills(self) -> Dict[str, Any]:
        """
        Discover available Claude skills from .claude/skills/ directory.

        Returns:
            Dictionary containing skills metadata
        """
        if self.skills_cache is not None:
            return {"skills": self.skills_cache}

        skills = []

        # Try multiple possible skills locations
        skills_dirs = [
            Path.cwd() / ".claude" / "skills",
            Path(self.workspace_path) / ".claude" / "skills",
            Path.cwd().parent / ".claude" / "skills",  # Go up one level
        ]

        for skills_dir in skills_dirs:
            if not skills_dir.exists():
                continue

            # Scan for skill directories
            for skill_path in skills_dir.iterdir():
                if not skill_path.is_dir():
                    continue

                skill_md = skill_path / "SKILL.md"
                if not skill_md.exists():
                    continue

                try:
                    # Parse SKILL.md for metadata
                    content = skill_md.read_text()
                    lines = content.split("\n")

                    # Extract title and description
                    title = skill_path.name
                    description = ""

                    for line in lines:
                        if line.startswith("# "):
                            title = line[2:].strip()
                        elif line.strip() and not line.startswith("#"):
                            description = line.strip()
                            if description:
                                break

                    skills.append({
                        "name": skill_path.name,
                        "title": title,
                        "description": description,
                        "path": str(skill_path)
                    })

                except Exception as e:
                    print(f"Error parsing skill {skill_path.name}: {e}")
                    continue

            # If we found skills, stop looking
            if skills:
                break

        self.skills_cache = skills
        return {"skills": skills}

    async def disconnect_session(self, session_id: str) -> None:
        """Disconnect session - placeholder."""
        pass

    async def disconnect_all(self) -> None:
        """Disconnect all sessions - placeholder."""
        pass
