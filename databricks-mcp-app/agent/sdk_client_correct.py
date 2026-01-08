"""
Claude Agent SDK integration for Databricks workflows - Correct Implementation.

This uses ClaudeSDKClient for maintaining conversation sessions across
multiple exchanges, which is required for FastAPI integration.
"""

import os
import asyncio
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from datetime import datetime

from claude_agent_sdk import (
    ClaudeSDKClient,
    ClaudeAgentOptions,
    AssistantMessage,
    TextBlock,
    ToolUseBlock,
    ToolResultBlock,
    ResultMessage,
)


class AgentSDKManager:
    """
    Manages Claude SDK clients for Databricks workflows.

    Uses ClaudeSDKClient to maintain conversation sessions,
    allowing multiple exchanges in the same context.
    """

    def __init__(self, workspace_path: Optional[str] = None):
        """
        Initialize the Agent SDK Manager.

        Args:
            workspace_path: Path to workspace for file operations
        """
        self.workspace_path = workspace_path or os.getcwd()
        self.sessions: Dict[str, ClaudeSDKClient] = {}
        self.skills_cache: Optional[List[Dict[str, Any]]] = None

    async def get_or_create_client(
        self,
        session_id: str,
        event_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> ClaudeSDKClient:
        """
        Get existing client or create new one for session.

        Args:
            session_id: Session identifier
            event_callback: Optional callback for events

        Returns:
            ClaudeSDKClient instance
        """
        if session_id not in self.sessions:
            # Configure Agent SDK options
            options = ClaudeAgentOptions(
                # Built-in SDK tools
                allowed_tools=[
                    "Read", "Write", "Edit",  # File operations
                    "Bash",                   # Command execution
                    "Glob", "Grep",          # Code search
                ],
                # MCP server for Databricks tools
                mcp_servers={
                    "databricks": {
                        "command": "python",
                        "args": ["-m", "databricks_mcp_server.stdio_server"]
                    }
                },
                # Load skills from .claude/skills/
                setting_sources=["project"],
                # Auto-approve for demo (change for production)
                permission_mode="acceptEdits",
                # Working directory
                cwd=self.workspace_path,
                # Max conversation turns
                max_turns=50
            )

            # Create client
            client = ClaudeSDKClient(options=options)

            # Connect to Claude
            await client.connect()

            # Store client
            self.sessions[session_id] = client

        return self.sessions[session_id]

    async def process_message(
        self,
        session_id: str,
        message: str,
        event_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> Dict[str, Any]:
        """
        Process a user message with the Agent SDK.

        Args:
            session_id: Session identifier
            message: User message
            event_callback: Optional callback for real-time events

        Returns:
            Dictionary with response and metadata
        """
        try:
            # Get or create client for this session
            client = await self.get_or_create_client(session_id, event_callback)

            # Send query to Claude
            await client.query(message)

            # Collect response
            response_text = []
            tools_used = []
            error = False

            # Iterate through messages
            async for msg in client.receive_response():
                if isinstance(msg, AssistantMessage):
                    for block in msg.content:
                        if isinstance(block, TextBlock):
                            response_text.append(block.text)
                        elif isinstance(block, ToolUseBlock):
                            # Tool is being used
                            tool_info = {
                                "name": block.name,
                                "input": block.input
                            }
                            tools_used.append(tool_info)

                            # Call event callback if provided
                            if event_callback:
                                try:
                                    event_callback({
                                        "type": "tool_start",
                                        "tool": block.name,
                                        "timestamp": datetime.now().isoformat()
                                    })
                                except Exception as e:
                                    print(f"Event callback error: {e}")

                        elif isinstance(block, ToolResultBlock):
                            # Tool completed
                            if event_callback:
                                try:
                                    event_callback({
                                        "type": "tool_complete",
                                        "tool_use_id": block.tool_use_id,
                                        "timestamp": datetime.now().isoformat()
                                    })
                                except Exception as e:
                                    print(f"Event callback error: {e}")

                elif isinstance(msg, ResultMessage):
                    # Final result
                    if msg.is_error:
                        error = True
                    break

            return {
                "response": "\n".join(response_text) if response_text else "Task completed successfully.",
                "tools_used": tools_used,
                "error": error,
                "history": []  # Client manages history internally
            }

        except Exception as e:
            print(f"Error processing message: {e}")
            return {
                "response": f"Error: {str(e)}",
                "tools_used": [],
                "error": True,
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
            Path.cwd().parent / ".claude" / "skills",
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
        """
        Disconnect and clean up a session.

        Args:
            session_id: Session identifier to disconnect
        """
        if session_id in self.sessions:
            client = self.sessions[session_id]
            try:
                await client.disconnect()
            except Exception as e:
                print(f"Error disconnecting session {session_id}: {e}")
            finally:
                del self.sessions[session_id]

    async def disconnect_all(self) -> None:
        """Disconnect all active sessions."""
        for session_id in list(self.sessions.keys()):
            await self.disconnect_session(session_id)
