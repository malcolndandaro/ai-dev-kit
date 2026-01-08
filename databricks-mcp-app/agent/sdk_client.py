"""
Claude Agent SDK integration for Databricks workflows.

This module wraps the Claude Agent SDK to provide:
- Hybrid tooling (MCP tools + built-in SDK tools)
- Skills integration from .claude/skills/
- Event streaming for real-time progress updates
- Session management with conversation continuity
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
    HookMatcher,
    HookContext,
)


class AgentSDKManager:
    """
    Manages Claude Agent SDK for Databricks workflows.

    Provides a FastAPI-friendly interface to the Agent SDK with:
    - Session management with conversation continuity
    - Real-time event streaming via callbacks
    - Skills discovery from .claude/skills/
    - Hybrid tool access (MCP + built-in)
    """

    def __init__(self, workspace_path: Optional[str] = None):
        """
        Initialize the Agent SDK Manager.

        Args:
            workspace_path: Path to workspace for file operations (default: current dir)
        """
        self.workspace_path = workspace_path or os.getcwd()
        self.sessions: Dict[str, ClaudeSDKClient] = {}
        self.skills_cache: Optional[List[Dict[str, Any]]] = None

    async def initialize_session(
        self,
        session_id: str,
        event_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> None:
        """
        Initialize a new Agent SDK session.

        Args:
            session_id: Unique session identifier
            event_callback: Optional callback for real-time event streaming
        """
        # Create hooks for event streaming
        hooks = {}
        if event_callback:
            hooks = self._create_event_hooks(event_callback)

        # Configure Agent SDK options
        options = ClaudeAgentOptions(
            # Built-in SDK tools for file and code operations
            allowed_tools=[
                "Read", "Write", "Edit",  # File operations
                "Bash",                   # Command execution
                "Glob", "Grep",          # Code search
            ],
            # MCP server configuration for Databricks tools
            mcp_servers={
                "databricks": {
                    "command": "python",
                    "args": ["-m", "databricks_mcp_server.stdio_server"]
                }
            },
            # Load skills from .claude/skills/ directory
            setting_sources=["project"],
            # Auto-approve file edits for demo (change to "default" for production)
            permission_mode="acceptEdits",
            # Working directory for file operations
            cwd=self.workspace_path,
            # Hook configuration for event streaming
            hooks=hooks if hooks else None,
            # Max conversation turns
            max_turns=50
        )

        # Create and store the client
        client = ClaudeSDKClient(options=options)
        await client.connect()
        self.sessions[session_id] = client

    def _create_event_hooks(
        self,
        event_callback: Callable[[Dict[str, Any]], None]
    ) -> Dict[str, List[HookMatcher]]:
        """
        Create hooks for streaming events to FastAPI.

        Args:
            event_callback: Callback function for event streaming

        Returns:
            Dictionary of hook configurations
        """
        async def pre_tool_hook(
            input_data: Dict[str, Any],
            tool_use_id: Optional[str],
            context: HookContext
        ) -> Dict[str, Any]:
            """Hook fired before tool execution."""
            try:
                asyncio.create_task(asyncio.to_thread(
                    event_callback,
                    {
                        "type": "tool_start",
                        "tool": input_data.get("tool_name"),
                        "tool_use_id": tool_use_id,
                        "input": input_data.get("tool_input"),
                        "timestamp": datetime.now().isoformat()
                    }
                ))
            except Exception as e:
                print(f"Error in pre_tool_hook: {e}")
            return {}

        async def post_tool_hook(
            input_data: Dict[str, Any],
            tool_use_id: Optional[str],
            context: HookContext
        ) -> Dict[str, Any]:
            """Hook fired after tool execution."""
            try:
                asyncio.create_task(asyncio.to_thread(
                    event_callback,
                    {
                        "type": "tool_complete",
                        "tool": input_data.get("tool_name"),
                        "tool_use_id": tool_use_id,
                        "response": str(input_data.get("tool_response", ""))[:500],  # Truncate for SSE
                        "timestamp": datetime.now().isoformat()
                    }
                ))
            except Exception as e:
                print(f"Error in post_tool_hook: {e}")
            return {}

        return {
            "PreToolUse": [HookMatcher(hooks=[pre_tool_hook])],
            "PostToolUse": [HookMatcher(hooks=[post_tool_hook])]
        }

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
            message: User message to process
            event_callback: Optional callback for real-time events

        Returns:
            Dictionary containing response and metadata
        """
        # Get or create session
        if session_id not in self.sessions:
            await self.initialize_session(session_id, event_callback)

        client = self.sessions[session_id]

        # Send query to agent
        await client.query(message)

        # Collect response
        response_text = []
        tools_used = []
        error = False

        try:
            async for msg in client.receive_response():
                if isinstance(msg, AssistantMessage):
                    for block in msg.content:
                        if isinstance(block, TextBlock):
                            response_text.append(block.text)
                        elif isinstance(block, ToolUseBlock):
                            tools_used.append({
                                "name": block.name,
                                "input": block.input
                            })

                elif isinstance(msg, ResultMessage):
                    # Final result message
                    if msg.is_error:
                        error = True
                    break

        except Exception as e:
            error = True
            response_text.append(f"Error processing message: {str(e)}")

        return {
            "response": "\n".join(response_text) if response_text else "Task completed successfully.",
            "tools_used": tools_used,
            "error": error,
            "history": []  # Agent SDK manages history internally
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
        skills_dir = Path.cwd() / ".claude" / "skills"

        if not skills_dir.exists():
            return {"skills": []}

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
            await client.disconnect()
            del self.sessions[session_id]

    async def disconnect_all(self) -> None:
        """Disconnect all active sessions."""
        for session_id in list(self.sessions.keys()):
            await self.disconnect_session(session_id)
