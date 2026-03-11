---
name: aidevkit:flow:introspect-fork
description: Reports available tools and context (runs as forked agent)
context: fork
agent: general-purpose
allowed-tools:
  - Bash
  - Read
  - Glob
  - Grep
---

# Self-Introspection (Forked Agent)

Do the following and report back:

1. List every tool you currently have access to
2. State whether you can see any conversation history
3. Run: `echo "Hello from forked skill — PID $$"`
4. Summarize: are you a separate agent, or part of the main conversation?
