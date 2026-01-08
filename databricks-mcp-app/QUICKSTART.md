# Quick Start Guide - Claude Agent SDK Integration

## ✅ Setup Complete!

Your databricks-mcp-app has been successfully enhanced with the Claude Agent SDK.

## What Was Done

1. **Integrated Claude Agent SDK** - Replaced custom orchestration with Agent SDK
2. **Added Hybrid Tooling** - Combined built-in tools (Read/Write/Edit/Bash) with your MCP tools
3. **Skills Integration** - Automatically loads skills from `.claude/skills/`
4. **New API Endpoints** - Added `/api/skills` for skill discovery
5. **Enhanced Tools Endpoint** - `/api/tools` now shows both built-in and MCP tools

## Running the App

### Prerequisites
- ✅ Claude Code CLI installed (you have `claude` version 2.0.76)
- ✅ Anthropic API key exported
- ✅ Python dependencies installed

### Start the App

```bash
cd /Users/cal.reynolds/Downloads/skunkworks/ai-dev-kit/databricks-mcp-app

# Make sure your API key is set
export ANTHROPIC_API_KEY='your-key-here'

# Optional: Set Databricks profile
export DATABRICKS_CONFIG_PROFILE='your-profile'

# Run the app
python app.py
```

The app will start on **http://localhost:8081**

## Testing the Endpoints

```bash
# Health check
curl http://localhost:8081/api/health

# List available skills
curl http://localhost:8081/api/skills

# List all tools (built-in + MCP)
curl http://localhost:8081/api/tools

# Open in browser
open http://localhost:8081
```

## What You Can Do Now

### Example Prompts

Try these in the chat interface:

1. **Synthetic Data Generation**
   ```
   Generate 1000 customer records with realistic data and save to a file
   ```

2. **Unity Catalog Operations**
   ```
   Create a new catalog called 'analytics' with a schema 'customer_data'
   ```

3. **Pipeline Creation**
   ```
   Generate synthetic customer data and create a DLT pipeline to process it
   ```

4. **Multi-Step Workflows**
   ```
   Set up a complete data pipeline: generate test data, upload to Unity Catalog Volume, create processing pipeline
   ```

## How It Works

### Architecture

```
User Request
    ↓
FastAPI Backend
    ↓
Agent SDK Manager
    ↓
┌───────────┴──────────┐
Built-in Tools    MCP Tools
(Read/Write/     (Unity Catalog,
 Edit/Bash/       Pipelines,
 Glob/Grep)       Synthetic Data)
    ↓                 ↓
Files/Commands    Databricks
```

### Skills Available

The Agent SDK automatically loads these skills from [`.claude/skills/`](../.claude/skills/):

1. **synthetic-data-generation** - Generate realistic test data with Faker
2. **databricks-python-sdk** - Databricks SDK patterns and best practices
3. **python-dev** - Python development standards
4. **sdp-writer** - Spark Declarative Pipeline guidance

Skills provide specialized guidance for complex workflows and are automatically activated when relevant.

## Troubleshooting

### App won't start

**Issue**: Import errors with `mcp.types`
**Solution**: Already fixed - renamed local `mcp/` directory to `mcp_local/` to avoid conflicts

**Issue**: `ANTHROPIC_API_KEY` not set
**Solution**:
```bash
export ANTHROPIC_API_KEY='your-key-from-console.anthropic.com'
```

### API calls return empty

**Check the app logs**:
```bash
# If running in background, check output
cat /tmp/claude/-Users-cal-reynolds-Downloads-skunkworks-ai-dev-kit/tasks/*.output
```

### Need to reset

```bash
# Kill the app
pkill -f "python app.py"

# Restart
python app.py
```

## Key Files Modified

- [`requirements.txt`](requirements.txt) - Added `claude-agent-sdk` and `mcp`
- [`agent/sdk_client.py`](agent/sdk_client.py) - NEW: Agent SDK wrapper
- [`app.py`](app.py) - Updated to use Agent SDK
- [`llm/orchestrator.py`](llm/orchestrator.py) - Fixed import for local MCP client
- `mcp/` → `mcp_local/` - Renamed to avoid package conflicts

## Next Steps

### Optional Enhancements

1. **Frontend** - Add skills panel to [static/index.html](static/index.html)
2. **Documentation** - Update [README.md](README.md) with new architecture
3. **Deploy** - Deploy to Databricks Apps using existing [app.yaml](app.yaml)

### Verify Everything Works

1. Open http://localhost:8080 in your browser
2. Try the example prompts above
3. Watch the tool execution in real-time
4. Check the skills are being used automatically

## Support

- Agent SDK Docs: https://platform.claude.com/docs/en/agent-sdk/overview
- Python SDK Reference: https://platform.claude.com/docs/en/agent-sdk/python
- Full Implementation Guide: [AGENT_SDK_INTEGRATION.md](AGENT_SDK_INTEGRATION.md)

## Summary

🎉 **You're all set!** Your app now uses the Claude Agent SDK for superior orchestration, hybrid tooling (built-in + MCP), and automatic skills integration. It can handle complex multi-step workflows like generating synthetic data and creating pipelines in a single request.

The app is currently running on port 8080 - open it in your browser and try it out!
