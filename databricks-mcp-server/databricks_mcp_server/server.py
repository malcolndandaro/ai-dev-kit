"""
Databricks MCP Server

FastAPI server implementing MCP protocol over Server-Sent Events (SSE).
"""
import json
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

from .tools import unity_catalog, compute, spark_declarative_pipelines

app = FastAPI(title="Databricks MCP Server (SSE)")


@app.get("/sse")
async def sse_endpoint():
    """SSE endpoint for MCP communication"""
    async def event_generator():
        # Send endpoint event
        endpoint_event = {
            "jsonrpc": "2.0",
            "method": "endpoint",
            "params": {
                "endpoint": "/message"
            }
        }
        yield f"data: {json.dumps(endpoint_event)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no"
        }
    )


@app.post("/message")
async def message_endpoint(request: Request):
    """Handle MCP JSON-RPC messages"""
    try:
        request_data = await request.json()
        method = request_data.get("method")

        if method == "initialize":
            response = {
                "jsonrpc": "2.0",
                "id": request_data.get("id"),
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {}
                    },
                    "serverInfo": {
                        "name": "databricks-mcp-server",
                        "version": "0.1.0"
                    }
                }
            }

        elif method == "tools/list":
            # Get tool definitions from all modules
            uc_tools = unity_catalog.get_tool_definitions()
            compute_tools = compute.get_tool_definitions()
            sdp_tools = spark_declarative_pipelines.get_tool_definitions()

            response = {
                "jsonrpc": "2.0",
                "id": request_data.get("id"),
                "result": {
                    "tools": uc_tools + compute_tools + sdp_tools
                }
            }

        elif method == "tools/call":
            params = request_data.get("params", {})
            tool_name = params.get("name")
            arguments = params.get("arguments", {})

            # Dispatch to appropriate tool handler
            if tool_name in unity_catalog.TOOL_HANDLERS:
                result = unity_catalog.TOOL_HANDLERS[tool_name](arguments)
            elif tool_name in compute.TOOL_HANDLERS:
                result = compute.TOOL_HANDLERS[tool_name](arguments)
            elif tool_name in spark_declarative_pipelines.TOOL_HANDLERS:
                result = spark_declarative_pipelines.TOOL_HANDLERS[tool_name](arguments)
            else:
                response = {
                    "jsonrpc": "2.0",
                    "id": request_data.get("id"),
                    "error": {
                        "code": -32601,
                        "message": f"Unknown tool: {tool_name}"
                    }
                }
                return response

            response = {
                "jsonrpc": "2.0",
                "id": request_data.get("id"),
                "result": result
            }
        else:
            response = {
                "jsonrpc": "2.0",
                "id": request_data.get("id"),
                "error": {
                    "code": -32601,
                    "message": f"Unknown method: {method}"
                }
            }

        return response

    except Exception as e:
        return {
            "jsonrpc": "2.0",
            "id": request_data.get("id") if 'request_data' in locals() else None,
            "error": {
                "code": -32603,
                "message": str(e)
            }
        }


@app.get("/")
async def health():
    """Health check endpoint"""
    return {"status": "ok", "transport": "sse"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
