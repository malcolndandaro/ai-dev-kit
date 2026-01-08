# AI Agent Tool Integration

This guide shows how to use `build_pipeline` as a tool for AI agents using Unity Catalog functions.

## Overview

The `build_sdp_pipeline` function is registered as a Unity Catalog function that AI agents can call to build Spark Declarative Pipelines from natural language descriptions.

**Key Features**:
- ✅ Automatic catalog/schema extraction from user requests
- ✅ Full access to Databricks SDK (runs in serverless mode)
- ✅ Schema introspection and validation
- ✅ Skills-driven SQL generation
- ✅ Compatible with LangChain, LlamaIndex, OpenAI, Anthropic

## Quick Start

### 1. Install Dependencies

```bash
pip install unitycatalog-ai[databricks]
pip install unitycatalog-langchain[databricks]
pip install databricks-langchain
```

### 2. Register the UC Function

```python
from databricks_ai_functions.sdp.register_agent_tool import register_tool

# Register function in Unity Catalog
register_tool(
    catalog="main",
    schema="ai_functions",
    replace=True
)
```

Or use the command line:

```bash
cd databricks-ai-functions
python databricks_ai_functions/sdp/register_agent_tool.py main ai_functions
```

### 3. Use in an AI Agent

```python
from databricks_langchain import UCFunctionToolkit, ChatDatabricks
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain.prompts import ChatPromptTemplate

# Create toolkit with UC function
toolkit = UCFunctionToolkit(
    function_names=["main.ai_functions.build_sdp_pipeline"]
)
tools = toolkit.tools

# Initialize LLM
llm = ChatDatabricks(endpoint="databricks-gpt-5-2", temperature=0.1)

# Create prompt
prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a data engineering assistant that builds pipelines."),
    ("human", "{input}"),
    ("placeholder", "{agent_scratchpad}"),
])

# Create and run agent
agent = create_tool_calling_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# Ask the agent to build a pipeline!
response = agent_executor.invoke({
    "input": "Create a pipeline to analyze customer orders from main.sales.orders"
})
```

## How It Works

### Architecture

```
User Request → AI Agent → UC Function (build_sdp_pipeline) → build_pipeline()
                                ↓
                    Serverless Compute (with full SDK access)
                                ↓
                    Pipeline Created & Deployed
```

### The UC Function

The registered function signature:

```python
def build_sdp_pipeline(
    user_request: str,
    model: str = "databricks-gpt-5-2"
) -> str:
    """
    Build a Spark Declarative Pipeline from natural language.
    
    Args:
        user_request: Natural language pipeline description
        model: LLM endpoint name
        
    Returns:
        JSON string with pipeline details
    """
```

### Catalog/Schema Extraction

The function automatically extracts catalog and schema from patterns like:

```python
"Target: main.ai_dev_kit"           → catalog="main", schema="ai_dev_kit"
"output to analytics schema"        → schema="analytics"
"save to production catalog"        → catalog="production"
```

Defaults: `catalog="main"`, `schema="default"`

## Example User Requests

### Basic Bronze-Silver-Gold

```
"Create a pipeline from samples.tpch.orders with:
- Bronze: Load raw data
- Silver: Filter valid orders
- Gold: Daily aggregations
Target: main.analytics"
```

### SCD Type 2

```
"Build a customer history pipeline from main.crm.customers using:
- Bronze: Stream customer updates
- Silver: SCD Type 2 with AUTO CDC to track changes
- Gold: Current customer view
Target: main.analytics"
```

### Multi-Source Join

```
"Create a sales analytics pipeline:
- Bronze: Ingest from samples.tpch.orders and samples.tpch.customer
- Silver: Join orders with customers, clean data
- Gold: Revenue by customer segment
Target: main.sales_analytics"
```

## Testing

### Test Registration

```bash
cd databricks-ai-functions
python test_agent_tool_registration.py
```

### Test Direct Execution

```python
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()

result = client.execute_function(
    function_name="main.ai_functions.build_sdp_pipeline",
    parameters={
        "user_request": "Create a pipeline from samples.nyctaxi.trips",
        "model": "databricks-gpt-5-2"
    }
)

print(result.value)  # JSON with pipeline details
```

## Agent Frameworks

### LangChain (shown above)

See [Databricks LangChain Integration](https://docs.databricks.com/en/generative-ai/agent-framework/create-custom-tool.html)

### LlamaIndex

```python
from llama_index.core.agent import ReActAgent
from databricks_langchain import UCFunctionToolkit

toolkit = UCFunctionToolkit(
    function_names=["main.ai_functions.build_sdp_pipeline"]
)
tools = toolkit.tools

agent = ReActAgent.from_tools(tools, verbose=True)
response = agent.chat("Build me a data pipeline for sales analysis")
```

### OpenAI Function Calling

```python
import openai
from databricks_langchain import UCFunctionToolkit

toolkit = UCFunctionToolkit(
    function_names=["main.ai_functions.build_sdp_pipeline"]
)

# Convert to OpenAI tool format
tools_schema = [
    {
        "type": "function",
        "function": {
            "name": "build_sdp_pipeline",
            "description": toolkit.tools[0].description,
            "parameters": {
                "type": "object",
                "properties": {
                    "user_request": {
                        "type": "string",
                        "description": "Pipeline description"
                    }
                },
                "required": ["user_request"]
            }
        }
    }
]

response = openai.chat.completions.create(
    model="gpt-4",
    messages=[{"role": "user", "content": "Build a pipeline..."}],
    tools=tools_schema
)
```

## Production Deployment

### Deploy as Databricks Agent

See [Agent Framework Documentation](https://docs.databricks.com/en/generative-ai/agent-framework/index.html)

### Permissions

Grant EXECUTE permission to users/groups:

```sql
GRANT EXECUTE ON FUNCTION main.ai_functions.build_sdp_pipeline 
TO `data-engineers@company.com`;
```

### Monitoring

View function execution logs:

```sql
SELECT *
FROM system.access.audit
WHERE request_params.function_name = 'main.ai_functions.build_sdp_pipeline'
ORDER BY event_time DESC;
```

## Execution Modes

### Serverless (Default - Production)

```python
client = DatabricksFunctionClient(execution_mode="serverless")
```

- ✅ Full Databricks SDK access
- ✅ Secure isolation via Lakeguard
- ✅ Production-ready
- ✅ Scalable

### Local (Development Only)

```python
client = DatabricksFunctionClient(execution_mode="local")
```

- ✅ Fast local testing
- ✅ Better error messages
- ❌ No SDK access (will fail for build_pipeline)
- ❌ Not for production

## Troubleshooting

### "Cannot access Spark Connect"

**Cause**: Serverless generic compute not enabled

**Solution**: Enable serverless in workspace settings

### "Module not found: databricks_ai_functions"

**Cause**: Package not accessible in UC function

**Solution**: UC functions run in serverless with packages pre-installed or via ENVIRONMENT clause. Consider creating a wheel and installing it:

```python
# In register_agent_tool.py, add:
function_info = client.create_python_function(
    func=build_sdp_pipeline,
    catalog=catalog,
    schema=schema,
    replace=replace,
    # Add environment with dependencies
    environment={
        "dependencies": [
            "databricks-sdk>=0.31.0",
            "/Volumes/main/ai_functions/wheels/databricks_ai_functions-0.1.0-py3-none-any.whl"
        ]
    }
)
```

### "Skills not found"

**Cause**: Workspace files not accessible from UC function

**Solution**: Move skills to UC Volumes:

```bash
databricks workspace export-dir \
  databricks-skills/sdp \
  /Volumes/main/ai_functions/skills/sdp
```

Then update `build_pipeline.py` to read from volumes.

## Best Practices

1. **Clear Documentation**: Write detailed docstrings following [Google style](https://google.github.io/styleguide/pyguide.html#383-functions-and-methods)
2. **Error Handling**: Return structured JSON errors for agent parsing
3. **Catalog/Schema**: Always include target location in requests
4. **Testing**: Test UC function directly before adding to agent
5. **Permissions**: Use least-privilege access for production
6. **Monitoring**: Track function usage and errors

## References

- [Create AI Agent Tools with UC Functions](https://docs.databricks.com/aws/en/generative-ai/agent-framework/create-custom-tool)
- [Unity Catalog UDFs](https://docs.databricks.com/aws/en/udf/unity-catalog)
- [Databricks Agent Framework](https://docs.databricks.com/aws/en/generative-ai/agent-framework/index.html)
- [UCFunctionToolkit Documentation](https://github.com/databrickslabs/unitycatalog-ai)

