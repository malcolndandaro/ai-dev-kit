# Deploying to Databricks

This guide explains how to upload and run the AI Dev Kit on Databricks (workspace, notebooks, or jobs).

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Access to a serving endpoint (e.g., `databricks-gpt-5-2`)
- Python 3.10+

---

## Option 1: Run in Databricks Notebooks (Recommended)

### Step 1: Upload to Workspace

Upload the entire project to Databricks Workspace using the CLI:

```bash
# Upload from your local machine
databricks workspace import-dir \
  /Users/cal.reynolds/Downloads/skunkworks/ai-dev-kit \
  /Workspace/Users/<your-username>/ai-dev-kit \
  --overwrite
```

Or use the Databricks UI:
1. Go to Workspace → Users → Your User
2. Create a folder called `ai-dev-kit`
3. Upload the project folders

### Step 2: Install Dependencies in Notebook

Create a notebook and run:

```python
# Install core dependencies
%pip install databricks-sdk>=0.31.0

# Install local packages (from workspace)
import sys
sys.path.insert(0, '/Workspace/Users/<your-username>/ai-dev-kit/databricks-tools-core')
sys.path.insert(0, '/Workspace/Users/<your-username>/ai-dev-kit/databricks-ai-functions')

# Verify imports work
from databricks_ai_functions.sdp.build_pipeline import build_pipeline
print("✅ Successfully imported build_pipeline")
```

### Step 3: Run Your Tests

```python
# Test basic pipeline
result = build_pipeline(
    user_request="""
    Create a simple data processing pipeline:
    
    1. Bronze: Load raw data from samples.tpch.orders
    2. Silver: Clean the data - remove nulls and filter for valid orders
    3. Gold: Create daily aggregations - count orders and sum totals by order date
    
    Keep it simple - just basic transformations, no CDC or SCD.
    """,
    catalog="main",
    schema="ai_dev_kit",
    model="databricks-gpt-5-2",
    start_run=True,
)

print(result)
```

---

## Option 2: Run as Databricks Jobs

### Step 1: Upload Files to Workspace

Same as Option 1, Step 1.

### Step 2: Create Job with Dependencies

Create a Databricks job with:

**Cluster Configuration**:
- Runtime: DBR 14.3 LTS or higher
- Python: 3.10+
- Libraries: `databricks-sdk>=0.31.0`

**Task Configuration**:
```yaml
task_key: run_build_pipeline
type: python_wheel
python_file: /Workspace/Users/<your-username>/ai-dev-kit/databricks-ai-functions/test_basic_pipeline.py
libraries:
  - pypi:
      package: databricks-sdk>=0.31.0
parameters:
  PYTHONPATH: /Workspace/Users/<your-username>/ai-dev-kit/databricks-tools-core:/Workspace/Users/<your-username>/ai-dev-kit/databricks-ai-functions
```

---

## Option 3: Install as Python Packages (Most Robust)

### Step 1: Build Wheels Locally

```bash
# Build databricks-tools-core
cd databricks-tools-core
python -m build

# Build databricks-ai-functions
cd ../databricks-ai-functions
python -m build
```

This creates `.whl` files in `dist/` folders.

### Step 2: Upload Wheels to Workspace or DBFS

```bash
# Upload to workspace
databricks workspace upload dist/databricks_tools_core-0.1.0-py3-none-any.whl \
  /Workspace/Users/<your-username>/ai-dev-kit/wheels/databricks_tools_core-0.1.0-py3-none-any.whl

databricks workspace upload dist/databricks_ai_functions-0.1.0-py3-none-any.whl \
  /Workspace/Users/<your-username>/ai-dev-kit/wheels/databricks_ai_functions-0.1.0-py3-none-any.whl

# Upload skills too
databricks workspace import-dir databricks-skills \
  /Workspace/Users/<your-username>/ai-dev-kit/databricks-skills
```

### Step 3: Install in Notebook

```python
%pip install /Workspace/Users/<your-username>/ai-dev-kit/wheels/databricks_tools_core-0.1.0-py3-none-any.whl
%pip install /Workspace/Users/<your-username>/ai-dev-kit/wheels/databricks_ai_functions-0.1.0-py3-none-any.whl

# Restart Python to load new packages
dbutils.library.restartPython()

# Now imports work cleanly
from databricks_ai_functions.sdp.build_pipeline import build_pipeline
```

---

## Important Path Considerations

### Skills Path Resolution

The `build_pipeline.py` uses this logic to find skills:

```python
module_dir = Path(__file__).parent  # databricks_ai_functions/sdp/
project_root = module_dir.parent.parent  # ai-dev-kit root
skills_path = project_root / "databricks-skills" / "sdp"
```

**This works if**:
- The directory structure is preserved
- `databricks-skills/` is at the same level as `databricks-ai-functions/`

**Example workspace structure**:
```
/Workspace/Users/<your-username>/ai-dev-kit/
├── databricks-ai-functions/
├── databricks-tools-core/
└── databricks-skills/
```

### Authentication

The code uses default Databricks authentication, which works automatically in:
- ✅ Databricks notebooks (uses notebook context)
- ✅ Databricks jobs (uses service principal or job identity)
- ✅ Local with `~/.databrickscfg` (already working for you)

No changes needed!

---

## Testing on Databricks

### Quick Notebook Test

```python
# Cell 1: Setup
%pip install databricks-sdk>=0.31.0
import sys
sys.path.insert(0, '/Workspace/Users/<your-username>/ai-dev-kit/databricks-tools-core')
sys.path.insert(0, '/Workspace/Users/<your-username>/ai-dev-kit/databricks-ai-functions')

# Cell 2: Import
from databricks_ai_functions.sdp.build_pipeline import build_pipeline

# Cell 3: Test
result = build_pipeline(
    user_request="Create a bronze-silver-gold pipeline from samples.nyctaxi.trips",
    catalog="main",
    schema="default",
    model="databricks-gpt-5-2",
    start_run=True
)

print(result)
displayHTML(f'<a href="{result["pipeline_url"]}" target="_blank">View Pipeline</a>')
```

---

## Troubleshooting

### Issue: Module not found

**Solution**: Make sure `sys.path` includes both packages:
```python
import sys
sys.path.insert(0, '/Workspace/Users/<your-username>/ai-dev-kit/databricks-tools-core')
sys.path.insert(0, '/Workspace/Users/<your-username>/ai-dev-kit/databricks-ai-functions')
```

### Issue: Skills not loading

**Solution**: Check the skills path:
```python
from pathlib import Path
from databricks_ai_functions._shared.skills import load_skills

skills_path = "/Workspace/Users/<your-username>/ai-dev-kit/databricks-skills/sdp"
skills = load_skills(skills_path)
print(f"Loaded {len(skills)} skills")
```

### Issue: Authentication failed

**Solution**: In notebooks, authentication is automatic. If running as a job, ensure the job has proper permissions to:
- Create/update pipelines
- Access Unity Catalog
- Query serving endpoints

---

## Production Deployment

For production use, consider:

1. **Package as wheels** (Option 3) for clean dependency management
2. **Store skills in Unity Catalog Volumes** for versioning
3. **Use Databricks Secrets** for any API keys (if needed)
4. **Create a reusable job template** for running pipeline generation
5. **Add monitoring** for pipeline creation success/failure

---

## Expected Behavior

✅ **What works**:
- All core `build_pipeline()` functionality
- Schema introspection from Unity Catalog
- Skills loading and retrieval
- LLM calls to serving endpoints
- Pipeline creation and deployment
- All test files (`test_basic_pipeline.py`, etc.)

✅ **No changes needed for**:
- Authentication (uses workspace context)
- Databricks SDK calls
- File paths (if directory structure preserved)

---

## Quick Start Command

```python
# Single command to test everything
%pip install databricks-sdk>=0.31.0
import sys; sys.path.extend(['/Workspace/Users/<your-username>/ai-dev-kit/databricks-tools-core', '/Workspace/Users/<your-username>/ai-dev-kit/databricks-ai-functions'])
from databricks_ai_functions.sdp.build_pipeline import build_pipeline
result = build_pipeline("Create a bronze-silver-gold pipeline from samples.nyctaxi.trips", catalog="main", schema="default", model="databricks-gpt-5-2", start_run=True)
print(f"Pipeline: {result['pipeline_url']}")
```

Replace `<your-username>` with your actual Databricks username!

