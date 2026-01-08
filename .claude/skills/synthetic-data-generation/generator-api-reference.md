# Generator API Reference

## Overview

Reference for `databricks_mcp_core.synthetic_data_generation.generator` module functions.

This module is located at: `databricks-mcp-core/databricks_mcp_core/synthetic_data_generation/generator.py`

---

## Local Execution Functions

### execute_script()

Execute generate_data.py locally to produce parquet files.

```python
def execute_script(
    project_path: str,
    scale_factor: float = 1.0,
    python_bin: Optional[str] = None,
    timeout_sec: int = 300,
    env: Optional[Dict[str, str]] = None
) -> Dict[str, Any]
```

**Args**:

- `project_path`: Folder containing generate_data.py
- `scale_factor`: Sets SCALE_FACTOR env var for the script (default: 1.0)
- `python_bin`: Optional Python executable (defaults to sys.executable)
- `timeout_sec`: Subprocess timeout in seconds (default: 300)
- `env`: Extra environment vars to merge

**Returns**:

```python
{
  "exit_code": int,
  "stdout": str,
  "stderr": str,
  "duration_sec": float,
  "log_path": str  # Path to generation.log
}
```

**Raises**:

- `FileNotFoundError`: If generate_data.py is missing
- `subprocess.TimeoutExpired`: On timeout
- `OSError`: On spawn errors

**Output**: Writes parquet files to `<project_path>/data/<table_name>/*.parquet`

---

### upload_to_volume()

Upload generated parquet files to a Unity Catalog Volume.

```python
def upload_to_volume(
    catalog: str,
    schema: str,
    volume: str,
    local_data_dir: str,
    remote_subfolder: str = "incoming_data",
    clean: bool = True,
    max_files: Optional[int] = None
) -> Dict[str, Any]
```

**Args**:

- `catalog`: Unity Catalog name
- `schema`: Schema name
- `volume`: Volume name
- `local_data_dir`: Local path containing table folders with *.parquet
- `remote_subfolder`: Base folder under the volume (default: "incoming_data")
- `clean`: If True, removes remote_subfolder before upload (default: True)
- `max_files`: Optional cap for number of files (debugging)

**Returns**:

```python
{
  "success": bool,
  "files_uploaded": int,
  "uploaded_paths": List[str],
  "errors": List[str],
  "remote_base": str  # /Volumes/{catalog}/{schema}/{volume}/{remote_subfolder}
}
```

**Raises**:

- `DatabricksError`: If API requests fail
- `FileNotFoundError`: If local_data_dir does not exist
- `OSError`: On local I/O errors

**Remote Path**: `/Volumes/{catalog}/{schema}/{volume}/{remote_subfolder}/...`

---

### generate_and_upload()

One-shot orchestration: execute local generate_data.py then upload to Volume.

```python
def generate_and_upload(
    project_path: str,
    catalog: str,
    schema: str,
    volume: str,
    scale_factor: float = 1.0,
    remote_subfolder: str = "incoming_data",
    clean: bool = True,
    python_bin: Optional[str] = None,
    timeout_sec: int = 300
) -> Dict[str, Any]
```

**Args**:

- `project_path`: Project directory containing generate_data.py
- `catalog`: Unity Catalog name
- `schema`: Schema name
- `volume`: Volume name
- `scale_factor`: Sets SCALE_FACTOR for generation run (default: 1.0)
- `remote_subfolder`: Base folder under the volume (default: "incoming_data")
- `clean`: If True, remote_subfolder is cleaned before upload (default: True)
- `python_bin`: Optional Python executable
- `timeout_sec`: Subprocess timeout (default: 300)

**Returns**:

```python
{
  "generation": {...execute_script result...},
  "upload": {...upload_to_volume result...} or None if generation failed
}
```

**Raises**: Exceptions from execute_script or upload_to_volume

**Recommended**: Use this function for local generation workflows instead of calling execute_script and upload_to_volume separately.

---

## Cluster Execution Functions

### execute_script_on_cluster()

Execute generate_data.py on Databricks cluster, writing to Volume.

```python
def execute_script_on_cluster(
    cluster_id: str,
    workspace_path: str,
    volume_output_path: str,
    scale_factor: float = 1.0,
    timeout_sec: int = 600
) -> Dict[str, Any]
```

**Args**:

- `cluster_id`: Databricks cluster ID
- `workspace_path`: Workspace path to generate_data.py (e.g., "/Workspace/Users/me/generate_data.py")
- `volume_output_path`: Volume path for output (e.g., "/Volumes/catalog/schema/volume/incoming_data")
- `scale_factor`: Sets SCALE_FACTOR env var for the script (default: 1.0)
- `timeout_sec`: Execution timeout in seconds (default: 600)

**Returns**:

```python
{
  "success": bool,
  "output": str,
  "error": Optional[str],
  "duration_sec": float
}
```

**Raises**: `DatabricksError`: If API requests fail

**Implementation Notes**:

- Reads script from workspace using `workspace_files.read_file()`
- Wraps script to set `SCALE_FACTOR` and `OUTPUT_PATH` environment variables
- Executes on cluster using `execution.execute_databricks_command()`

---

### generate_and_upload_on_cluster()

One-shot orchestration: execute generate_data.py on cluster to Volume.

```python
def generate_and_upload_on_cluster(
    cluster_id: str,
    workspace_path: str,
    catalog: str,
    schema: str,
    volume: str,
    scale_factor: float = 1.0,
    remote_subfolder: str = "incoming_data",
    clean: bool = True,
    timeout_sec: int = 600
) -> Dict[str, Any]
```

**Args**:

- `cluster_id`: Databricks cluster ID
- `workspace_path`: Workspace path to generate_data.py
- `catalog`: Unity Catalog name
- `schema`: Schema name
- `volume`: Volume name
- `scale_factor`: Sets SCALE_FACTOR for generation run (default: 1.0)
- `remote_subfolder`: Base folder under the volume (default: "incoming_data")
- `clean`: If True, remote_subfolder is cleaned before execution (default: True)
- `timeout_sec`: Execution timeout (default: 600)

**Returns**:

```python
{
  "success": bool,
  "output": str,
  "volume_path": str,
  "error": Optional[str],
  "duration_sec": float
}
```

**Raises**: `DatabricksError`: If API requests fail

**Recommended**: Use this function for cluster-based generation workflows instead of calling execute_script_on_cluster separately.

---

## Script Requirements

Your `generate_data.py` script must:

1. **Read environment variables**:

   ```python
   import os
   SCALE_FACTOR = float(os.environ.get('SCALE_FACTOR', 1.0))
   OUTPUT_PATH = os.environ.get('OUTPUT_PATH', './data')
   ```

2. **Write parquet files** to `{OUTPUT_PATH}/{table_name}/`:

   ```python
   from pathlib import Path
   output_dir = Path(OUTPUT_PATH) / "customers"
   output_dir.mkdir(parents=True, exist_ok=True)
   df.to_parquet(output_dir / "data.parquet")
   ```

3. **Handle errors gracefully**: Print errors to stderr for debugging

---

## Usage Examples

### Local Generation + Upload

```python
from databricks_mcp_core.synthetic_data_generation import generator

result = generator.generate_and_upload(
    project_path="./my_project",
    catalog="dev",
    schema="synthetic",
    volume="data",
    scale_factor=2.0,
    remote_subfolder="test_run_001",
    clean=True,
    timeout_sec=300
)

if result["generation"]["exit_code"] == 0:
    print(f"✓ Generated data successfully")
    print(f"✓ Uploaded {result['upload']['files_uploaded']} files")
    print(f"  Location: {result['upload']['remote_base']}")
else:
    print(f"✗ Generation failed: {result['generation']['stderr']}")
```

### Cluster-Based Generation

```python
from databricks_mcp_core.synthetic_data_generation import generator

result = generator.generate_and_upload_on_cluster(
    cluster_id="0123-456789-abcdef01",
    workspace_path="/Workspace/Users/me/projects/generate_data.py",
    catalog="prod",
    schema="synthetic",
    volume="data",
    scale_factor=10.0,
    remote_subfolder="production_v1",
    clean=True,
    timeout_sec=900
)

if result["success"]:
    print(f"✓ Generated data on cluster")
    print(f"  Location: {result['volume_path']}")
else:
    print(f"✗ Generation failed: {result['error']}")
```

---

## Error Handling

### Common Errors

**FileNotFoundError**: `generate_data.py` not found in project_path

- Check project_path is correct
- Ensure generate_data.py exists

**subprocess.TimeoutExpired**: Script took too long

- Increase timeout_sec
- Reduce scale_factor
- Optimize script performance

**DatabricksError**: API call failed

- Check cluster is running
- Verify Unity Catalog permissions
- Check network connectivity

**Upload errors**: File upload failed

- Verify Volume exists: `/Volumes/{catalog}/{schema}/{volume}`
- Check write permissions on Volume
- Ensure enough space in Volume

---

## Best Practices

1. **Use orchestration functions**: Prefer `generate_and_upload()` or `generate_and_upload_on_cluster()` over individual functions

2. **Handle failures gracefully**: Check `exit_code` or `success` before assuming operation succeeded

3. **Clean between runs**: Use `clean=True` (default) to avoid data conflicts

4. **Start small**: Use `scale_factor=0.1` for testing, then scale up

5. **Monitor logs**: Check `generation.log` (local) or execution output (cluster) for debugging

6. **Use appropriate timeouts**: Local scripts may be slower than cluster execution

7. **Test locally first**: Debug scripts locally before running on cluster
