---
name: synthetic-data-generation
description: "Generate realistic synthetic data using Faker for Databricks testing, development, and demos. Use when: (1) Creating test datasets, (2) Generating sample data for pipelines, (3) Building demo environments, (4) Prototyping with realistic data. Supports local generation with upload to Unity Catalog Volumes or direct cluster-based generation."
---

# Synthetic Data Generation

## Overview

Generate realistic synthetic data using Python Faker library for Databricks development, testing, and demos.

**When to use this skill**:

- Creating test datasets for pipeline development
- Generating sample data for SDP/Delta Live Tables
- Building demo environments with realistic data
- Prototyping data models quickly

## Quick Start

### Step 1: Use the template

Start with the provided Faker template: **[generate_data_template.py](generate_data_template.py)**

This template includes:

- Faker-based data generation for common scenarios
- SCALE_FACTOR support for controlling data volume
- Parquet output format
- Sample customer and order tables

### Step 2: Customize for your use case

Edit `generate_data.py` to:

- Add/remove tables
- Customize column definitions
- Adjust data distributions
- Implement specific business logic

See **[faker-patterns.md](faker-patterns.md)** for common Faker patterns and examples.

### Step 3: Execute

Choose execution method:

- **Local + Upload**: Generate parquet files locally, upload to Volume
- **Cluster-based**: Execute directly on Databricks cluster, write to Volume

## Reference Documentation

- **[generate_data_template.py](generate_data_template.py)** - Starter template with Faker examples
- **[faker-patterns.md](faker-patterns.md)** - Common Faker patterns and examples (TODO: Add your patterns)
- **[generator-api-reference.md](generator-api-reference.md)** - Python API reference for generator.py script

## Core Workflow

### Local Generation + Upload

```python
from databricks_mcp_core.synthetic_data_generation import generator

# 1. Copy template from this skill to your project
# Copy .claude/skills/synthetic-data-generation/generate_data_template.py
# to your project as generate_data.py

# 2. Customize generate_data.py for your use case
# Edit tables, columns, logic as needed

# 3. Execute locally and upload to Volume
result = generator.generate_and_upload(
    project_path="./my_data_project",
    catalog="my_catalog",
    schema="my_schema",
    volume="my_volume",
    scale_factor=1.0,
    remote_subfolder="incoming_data",
    clean=True
)

# Check results
if result["generation"]["exit_code"] == 0:
    print(f"✓ Uploaded {result['upload']['files_uploaded']} files")
    print(f"  Location: {result['upload']['remote_base']}")
else:
    print(f"✗ Generation failed: {result['generation']['stderr']}")
```

**Output**: Parquet files in `/Volumes/{catalog}/{schema}/{volume}/incoming_data/`

**Script Requirements**:

- Read `SCALE_FACTOR` environment variable
- Read `OUTPUT_PATH` environment variable
- Write parquet files to `{OUTPUT_PATH}/{table_name}/`

### Cluster-Based Generation

```python
# 1. Upload generate_data.py to Workspace
# (Use Workspace Files API or UI)

# 2. Execute on cluster
result = generator.generate_and_upload_on_cluster(
    cluster_id="0123-456789-abcdef01",
    workspace_path="/Workspace/Users/me/generate_data.py",
    catalog="my_catalog",
    schema="my_schema",
    volume="my_volume",
    scale_factor=1.0,
    remote_subfolder="incoming_data"
)
```

**Output**: Parquet files written directly to Volume by cluster

## Key Parameters

### scale_factor

Controls data volume (sets `SCALE_FACTOR` environment variable):

- `1.0`: Default, baseline volume
- `10.0`: 10x more data
- `0.1`: 10% of baseline

Your `generate_data.py` script should read `SCALE_FACTOR` and multiply row counts accordingly.

### remote_subfolder

Organizes data under Volume:

- Default: `"incoming_data"`
- Pattern: `/Volumes/{catalog}/{schema}/{volume}/{remote_subfolder}/`

### clean

- `True`: Removes `remote_subfolder` before upload (default)
- `False`: Appends to existing data

## Integration with SDP Pipelines

Use generated data as source for Lakeflow Spark Declarative Pipelines:

```sql
-- Bronze layer ingestion from Volume
CREATE OR REPLACE STREAMING TABLE catalog.schema.bronze_stories
CLUSTER BY (story_id)
AS
SELECT *
FROM read_files(
  '/Volumes/catalog/schema/volume/incoming_data/stories/',
  format => 'parquet'
);
```

See **sdp-writer** skill for complete SDP pipeline patterns.

## Common Patterns

### Pattern 1: Test Data for Development

Generate small dataset for local testing:

```python
result = generator.generate_and_upload(
    project_path="./test_data",
    catalog="dev",
    schema="test",
    volume="synthetic",
    scale_factor=0.1,  # Small dataset
    remote_subfolder="test_run_001"
)
```

### Pattern 2: Demo Environment Setup

Generate realistic data for demos:

```python
result = generator.generate_and_upload(
    project_path="./demo_data",
    catalog="demo",
    schema="retail",
    volume="data",
    scale_factor=5.0,  # Larger dataset for realistic demos
    remote_subfolder="v1"
)
```

### Pattern 3: Iterative Development

Generate multiple versions while developing:

```python
# Version 1
generator.generate_and_upload(..., remote_subfolder="v1")

# Version 2 (with schema changes)
generator.generate_and_upload(..., remote_subfolder="v2")

# Keep previous versions for comparison
```

## Core Functions Reference

See **[generator-api-reference.md](generator-api-reference.md)** for complete API reference.

**Local Execution**:

- `execute_script(project_path, scale_factor)` - Run generate_data.py locally
- `upload_to_volume(catalog, schema, volume, local_data_dir)` - Upload parquet files
- `generate_and_upload(...)` - One-shot orchestration (recommended)

**Cluster Execution**:

- `execute_script_on_cluster(cluster_id, workspace_path, volume_output_path)` - Run on cluster
- `generate_and_upload_on_cluster(...)` - One-shot orchestration (recommended)

## Requirements

### Local Execution

- Python 3.8+
- `faker` library: `pip install faker`
- `pandas` library: `pip install pandas`
- `pyarrow` library: `pip install pyarrow` (for parquet support)
- Databricks SDK: `pip install databricks-sdk`

### Cluster Execution

- Databricks cluster with required libraries installed (faker, pandas, pyarrow)
- Script uploaded to Workspace
- Cluster must have write access to target Volume

## Troubleshooting

### Issue: Script execution fails with import errors

**Cause**: Missing faker, pandas, or pyarrow
**Solution**: Install dependencies: `pip install faker pandas pyarrow databricks-sdk`

### Issue: Upload fails with permission error

**Cause**: No write access to Volume
**Solution**: Verify Unity Catalog permissions for catalog, schema, and volume

### Issue: Generated files are empty or missing

**Cause**: Script error during execution
**Solution**: Check `generation.log` in project directory for errors

### Issue: Cluster execution times out

**Cause**: Scale factor too large or cluster too small
**Solution**: Reduce scale_factor or use larger cluster

## Resources

- [Faker Documentation](https://faker.readthedocs.io/)
- [Unity Catalog Volumes](https://docs.databricks.com/en/volumes/index.html)
- [Databricks SDK for Python](https://docs.databricks.com/en/dev-tools/sdk-python.html)
