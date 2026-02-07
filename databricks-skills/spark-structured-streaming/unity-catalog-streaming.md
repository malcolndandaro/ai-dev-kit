---
name: unity-catalog-streaming
description: Integrate Unity Catalog with Spark Structured Streaming for governance, access control, and lineage tracking. Use when setting up production streaming pipelines, implementing row/column-level security, tracking data lineage, or managing cross-workspace data sharing.
---

# Unity Catalog Streaming

Integrate Unity Catalog with Spark Structured Streaming for unified governance, access control, and automatic lineage tracking.

## Quick Start

```python
# Store checkpoint in Unity Catalog volume
checkpoint_path = "/Volumes/catalog/volume/checkpoints/stream_name"

# Write to Unity Catalog table
(df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .start("catalog.schema.table")  # Unity Catalog table
)
```

## Unity Catalog Benefits

- **Unified Access Control**: Row/column-level security on streaming tables
- **Lineage Tracking**: Automatic data lineage for streams
- **Audit Logging**: Complete audit trail
- **Cross-Workspace Sharing**: Share streams across workspaces

## Checkpoint in UC Volumes

### Store Checkpoints in UC Volumes

```python
# Store checkpoint in Unity Catalog volume (S3/ADLS-backed)
checkpoint_path = "/Volumes/catalog/volume/checkpoints/stream_name"

(df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .start("catalog.schema.table")
)

# Benefits:
# - Persistent storage (not DBFS)
# - Cross-workspace access
# - Governance and access control
```

### Checkpoint Volume Permissions

```sql
-- Grant permissions on checkpoint volume
GRANT READ VOLUME ON catalog.volume TO user streaming_job;
GRANT WRITE VOLUME ON catalog.volume TO user streaming_job;

-- Or use service principal
-- Configure in Unity Catalog settings
```

## Streaming Table Access Control

### Grant Table Permissions

```sql
-- Grant read access to streaming table
GRANT SELECT ON catalog.schema.stream_table TO group analysts;

-- Grant write access for streaming job
GRANT MODIFY ON catalog.schema.target_table TO user streaming_job;

-- Grant use catalog/schema
GRANT USE CATALOG ON catalog TO group analysts;
GRANT USE SCHEMA ON catalog.schema TO group analysts;
```

### Row-Level Security

```sql
-- Create row filter
CREATE ROW FILTER catalog.schema.filter_name
ON catalog.schema.stream_table
AS (region = current_user());

-- Apply to table
ALTER TABLE catalog.schema.stream_table
SET ROW FILTER catalog.schema.filter_name;

-- Row-level security applies to streaming reads
-- Users only see rows matching their filter
```

### Column-Level Security

```sql
-- Grant access to specific columns
GRANT SELECT (id, name, timestamp) ON catalog.schema.stream_table 
TO group analysts;

-- Sensitive columns are hidden
-- Streaming reads respect column permissions
```

## Managed vs External Tables

| Type | Checkpoint | Data | Use Case |
|------|------------|------|----------|
| **Managed** | UC Volume | UC Storage | Default - Unity Catalog manages storage |
| **External** | UC Volume | External | Existing data in external location |

### Managed Table

```python
# Managed table - Unity Catalog manages storage
(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/catalog/checkpoints/stream")
    .start("catalog.schema.managed_table")  # Managed table
)
```

### External Table

```python
# External table - data in external location
spark.sql("""
    CREATE TABLE catalog.schema.external_table (
        id STRING,
        timestamp TIMESTAMP,
        data STRING
    ) USING DELTA
    LOCATION '/external/path/to/data'
""")

(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/catalog/checkpoints/stream")
    .start("catalog.schema.external_table")
)
```

## Lineage Tracking

### Automatic Lineage Capture

Unity Catalog automatically captures:
- Source → Target relationships
- Column-level lineage
- Transformation logic

```python
# Lineage automatically captured
source_df = spark.readStream.table("catalog.schema.source_table")
target_df = source_df.transform(...)
target_df.writeStream.start("catalog.schema.target_table")

# View lineage in Catalog Explorer
# Or query via Unity Catalog API
```

### Query Lineage

```python
# Query lineage via Unity Catalog API
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
lineage = w.catalogs.get_lineage("catalog.schema.table")

# Access upstream/downstream tables
for upstream in lineage.upstreams:
    print(f"Upstream: {upstream.table_name}")

for downstream in lineage.downstreams:
    print(f"Downstream: {downstream.table_name}")
```

## Common Patterns

### Pattern 1: Production Streaming with UC

```python
# Production pattern with Unity Catalog
checkpoint_path = "/Volumes/prod/checkpoints/orders_stream"

(df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .start("prod.analytics.orders")  # Unity Catalog table
)

# Benefits:
# - Governance and access control
# - Automatic lineage tracking
# - Audit logging
# - Cross-workspace sharing
```

### Pattern 2: Cross-Workspace Sharing

```python
# Share streaming table across workspaces
# Configure in Unity Catalog:
# 1. Create share
# 2. Add table to share
# 3. Grant access to external workspace

# In consuming workspace:
# Read from shared table
shared_df = spark.readStream.table("shared_catalog.schema.table")
```

### Pattern 3: Service Principal Authentication

```python
# Use service principal for production jobs
# Configure in Unity Catalog:
# 1. Create service principal
# 2. Grant permissions
# 3. Use in job configuration

# Job runs with service principal credentials
# No user credentials needed
```

## Best Practices

### Checkpoint Management

```python
# Use UC volumes for checkpoints (not DBFS)
checkpoint_path = "/Volumes/catalog/checkpoints/{table_name}"

# Benefits:
# - Persistent storage
# - Cross-workspace access
# - Governance and access control
```

### Permission Management

```sql
-- Use groups for permissions (not individual users)
GRANT SELECT ON catalog.schema.table TO group analysts;
GRANT MODIFY ON catalog.schema.table TO group data_engineers;

-- Use service principals for production jobs
GRANT MODIFY ON catalog.schema.table TO service_principal streaming_job;
```

### Lineage Utilization

```python
# Leverage lineage for impact analysis
# Before dropping table:
# 1. Query lineage to find downstream dependencies
# 2. Notify downstream consumers
# 3. Plan migration strategy
```

## Production Checklist

- [ ] Checkpoints stored in UC volumes (not DBFS)
- [ ] Appropriate permissions configured
- [ ] Service principals used for production jobs
- [ ] Row/column-level security configured (if needed)
- [ ] Lineage tracking verified
- [ ] Audit logging enabled
- [ ] Cross-workspace sharing configured (if needed)

## Related Skills

- `checkpoint-best-practices` - Checkpoint configuration
- `kafka-to-delta` - Ingestion patterns with UC
- `cost-tuning` - Cost optimization with UC
