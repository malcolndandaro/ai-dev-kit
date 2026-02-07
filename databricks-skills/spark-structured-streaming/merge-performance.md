---
name: merge-performance-tuning
description: Optimize Delta MERGE operations for streaming workloads. Use when improving merge performance, eliminating optimize pauses, reducing P99 latency, enabling concurrent merges, or configuring Liquid Clustering with Deletion Vectors and Row-Level Concurrency.
---

# Merge Performance Tuning

Optimize Delta MERGE operations for streaming workloads. Enable Liquid Clustering + Deletion Vectors + Row-Level Concurrency for conflict-free concurrent merges and lower latency.

## Quick Start

```python
from delta.tables import DeltaTable

# Enable modern Delta features
spark.sql("""
    ALTER TABLE target_table SET TBLPROPERTIES (
        'delta.enableDeletionVectors' = true,
        'delta.enableRowLevelConcurrency' = true,
        'delta.liquid.clustering' = true
    )
""")

# MERGE in ForEachBatch
def upsert_batch(batch_df, batch_id):
    batch_df.createOrReplaceTempView("updates")
    spark.sql("""
        MERGE INTO target_table t
        USING updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    # No optimize needed - Liquid Clustering handles it automatically

stream.writeStream \
    .foreachBatch(upsert_batch) \
    .option("checkpointLocation", "/checkpoints/merge") \
    .start()
```

## Enable Liquid Clustering + DV + RLC

Enable modern Delta features for optimal merge performance:

```sql
-- Enable for target table
ALTER TABLE target_table SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = true,
    'delta.enableRowLevelConcurrency' = true,
    'delta.liquid.clustering' = true
);
```

**Benefits:**
- **Deletion Vectors**: Soft deletes without file rewrite
- **Row-Level Concurrency**: Concurrent updates to different rows
- **Liquid Clustering**: Automatic optimization without pauses
- **Result**: Eliminates optimize pauses, lower P99 latency, simpler code

## Optimization Strategies

### Strategy 1: Liquid Clustering + DV + RLC

Enable for conflict-free concurrent merges:

```sql
-- Create table with Liquid Clustering
CREATE TABLE target_table (
    id STRING,
    name STRING,
    updated_at TIMESTAMP
) USING DELTA
CLUSTER BY (id)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = true,
    'delta.enableRowLevelConcurrency' = true
);

-- Or alter existing table
ALTER TABLE target_table SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = true,
    'delta.enableRowLevelConcurrency' = true,
    'delta.liquid.clustering' = true
);
ALTER TABLE target_table CLUSTER BY (id);
```

### Strategy 2: File Size Tuning

Configure target file size for optimal merge performance:

```sql
-- Target file size for optimal merge
ALTER TABLE target_table SET TBLPROPERTIES (
    'delta.targetFileSize' = '128mb'
);

-- Enable auto-optimize
ALTER TABLE target_table SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = true,
    'delta.autoOptimize.autoCompact' = true
);
```

### Strategy 3: Z-Ordering on Match Key

Co-locate data by merge key for faster lookups:

```sql
-- Z-Order on merge key
OPTIMIZE target_table ZORDER BY (id);

-- Run periodically or via Predictive Optimization
-- 5-10x faster for targeted lookups
```

### Strategy 4: Partition Pruning

Include partition columns in merge condition:

```python
# Include partition column in merge condition
spark.sql("""
    MERGE INTO target t
    USING source s 
    ON t.id = s.id AND t.date = s.date  -- partition column
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# Skips irrelevant partitions
# Faster merge execution
```

### Strategy 5: Batch Size Control

Control microbatch size for optimal merge performance:

```python
# Control microbatch size
df = (spark.readStream
    .format("kafka")
    .option("maxOffsetsPerTrigger", 10000)  # Adjust based on merge performance
    .load()
)

# Or for Delta source
df = (spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 500)
    .load()
)
```

## Performance Comparison

| Optimization | Impact | Use Case |
|--------------|--------|----------|
| **Liquid + DV + RLC** | Eliminates conflicts, lower P99 | Concurrent merges, no optimize pauses |
| **Z-Ordering** | 5-10x faster for targeted lookups | High-cardinality merge keys |
| **Right file size** | Better parallelism | Large tables |
| **Partition pruning** | Skips irrelevant partitions | Partitioned tables |

## Common Patterns

### Pattern 1: Basic MERGE with Optimization

```python
from delta.tables import DeltaTable

def optimized_merge(batch_df, batch_id):
    """MERGE with optimized table"""
    batch_df.createOrReplaceTempView("updates")
    
    spark.sql("""
        MERGE INTO target_table t
        USING updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    # No optimize needed - Liquid Clustering handles it

stream.writeStream \
    .foreachBatch(optimized_merge) \
    .option("checkpointLocation", "/checkpoints/merge") \
    .start()
```

### Pattern 2: MERGE with Partition Pruning

```python
def partition_pruned_merge(batch_df, batch_id):
    """MERGE with partition column in condition"""
    batch_df.createOrReplaceTempView("updates")
    
    spark.sql("""
        MERGE INTO target_table t
        USING updates s 
        ON t.id = s.id AND t.date = s.date  -- partition column
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

stream.writeStream \
    .foreachBatch(partition_pruned_merge) \
    .option("checkpointLocation", "/checkpoints/merge") \
    .start()
```

### Pattern 3: MERGE with Conditional Updates

```python
def conditional_merge(batch_df, batch_id):
    """MERGE with conditional update logic"""
    batch_df.createOrReplaceTempView("updates")
    
    spark.sql("""
        MERGE INTO target_table t
        USING updates s ON t.id = s.id
        WHEN MATCHED AND s.updated_at > t.updated_at THEN 
            UPDATE SET *
        WHEN NOT MATCHED THEN 
            INSERT *
    """)

stream.writeStream \
    .foreachBatch(conditional_merge) \
    .option("checkpointLocation", "/checkpoints/merge") \
    .start()
```

## Monitoring

### Check Merge Performance in Spark UI

- **Look for broadcast hash joins**: Indicates efficient merge
- **Check file pruning effectiveness**: Verify partition pruning
- **Monitor task skew**: Ensure even distribution
- **Check merge duration**: Track P50, P95, P99 latency

### Programmatic Monitoring

```python
# Track merge performance
import time

def monitored_merge(batch_df, batch_id):
    start_time = time.time()
    
    batch_df.createOrReplaceTempView("updates")
    spark.sql("""
        MERGE INTO target_table t
        USING updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    duration = time.time() - start_time
    print(f"Merge duration: {duration:.2f}s")
    
    # Alert if duration exceeds threshold
    if duration > 30:  # 30 seconds threshold
        print(f"WARNING: Merge duration {duration:.2f}s exceeds threshold")

stream.writeStream \
    .foreachBatch(monitored_merge) \
    .option("checkpointLocation", "/checkpoints/merge") \
    .start()
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **High P99 latency** | OPTIMIZE pauses | Enable Liquid Clustering (no pauses) |
| **Merge conflicts** | Concurrent updates to same rows | Enable Row-Level Concurrency |
| **Slow merges** | Large files, no optimization | Enable Liquid Clustering; Z-Order on merge key |
| **File pruning ineffective** | Partition column not in merge condition | Include partition column in ON clause |
| **Task skew** | Uneven data distribution | Check data distribution; consider salting |

## Production Best Practices

### Table Configuration

```sql
-- Enable all optimizations
ALTER TABLE target_table SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = true,
    'delta.enableRowLevelConcurrency' = true,
    'delta.liquid.clustering' = true,
    'delta.targetFileSize' = '128mb',
    'delta.autoOptimize.optimizeWrite' = true,
    'delta.autoOptimize.autoCompact' = true
);

-- Z-Order on merge key
OPTIMIZE target_table ZORDER BY (merge_key);
```

### MERGE Pattern

```python
# Use ForEachBatch for MERGE operations
def upsert_batch(batch_df, batch_id):
    batch_df.createOrReplaceTempView("updates")
    
    # Include partition columns in merge condition if partitioned
    spark.sql("""
        MERGE INTO target_table t
        USING updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    # No optimize needed - Liquid Clustering handles it

stream.writeStream \
    .foreachBatch(upsert_batch) \
    .option("checkpointLocation", "/checkpoints/merge") \
    .start()
```

## Production Checklist

- [ ] Liquid Clustering + DV + RLC enabled
- [ ] Z-Ordering configured on merge key
- [ ] File size tuned (128MB target)
- [ ] Partition pruning enabled (if partitioned)
- [ ] Batch size optimized
- [ ] Performance monitoring configured
- [ ] Merge duration tracked (P50, P95, P99)

## Related Skills

- `merge-multiple-tables-parallel` - Parallel MERGE operations
- `write-multiple-tables` - Multi-sink patterns
- `partitioning-strategy` - Partition optimization for merges
