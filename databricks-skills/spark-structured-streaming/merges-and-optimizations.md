---
name: "streaming-merges-optimizations"
description: "Optimize streaming merge operations using Liquid Clustering, Deletion Vectors, and Row-Level Concurrency."
tags: ["spark-streaming", "merge", "liquid", "performance"]
---

# Streaming Merges Optimizations

## Liquid Clustering + DV + RLC

### Problem

Traditional streaming merges require pausing to run OPTIMIZE, causing P99 latency spikes.

### Solution

```sql
-- Enable modern Delta features
ALTER TABLE target_table SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = true,
  'delta.enableRowLevelConcurrency' = true,
  'delta.liquid.clustering' = true
);
```

### Benefits

| Feature | Benefit |
|---------|---------|
| **Deletion Vectors** | Soft deletes without file rewrite |
| **Row-Level Concurrency** | Concurrent updates to different rows |
| **Liquid Clustering** | Automatic optimization without pauses |

### Result

- Eliminates optimize pauses
- Lower, more consistent P99 latency
- Simpler code (no batch counting for optimize)

### Code Pattern

```python
# Before: Had to pause for optimize
# After: Stream continues, optimize runs in parallel

def upsert_batch(df, batch_id):
    df.createOrReplaceTempView("updates")
    spark.sql("""
        MERGE INTO target_table t
        USING updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    # No optimize needed here!

# Optimize runs separately via Predictive Optimization
```