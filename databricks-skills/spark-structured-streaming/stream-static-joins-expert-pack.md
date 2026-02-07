---
name: "stream-static-joins-expert-pack"
description: "Expert guide to stream-static joins: Delta enrichment, left join patterns, broadcast optimization, and production monitoring for real-time data pipelines."
tags: ["spark-streaming", "stream-static-join", "delta", "enrichment", "expert"]
---

# Stream-Static Joins Expert Pack

## Overview

Stream-static joins are the workhorse of real-time data enrichment — joining fast-moving streaming data with slowly-changing reference data stored in Delta tables.

**Key Insight**: Delta's versioning ensures each microbatch gets the latest dimension data automatically.

## Quick Start

```python
# Streaming source (IoT events)
stream = spark.readStream.format("kafka").load()

# Static Delta dimension table
dim = spark.table("device_dimensions")  # Refreshes each microbatch

# Enrich streaming data
enriched = stream.join(dim, "device_id", "left")

enriched.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/enriched") \
    .start("/delta/enriched_events")
```

## Core Concepts

### Why Delta Matters

```python
# Delta tables: Version checked every microbatch
# Non-Delta: Read once at startup (truly static)

# Works with version checking:
dim = spark.table("device_dimensions")  # Delta format

# Static read (no refresh):
dim = spark.read.parquet("/path/to/devices")  # Parquet
```

### Join Semantics

| Join Type | Behavior | Production Use |
|-----------|----------|----------------|
| **Left** | Preserves all stream events | ✅ Recommended |
| **Inner** | Drops unmatched events | ⚠️ Risk of data loss |
| **Right** | Preserves all dimension rows | Rarely used |
| **Full** | Preserves both sides | Rarely used |

## Common Patterns

### Pattern 1: Left Join (Production Default)

```python
# RECOMMENDED: Left join prevents data loss
enriched = stream.join(
    dim,
    "device_id",
    "left"
)

# Why left join?
# - New devices may not exist in dimension yet
# - Dimension updates may lag
# - Events are never dropped
```

### Pattern 2: Broadcast Hash Join

```python
# Spark automatically broadcasts small tables
# Look for "BroadcastHashJoin" in query plan

# Optimize by reducing dimension size:
small_dim = dim.select("device_id", "device_type", "location")

# Or filter active records:
active_dim = dim.filter(col("status") == "active")

# Force broadcast if needed:
from pyspark.sql.functions import broadcast
enriched = stream.join(broadcast(small_dim), "device_id")
```

### Pattern 3: Multi-Table Enrichment

```python
# Chain multiple dimension joins
devices = spark.table("devices")
locations = spark.table("locations")
categories = spark.table("categories")

enriched = (stream
    .join(devices, "device_id", "left")
    .join(locations, "location_id", "left")
    .join(categories, "category_id", "left")
)

# Each join is stateless and refreshes independently
```

### Pattern 4: Time-Travel Dimension

```python
# Join with dimension as-of event time
from delta import DeltaTable

# Get version at event timestamp
version = DeltaTable.forName(spark, "dimensions") \
    .history() \
    .filter(col("timestamp") <= event_ts) \
    .orderBy(desc("timestamp")) \
    .select("version").first()[0]

# Read specific version
dim_at_time = spark.read \
    .format("delta") \
    .option("versionAsOf", version) \
    .table("dimensions")
```

## Production Patterns

### Pattern 5: Audit Dimension Freshness

```python
# Track how fresh dimension data is
enriched = stream.join(
    dim,
    "device_id",
    "left"
).withColumn(
    "dim_lag_seconds",
    unix_timestamp(col("event_time")) - 
    unix_timestamp(col("dim_updated_at"))
)

# Monitor: Alert if dim_lag_seconds > threshold
```

### Pattern 6: Backfill Missing Dimensions

```python
# Daily job to fix null dimensions from left join
spark.sql("""
    MERGE INTO enriched_events target
    USING device_dimensions source
    ON target.device_id = source.device_id
      AND target.device_type IS NULL
    WHEN MATCHED THEN UPDATE SET *
""")
```

### Pattern 7: Dimension Change Detection

```python
# Stream that reacts to dimension changes
dim_changes = spark.readStream \
    .format("delta") \
    .table("device_dimensions") \
    .writeStream \
    .foreachBatch(update_reference_cache) \
    .start()
```

## Performance Optimization

### Checklist

- [ ] Dimension table < 100MB for broadcast (or increase threshold)
- [ ] Select only needed columns before join
- [ ] Filter dimension to active records only
- [ ] Verify "BroadcastHashJoin" in query plan
- [ ] Partition size 100-200MB in memory
- [ ] Use same region for compute and storage

### Configuration

```python
# Increase broadcast threshold if needed
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1g")

# Control partition size
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

## Monitoring

### Key Metrics

```python
# Null rate (left join quality)
spark.sql("""
    SELECT 
        date_trunc('hour', timestamp) as hour,
        count(*) as total,
        count(device_type) as matched,
        (count(*) - count(device_type)) / count(*) as null_rate
    FROM enriched_events
    GROUP BY 1
""")

# Batch duration vs trigger interval
# Should have headroom: trigger(30s) vs batch_duration(10s)
```

### Spark UI Checks

- **Streaming Tab**: Input rate vs processing rate
- **SQL Tab**: Look for BroadcastHashJoin
- **Jobs Tab**: Check for shuffle operations

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **Data loss** | Inner join | Switch to left join |
| **Slow joins** | Shuffle join | Reduce dimension size; force broadcast |
| **Stale data** | Non-Delta format | Convert to Delta |
| **Memory issues** | Large dimension | Filter before join |
| **Skewed joins** | Hot keys | Salt the join key |

## Related Skills

- `spark-streaming-master-class-kafka-to-delta` — End-to-end Kafka ingestion
- `mastering-checkpoints-in-spark-streaming` — Checkpoint patterns
- `parallel-writes-foreachbatch` — ForEachBatch optimization
