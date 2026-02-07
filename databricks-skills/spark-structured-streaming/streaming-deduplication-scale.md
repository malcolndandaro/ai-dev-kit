---
name: "streaming-deduplication-scale"
description: "Strategies for deduplicating streaming data at scale: comparing dropDuplicates state store vs Delta merge approaches for different volume scenarios."
author: "Databricks Expert"
date: "2025-02-07"
tags: ["spark-streaming", "deduplication", "exactly-once", "state-management", "delta-merge", "scale"]
---

# Streaming Deduplication at Scale

## Overview

Deduplication in streaming ensures exactly-once processing semantics. Choose the right approach based on data volume and latency requirements.

**Two Primary Approaches:**
1. **`dropDuplicates()`** — State store-based (Spark native)
2. **Delta `MERGE`** — Table-based lookup (scalable)

**Decision Matrix:**

| Approach | Volume | Latency | State Growth | Use Case |
|----------|--------|---------|--------------|----------|
| `dropDuplicates()` | < 100M unique keys | Low | Unbounded (with watermark) | Simple dedup |
| Delta `MERGE` | > 100M unique keys | Higher | Bounded (table storage) | High-scale dedup |

## Quick Start

### Approach 1: dropDuplicates() (State Store)

```python
# Simple deduplication using state store
deduped = (df
    .withWatermark("timestamp", "10 minutes")  # Required for state cleanup
    .dropDuplicates(["event_id", "user_id"])   # Composite key
)

(deduped.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/dedup-simple")
    .start("/delta/deduplicated_events")
)
```

### Approach 2: Delta MERGE (Table Lookup)

```python
# High-scale deduplication using Delta table
from delta.tables import DeltaTable

def dedup_with_merge(batch_df, batch_id):
    """Deduplicate using Delta table as lookup"""
    
    target_table = DeltaTable.forPath(spark, "/delta/deduplicated_events")
    
    # Merge: Insert only if event_id doesn't exist
    target_table.alias("target").merge(
        batch_df.alias("source"),
        "target.event_id = source.event_id"
    ).whenNotMatchedInsertAll().execute()

(df.writeStream
    .foreachBatch(dedup_with_merge)
    .option("checkpointLocation", "/checkpoints/dedup-merge")
    .start()
)
```

## Approach 1: dropDuplicates Deep Dive

### How It Works

```python
# State store maintains seen keys
# Key structure: (event_id, user_id) + timestamp for watermark

# State is partitioned by dedup key hash
# Each executor tracks its partition of keys

# Watermark triggers cleanup:
# - Keys older than watermark are dropped
# - Reduces state store size
```

### Configuration

```python
# State store provider (RocksDB recommended for scale)
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateProvider"
)

# State store location
spark.conf.set(
    "spark.sql.streaming.stateStore.stateSchemaCheck",
    "false"
)
```

### Monitoring State Size

```python
# Check state store metrics
state_df = spark.read.format("statestore").load("/checkpoint/state")

# State size by partition
state_df.groupBy("partitionId").count().orderBy(desc("count")).show()

# State metadata
spark.read.format("state-metadata").load("/checkpoint").show()
```

### Limitations

```python
# State store size = number of unique keys within watermark
# With 10 min watermark and 100K events/sec:
# - 60M unique keys in state
# - ~2-4GB state store size (depends on key size)

# Warning signs:
# - Increasing batch duration
# - State store spill to disk
# - OOM errors
```

## Approach 2: Delta MERGE Deep Dive

### High-Scale Pattern

```python
from delta.tables import DeltaTable
import uuid

def high_scale_dedup(batch_df, batch_id):
    """
    Deduplicate at trillion-record scale using Delta.
    Handles late arriving data with idempotent inserts.
    """
    
    # Add batch metadata for debugging
    batch_with_meta = batch_df.withColumn(
        "_ingestion_batch_id", lit(batch_id)
    ).withColumn(
        "_ingestion_time", current_timestamp()
    )
    
    # Get or create target table
    target_path = "/delta/events_deduped"
    
    if DeltaTable.isDeltaTable(spark, target_path):
        target_table = DeltaTable.forPath(spark, target_path)
        
        # Merge with dedup key
        target_table.alias("target").merge(
            batch_with_meta.alias("source"),
            """
            target.event_id = source.event_id AND 
            target.event_date = source.event_date
            """
        ).whenNotMatchedInsertAll().execute()
    else:
        # First batch - create table
        batch_with_meta.write \
            .format("delta") \
            .partitionBy("event_date") \
            .save(target_path)

# Use in streaming query
(df.writeStream
    .foreachBatch(high_scale_dedup)
    .option("checkpointLocation", "/checkpoints/high-scale-dedup")
    .trigger(processingTime="30 seconds")
    .start()
)
```

### Optimized Merge with ZORDER

```python
# Optimize lookup performance with ZORDER
spark.sql("""
    OPTIMIZE delta.`/delta/events_deduped`
    ZORDER BY (event_id)
""")

# Auto-optimize settings
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

### Partitioning Strategy

```python
# Partition by date for efficient pruning
# Query only needs to check today's partition for most events

def dedup_with_partition_pruning(batch_df, batch_id):
    """Deduplicate with partition-aware merge"""
    
    # Get distinct dates in batch
    dates_in_batch = [row.event_date for row in batch_df.select("event_date").distinct().collect()]
    
    target_table = DeltaTable.forPath(spark, "/delta/events_deduped")
    
    # Add partition pruning hint
    batch_with_hint = batch_df.withColumn(
        "_date_list", lit(",".join(dates_in_batch))
    )
    
    # Merge with partition predicate
    target_table.alias("target").merge(
        batch_with_hint.alias("source"),
        """
        target.event_id = source.event_id AND
        target.event_date = source.event_date AND
        target.event_date IN (source._date_list)
        """
    ).whenNotMatchedInsertAll().execute()
```

## Hybrid Approach: Tiered Deduplication

### Best of Both Worlds

```python
def tiered_deduplication(batch_df, batch_id):
    """
    Use dropDuplicates for recent data (state store),
    Delta MERGE for historical lookup (scalable).
    """
    from delta.tables import DeltaTable
    
    # Step 1: Quick dedup within batch using state store concept
    recent_deduped = batch_df.dropDuplicates(["event_id"])
    
    # Step 2: Check against Delta table for duplicates
    target_table = DeltaTable.forPath(spark, "/delta/events_deduped")
    
    # Anti-join to find truly new events
    existing_ids = target_table.toDF().select("event_id")
    new_events = recent_deduped.join(existing_ids, "event_id", "left_anti")
    
    # Step 3: Insert new events
    if new_events.count() > 0:
        new_events.write \
            .format("delta") \
            .mode("append") \
            .save("/delta/events_deduped")

(df.writeStream
    .foreachBatch(tiered_deduplication)
    .option("checkpointLocation", "/checkpoints/tiered-dedup")
    .start()
)
```

## Common Patterns

### Pattern 1: Session Deduplication

```python
# Deduplicate entire sessions (all events with same session_id)
from pyspark.sql.window import Window

def dedup_sessions(batch_df, batch_id):
    """Keep only first event per session"""
    
    window_spec = Window.partitionBy("session_id").orderBy("timestamp")
    
    first_events = (batch_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    # Write deduplicated sessions
    first_events.write \
        .format("delta") \
        .mode("append") \
        .save("/delta/sessions")

(session_stream
    .writeStream
    .foreachBatch(dedup_sessions)
    .start()
)
```

### Pattern 2: Exactly-Once Kafka Output

```python
# Ensure exactly-once when writing back to Kafka
def exactly_once_kafka_output(batch_df, batch_id):
    """
    Use transaction IDs for exactly-once Kafka writes.
    """
    app_id = "my_streaming_job"
    
    # Deduplicate within batch first
    deduped = batch_df.dropDuplicates(["event_id"])
    
    # Write with transaction ID
    (deduped
        .select(to_json(struct("*")).alias("value"))
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("topic", "output-topic")
        .option("txnVersion", batch_id)  # For idempotency
        .option("txnAppId", app_id)
        .save()
    )
```

### Pattern 3: Global Deduplication (Cross-Cluster)

```python
# Use Delta table as global dedup store accessible by multiple clusters
# All streaming jobs check same Delta table

def global_dedup(batch_df, batch_id):
    """Global deduplication across multiple streaming jobs"""
    
    target_table = DeltaTable.forName(spark, "global_dedup.events")
    
    # Add job identifier
    enriched_df = batch_df.withColumn(
        "_source_job", lit("job_cluster_1")
    ).withColumn(
        "_ingested_at", current_timestamp()
    )
    
    # Merge with global table
    target_table.alias("target").merge(
        enriched_df.alias("source"),
        "target.global_event_id = source.global_event_id"
    ).whenNotMatchedInsertAll().execute()
```

## Performance Comparison

### State Store (dropDuplicates)

| Metric | Value |
|--------|-------|
| Latency | ~10-50ms overhead |
| Throughput | 100K-500K events/sec |
| State Size | Grows with unique keys |
| Recovery | Replay from checkpoint |

### Delta MERGE

| Metric | Value |
|--------|-------|
| Latency | ~100-500ms per batch |
| Throughput | 10K-100K events/sec |
| State Size | Bounded (table size) |
| Recovery | Table history + checkpoint |

### Decision Flowchart

```
Data Volume?
├── < 1M events/day → dropDuplicates (simple)
├── 1M - 100M events/day → dropDuplicates with watermark
├── 100M - 1B events/day → Delta MERGE with optimization
└── > 1B events/day → Delta MERGE + partitioning + ZORDER

Latency Requirement?
├── < 1 second → dropDuplicates
└── > 1 second → Delta MERGE acceptable

Key Cardinality?
├── Low (< 1M unique keys) → dropDuplicates
└── High (> 10M unique keys) → Delta MERGE
```

## Troubleshooting

### State Store Growing Too Large

```python
# Symptoms: OOM, slow batches, spill to disk
# Solutions:

# 1. Reduce watermark duration
.withWatermark("timestamp", "5 minutes")  # Instead of 30 minutes

# 2. Use composite keys (fewer unique combinations)
.dropDuplicates(["user_id"])  # Instead of ["user_id", "session_id", "event_id"]

# 3. Switch to Delta MERGE
```

### Delta MERGE Slow

```python
# Symptoms: Long merge times, high IO
# Solutions:

# 1. ZORDER on join keys
spark.sql("OPTIMIZE table ZORDER BY (event_id)")

# 2. Partition pruning
# Partition by date, include date in merge condition

# 3. Reduce batch size
.option("maxOffsetsPerTrigger", "1000")

# 4. Enable liquid clustering (DBR 13.3+)
spark.sql("ALTER TABLE events CLUSTER BY (event_id)")
```

### Duplicate Data Still Appearing

```python
# Check for:
# 1. Missing watermark → state never cleans up
# 2. Shared checkpoint between streams
# 3. Late data exceeding watermark
# 4. Case sensitivity in keys (EventID vs eventid)

# Debug: Check for near-duplicates
spark.sql("""
    SELECT event_id, COUNT(*) as cnt
    FROM events
    GROUP BY event_id
    HAVING cnt > 1
""").show()
```

## Best Practices

1. **Start with dropDuplicates** — Simpler, switch to Delta MERGE when needed
2. **Always use watermark** — Prevents infinite state growth
3. **Monitor state size** — Alert when > 10GB
4. **ZORDER on join keys** — Critical for Delta MERGE performance
5. **Partition by date** — Enables partition pruning
6. **Test recovery** — Verify exactly-once after restart
7. **Use composite keys wisely** — Balance uniqueness vs cardinality

## Related Skills

- `spark-structured-streaming-expert-pack` — Core streaming concepts
- `stream-stream-joins-expert-pack` — Stateful operations
- `streaming-best-practices-expert-pack` — Production guidelines
- `delta-lake-performance-tuning` — Optimize and ZORDER
