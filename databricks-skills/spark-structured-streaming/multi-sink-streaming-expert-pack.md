---
name: "multi-sink-streaming-expert-pack"
description: "Expert patterns for writing stream output to multiple tables: broadcast-to-multiple-sinks, ForEachBatch fan-out, and transactional consistency patterns."
tags: ["spark-streaming", "multi-sink", "foreachbatch", "fan-out", "expert"]
---

# Multi-Sink Streaming Expert Pack

## Overview

Writing streaming output to multiple destinations is a common requirement: bronze/silver/gold tiers, raw vs aggregated views, or different consumer teams.

**Key Challenge**: Do it efficiently without reprocessing the source multiple times.

## Quick Start

```python
# Pattern: Read once, write many using ForEachBatch

def write_multiple_tables(batch_df, batch_id):
    """Write batch to multiple sinks"""
    # Bronze - raw data
    batch_df.write \
        .format("delta") \
        .mode("append") \
        .save("/delta/bronze_events")
    
    # Silver - cleansed
    cleansed = batch_df.dropDuplicates(["event_id"])
    cleansed.write \
        .format("delta") \
        .mode("append") \
        .save("/delta/silver_events")
    
    # Gold - aggregated
    aggregated = batch_df.groupBy("category").count()
    aggregated.write \
        .format("delta") \
        .mode("append") \
        .save("/delta/category_counts")

stream.writeStream \
    .foreachBatch(write_multiple_tables) \
    .option("checkpointLocation", "/checkpoints/multi_sink") \
    .start()
```

## Common Patterns

### Pattern 1: Bronze-Silver-Gold in One Stream

```python
def medallion_architecture(batch_df, batch_id):
    """Single stream feeding all three medallion layers"""
    
    # Bronze: Raw ingestion
    (batch_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("bronze.events")
    )
    
    # Silver: Cleansed and validated
    silver_df = (batch_df
        .dropDuplicates(["event_id"])
        .filter(col("status").isin(["active", "pending"]))
        .withColumn("processed_at", current_timestamp())
    )
    
    (silver_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("silver.events")
    )
    
    # Gold: Business aggregates
    gold_df = (silver_df
        .groupBy(window(col("timestamp"), "5 minutes"), "category")
        .agg(
            count("*").alias("event_count"),
            sum("amount").alias("total_amount")
        )
    )
    
    (gold_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("gold.category_metrics")
    )

# Single stream, multiple outputs
stream.writeStream \
    .foreachBatch(medallion_architecture) \
    .trigger(processingTime="30 seconds") \
    .start()
```

### Pattern 2: Parallel Fan-Out with Async

```python
from concurrent.futures import ThreadPoolExecutor
import asyncio

def parallel_write(batch_df, batch_id):
    """Write to multiple sinks in parallel"""
    
    # Create separate DataFrames (lazy evaluation)
    df1 = batch_df.filter(col("region") == "NA")
    df2 = batch_df.filter(col("region") == "EU")
    df3 = batch_df.filter(col("region") == "APAC")
    
    def write_region(df, region):
        df.write.format("delta").mode("append").save(f"/delta/events_{region}")
    
    # Parallel writes
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.submit(write_region, df1, "na")
        executor.submit(write_region, df2, "eu")
        executor.submit(write_region, df3, "apac")
```

### Pattern 3: Conditional Routing

```python
def route_by_type(batch_df, batch_id):
    """Route events to different tables based on type"""
    
    # Split by event type
    orders = batch_df.filter(col("event_type") == "order")
    refunds = batch_df.filter(col("event_type") == "refund")
    reviews = batch_df.filter(col("event_type") == "review")
    
    # Write to respective tables
    if orders.count() > 0:
        orders.write.format("delta").mode("append").saveAsTable("orders")
    
    if refunds.count() > 0:
        refunds.write.format("delta").mode("append").saveAsTable("refunds")
    
    if reviews.count() > 0:
        reviews.write.format("delta").mode("append").saveAsTable("reviews")
```

### Pattern 4: Transactional Multi-Table Writes

```python
def transactional_write(batch_df, batch_id):
    """Ensure atomicity across multiple tables using transaction IDs"""
    
    # Use batch_id as transaction version
    txn_version = batch_id
    app_id = "multi_sink_stream"
    
    # Table 1
    (batch_df.write
        .format("delta")
        .option("txnVersion", txn_version)
        .option("txnAppId", f"{app_id}_table1")
        .mode("append")
        .saveAsTable("table1")
    )
    
    # Table 2
    (batch_df.write
        .format("delta")
        .option("txnVersion", txn_version)
        .option("txnAppId", f"{app_id}_table2")
        .mode("append")
        .saveAsTable("table2")
    )
    
    # If one fails, Spark will retry the entire batch
```

### Pattern 5: Stream to Multiple Kafka Topics

```python
def kafka_fan_out(batch_df, batch_id):
    """Write to multiple Kafka topics"""
    
    # High priority events
    high_priority = batch_df.filter(col("priority") == "high")
    if high_priority.count() > 0:
        (high_priority.select(to_json(struct("*")).alias("value"))
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("topic", "high_priority_events")
            .save()
        )
    
    # Standard events
    standard = batch_df.filter(col("priority") == "standard")
    if standard.count() > 0:
        (standard.select(to_json(struct("*")).alias("value"))
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("topic", "standard_events")
            .save()
        )
```

## Performance Optimization

### Minimize Recomputation

```python
def optimized_multi_sink(batch_df, batch_id):
    """Cache to avoid recomputation"""
    
    # Cache the batch
    batch_df.cache()
    
    # Multiple writes from cached data
    batch_df.write...  # Sink 1
    batch_df.write...  # Sink 2
    batch_df.write...  # Sink 3
    
    # Unpersist when done
    batch_df.unpersist()
```

### Checkpoint Strategy

```python
# One checkpoint for the entire multi-sink stream
# NOT multiple checkpoints

stream.writeStream \
    .foreachBatch(multi_sink_function) \
    .option("checkpointLocation", "/checkpoints/single_source_multi_sink") \
    .start()
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **Slow writes** | Sequential processing | Use parallel ThreadPoolExecutor |
| **Recomputation** | Multiple actions | Cache the batch DataFrame |
| **Partial failures** | One sink fails | Use idempotent writes; Spark retries |
| **Schema conflicts** | Tables have different schemas | Transform before each write |
| **Resource contention** | Too many concurrent writes | Limit parallelism; batch writes |

## Best Practices

1. **One source, one checkpoint** — Don't create separate streams
2. **Cache in ForEachBatch** — Avoid recomputation
3. **Use ThreadPoolExecutor** — Parallelize independent writes
4. **Idempotent writes** — txnVersion for exactly-once
5. **Monitor all sinks** — Each output needs observability

## Related Skills

- `parallel-writes-foreachbatch` — ForEachBatch optimization
- `foreachbatch-patterns-expert-pack` — Deep dive on ForEachBatch
- `spark-streaming-master-class-kafka-to-delta` — Kafka integration
