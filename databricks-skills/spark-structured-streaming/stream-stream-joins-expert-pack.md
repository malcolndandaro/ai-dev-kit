---
name: "stream-stream-joins-expert-pack"
description: "Expert guide to stream-stream joins: event-time processing, watermarking, state management, and handling late data in real-time correlation pipelines."
tags: ["spark-streaming", "stream-stream-join", "watermark", "state-management", "expert"]
---

# Stream-Stream Joins Expert Pack

## Overview

Stream-stream joins correlate events from two streaming sources in real-time — matching orders with payments, clicks with conversions, or sensor readings across devices.

**Key Challenge**: Events may arrive out of order and at different speeds. Watermarks manage this uncertainty.

## Quick Start

```python
# Two streaming sources
orders = spark.readStream.format("kafka").load()
payments = spark.readStream.format("kafka").load()

# Join with watermarks
joined = (orders
    .withWatermark("order_time", "10 minutes")
    .join(
        payments.withWatermark("payment_time", "10 minutes"),
        expr("""
            order_id = payment_id AND
            payment_time >= order_time - interval 5 minutes AND
            payment_time <= order_time + interval 5 minutes
        """),
        "inner"
    )
)
```

## Core Concepts

### Event Time vs Processing Time

```python
# Event time: When the event occurred (recommended)
.withWatermark("event_time", "10 minutes")

# Processing time: When Spark processed it
# Not recommended for joins - non-deterministic
```

### Watermark Semantics

```python
# Watermark = event_time - delay_threshold
.withWatermark("timestamp", "10 minutes")

# Events with timestamp < watermark are considered "too late"
# Late events may be dropped (outer joins) or processed (inner joins)
```

## Join Types

| Type | Behavior | Use Case |
|------|----------|----------|
| **Inner** | Only matched events | Correlation analysis |
| **Left Outer** | All left + matched right | Enrichment with optional data |
| **Right Outer** | All right + matched left | Rarely used |
| **Full Outer** | All events from both | Complete picture |

## Common Patterns

### Pattern 1: Order-Payment Matching

```python
orders = (spark.readStream
    .format("kafka")
    .option("subscribe", "orders")
    .load()
    .select(from_json(col("value"), order_schema).alias("data"))
    .select("data.*")
)

payments = (spark.readStream
    .format("kafka")
    .option("subscribe", "payments")
    .load()
    .select(from_json(col("value"), payment_schema).alias("data"))
    .select("data.*")
)

# Match orders with payments within 10 minutes
matched = (orders
    .withWatermark("order_time", "10 minutes")
    .join(
        payments.withWatermark("payment_time", "10 minutes"),
        expr("""
            order_id = payment_id AND
            payment_time >= order_time - interval 5 minutes AND
            payment_time <= order_time + interval 10 minutes
        """),
        "leftOuter"
    )
)

matched.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/orders_payments") \
    .start("/delta/order_payments")
```

### Pattern 2: Sessionization

```python
# Group events into sessions with gaps
sessions = (events
    .withWatermark("event_time", "30 minutes")
    .groupBy(
        col("user_id"),
        session_window(col("event_time"), "10 minutes")
    )
    .agg(
        min("event_time").alias("session_start"),
        max("event_time").alias("session_end"),
        count("*").alias("event_count")
    )
)
```

### Pattern 3: Late Data Handling

```python
# Inner join: Late events may still match if other side hasn't expired
# Outer join: Late events are dropped from the outer side

# Strategy: Use inner join for critical matching
# Use left join + dead letter queue for audit

matched = orders.join(payments, "order_id", "leftOuter")

# Send unmatched to dead letter after watermark
unmatched = matched.filter(col("payment_id").isNull())
```

### Pattern 4: Multi-Stream Joins

```python
# Join more than 2 streams (chain them)
# A.join(B).join(C) works but increases state

# Better: Join related streams first
ab = stream_a.join(stream_b, "key", "inner")
abc = ab.join(stream_c, "key", "inner")
```

## State Management

### State Store Internals

```python
# State is stored in checkpoint: /checkpoint/state/
# RocksDB backend recommended for large state

spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "com.databricks.sql.streaming.state.RocksDBStateProvider")
```

### State Size Control

```python
# 1. Use watermarks (automatic cleanup)
.withWatermark("timestamp", "10 minutes")

# 2. Reduce key cardinality
# Bad: user_id (millions)
# Good: session_id (expires)

# 3. State TTL for mapGroupsWithState
# Configurable timeout for stale state
```

### Monitoring State

```python
# Read state store (for debugging)
state_df = (spark
    .read
    .format("statestore")
    .load("/checkpoint/state")
)

# Check partition distribution
state_df.groupBy("partitionId").count().orderBy(desc("count")).show()
```

## Watermark Strategy

### Choosing Watermark Delay

```python
# Balance between:
# - Latency (lower watermark = faster results)
# - Completeness (higher watermark = more matches)

# Rule of thumb: 2-3x the expected delay
# If 99th percentile delay is 5 min → watermark = 10-15 min
```

### Multiple Watermarks

```python
# When joining streams with different latencies
stream1.withWatermark("ts", "5 minutes").join(
    stream2.withWatermark("ts", "15 minutes")
)
# Effective watermark = max(5, 15) = 15 minutes
```

## Production Checklist

- [ ] Watermark configured on both sides
- [ ] Join condition includes time bounds
- [ ] State store provider set (RocksDB for scale)
- [ ] State size monitored
- [ ] Late data handling strategy defined
- [ ] Output mode appropriate for use case

## Performance Tuning

```python
# State store batch size
spark.conf.set("spark.sql.streaming.stateStore.minBatchesToRetain", "2")

# State maintenance interval
spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "5m")
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **State too large** | High cardinality keys | Reduce key space; add watermark |
| **Late events dropped** | Watermark too aggressive | Increase watermark delay |
| **No matches** | Time condition wrong | Check time bounds and units |
| **OOM errors** | State explosion | Use RocksDB; increase memory |

## Related Skills

- `stream-static-joins-expert-pack` — Dimension enrichment
- `watermarking-and-state-management` — Deep dive on state
- `spark-streaming-master-class-kafka-to-delta` — Kafka integration
