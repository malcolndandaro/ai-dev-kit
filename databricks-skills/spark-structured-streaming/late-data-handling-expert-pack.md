---
name: "late-data-handling-expert-pack"
description: "Expert strategies for handling out-of-order events in streaming: watermark tuning, late arrival patterns in joins and aggregations, recovery scenarios for delayed data."
tags: ["spark-streaming", "late-data", "watermark", "out-of-order", "state-management", "expert"]
---

# Late Data Handling Expert Pack

## Overview

Real-world streaming data rarely arrives in perfect order. Late data handling ensures completeness without sacrificing performance. This expert pack covers watermark tuning, join semantics for late arrivals, and recovery patterns for delayed data.

**Key Challenge**: Balance between latency (early results) and completeness (waiting for all data).

## Quick Start

### Basic Watermark Configuration

```python
from pyspark.sql.functions import col

# Default watermark: 10 minutes tolerance for late data
df = (spark.readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withWatermark("event_time", "10 minutes")  # Late data threshold
)

# Late data beyond watermark is dropped
```

### Watermark vs Late Data Window

```python
# Watermark = event_time - 10 minutes
# Events with timestamp < watermark are considered "too late"

# Late data window = watermark duration
# In this example: 10 minutes tolerance for lateness
```

## Common Patterns

### Pattern 1: Join-Specific Watermark Tuning

```python
# Stream-stream join with different latency expectations
impressions = (spark.readStream
    .format("kafka")
    .option("subscribe", "impressions")
    .load()
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), impression_schema).alias("data"))
    .select("data.*")
    .withWatermark("impression_time", "5 minutes")  # Fast source
)

clicks = (spark.readStream
    .format("kafka")
    .option("subscribe", "clicks")
    .load()
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), click_schema).alias("data"))
    .select("data.*")
    .withWatermark("click_time", "15 minutes")  # Slower source (more delay)
)

# Join with late data tolerance
joined = impressions.join(
    clicks,
    expr("""
        impressions.ad_id = clicks.ad_id AND
        clicks.click_time BETWEEN impressions.impression_time AND
                                impressions.impression_time + interval 1 hour
    """),
    "inner"
)
```

### Pattern 2: Aggregations with Late Data Recovery

```python
# Windowed aggregations with watermark
windowed = (df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    )
    .agg(
        count("*").alias("event_count"),
        sum("value").alias("total_value"),
        max("event_time").alias("latest_event")
    )
    .withColumn("processing_time", current_timestamp())
)

# Strategy: Update aggregations when late data arrives
# Use update mode (vs append) for corrected results
```

### Pattern 3: Dead Letter Queue for Very Late Data

```python
from pyspark.sql.functions import when, lit

def handle_late_data_with_dlq(df, batch_id):
    """Route late data to dead letter queue for manual processing"""
    
    # Calculate lateness
    df_with_lateness = df.withColumn(
        "processing_delay",
        unix_timestamp(current_timestamp()) - unix_timestamp(col("event_time"))
    )
    
    # On-time data (< 10 minutes)
    on_time = df_with_lateness.filter(col("processing_delay") <= 600)
    
    # Late but acceptable (10-30 minutes)
    late = df_with_lateness.filter(
        (col("processing_delay") > 600) & (col("processing_delay") <= 1800)
    )
    
    # Very late (> 30 minutes)
    very_late = df_with_lateness.filter(col("processing_delay") > 1800)
    
    # Write on-time to main table
    on_time.drop("processing_delay").write.format("delta").mode("append").save("/delta/events")
    
    # Write late to late table (can be merged later)
    late.write.format("delta").mode("append").save("/delta/late_events")
    
    # Write very late to DLQ for investigation
    very_late.write.format("delta").mode("append").save("/delta/dlq")

# Use forEachBatch for custom late data handling
df.writeStream.foreachBatch(handle_late_data_with_dlq).start()
```

### Pattern 4: Time-Travel Merge for Late Data Correction

```python
def merge_late_data_correction(batch_df, batch_id):
    """Merge late data into existing aggregations"""
    from delta.tables import DeltaTable
    
    target_table = DeltaTable.forPath(spark, "/delta/hourly_aggregations")
    
    # Update existing windows with late data
    (target_table.alias("target")
        .merge(
            batch_df.alias("source"),
            """
            target.window = source.window AND
            target.user_id = source.user_id
            """
        )
        .whenMatchedUpdate(set={
            "event_count": "target.event_count + source.event_count",
            "total_value": "target.total_value + source.total_value",
            "latest_event": "greatest(target.latest_event, source.latest_event)",
            "updated_at": "current_timestamp()"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
```

## Reference Files

### Watermark Semantics

| Watermark Setting | Effect | Use Case |
|-------------------|--------|----------|
| `"10 minutes"` | Moderate latency | General streaming |
| `"1 hour"` | High completeness | Financial transactions |
| `"5 minutes"` | Low latency | Real-time analytics |
| `"24 hours"` | Batch-like | Backfill scenarios |

### Late Data Classification

| Delay | Category | Handling |
|-------|----------|----------|
| < Watermark | On-time | Normal processing |
| Watermark < delay < 2×Watermark | Late | Join with inner match |
| > 2×Watermark | Very late | DLQ for manual handling |

### State Store Impact

```python
# Watermark directly affects state store size
# State kept for watermark duration + processing time

# Example: 10 minute watermark with 1M events/min
# State size = ~10M keys × key_size
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **State store explosion** | Watermark too long | Reduce watermark; archive old state |
| **Late data dropped** | Watermark too short | Increase watermark; analyze latency patterns |
| **Inconsistent results** | Varying lateness | Use event-time processing + consistent watermarks |
| **DLQ overflow** | Systematic delays | Investigate upstream systems; adjust SLA |

## Advanced Tips

### Dynamic Watermark Tuning

```python
# Adjust watermark based on observed lateness
def adaptive_watermark(df):
    """Calculate watermark based on p95 latency"""
    # Get latency statistics from metrics
    latency_df = spark.sql("""
        SELECT percentile(processing_delay, 0.95) as p95_latency
        FROM event_metrics
        WHERE timestamp > current_timestamp() - interval 1 hour
    """)
    
    p95 = latency_df.collect()[0]["p95_latency"]
    
    # Set watermark = p95 × 1.5 (safety margin)
    watermark_duration = f"{int(p95 * 1.5)} minutes"
    
    return df.withWatermark("event_time", watermark_duration)
```

### Cross-Stream Watermark Synchronization

```python
# Ensure consistent watermarks across related streams
def synchronized_watermarks(streams):
    """Apply same watermark to all related streams"""
    base_watermark = "15 minutes"
    
    synchronized_streams = []
    for stream in streams:
        synchronized_streams.append(
            stream.withWatermark("event_time", base_watermark)
        )
    
    return synchronized_streams
```

### Late Data Impact Measurement

```python
# Monitor late data rates
def measure_late_data_impact():
    """Track how much data is arriving late"""
    metrics_df = spark.sql("""
        SELECT 
            date_trunc('hour', event_time) as hour,
            COUNT(*) as total_events,
            SUM(CASE 
                WHEN unix_timestamp(processing_time) - unix_timestamp(event_time) > 600 
                THEN 1 ELSE 0 
            END) as late_events,
            AVG(unix_timestamp(processing_time) - unix_timestamp(event_time)) as avg_delay_seconds
        FROM events
        GROUP BY 1
        ORDER BY 1 DESC
    """)
    
    return metrics_df
```

## FAQ

**Q: How do I choose the right watermark duration?**
A: Start with 2-3× your p95 latency. Monitor late data rate and adjust.

**Q: What happens to late data in aggregations?**
A: With watermark, late data may be dropped. Consider update mode or DLQ.

**Q: Can I recover dropped late data?**
A: Yes, through backfill or manual merge if stored in DLQ.

**Q: How does watermark affect state store size?**
A: State kept for watermark duration + processing. Larger watermark = larger state.

**Q: What if my data has inconsistent lateness?**
A: Use adaptive watermarks or separate streams by latency category.

**Q: Can I have different watermarks for different operations?**
A: Yes, but join operations use the maximum of the two watermarks.

## Related Skills

- `stream-stream-joins-expert-pack` — Late data in joins
- `state-store-tuning` — State management for late data
- `streaming-deduplication-scale` — Deduplication with watermarks
- `trigger-tuning-deep-dive` — Trigger impact on late data