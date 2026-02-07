---
name: watermark-configuration
description: Configure watermarks for handling late-arriving data in Spark Structured Streaming. Use when setting up stateful operations, tuning watermark duration, handling out-of-order events, managing state store size, or implementing late data recovery patterns.
---

# Watermark Configuration

Configure watermarks to handle late-arriving data while managing state store size. Watermarks define how long to wait for late data before considering it "too late".

## Quick Start

```python
from pyspark.sql.functions import col

# Configure watermark for late data tolerance
df = (spark.readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withWatermark("event_time", "10 minutes")  # Late data threshold
)

# Watermark = latest_event_time - 10 minutes
# Events with timestamp < watermark are considered "too late"
```

## Watermark Semantics

### How Watermarks Work

```python
# Watermark = latest_event_time - delay_threshold
.withWatermark("event_time", "10 minutes")

# Events with timestamp < watermark are considered "too late"
# State for late events is automatically cleaned up
# Late events may be dropped (outer joins) or processed (inner joins)
```

### Watermark Duration Selection

| Watermark Setting | Effect | Use Case |
|-------------------|--------|----------|
| `"10 minutes"` | Moderate latency | General streaming |
| `"1 hour"` | High completeness | Financial transactions |
| `"5 minutes"` | Low latency | Real-time analytics |
| `"24 hours"` | Batch-like | Backfill scenarios |

**Rule of thumb**: Start with 2-3× your p95 latency. Monitor late data rate and adjust.

## Common Patterns

### Pattern 1: Basic Watermark Configuration

Configure watermark for stateful operations:

```python
# Watermark for deduplication
df = (spark.readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"])
)

# State expires after watermark duration
# Prevents infinite state growth
```

### Pattern 2: Join-Specific Watermark Tuning

Different watermarks for streams with different latencies:

```python
# Fast source: 5 minute watermark
impressions = (spark.readStream
    .format("kafka")
    .option("subscribe", "impressions")
    .load()
    .select(from_json(col("value").cast("string"), impression_schema).alias("data"))
    .select("data.*")
    .withWatermark("impression_time", "5 minutes")
)

# Slower source: 15 minute watermark
clicks = (spark.readStream
    .format("kafka")
    .option("subscribe", "clicks")
    .load()
    .select(from_json(col("value").cast("string"), click_schema).alias("data"))
    .select("data.*")
    .withWatermark("click_time", "15 minutes")
)

# Effective watermark = max(5, 15) = 15 minutes
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

### Pattern 3: Windowed Aggregations with Watermark

Handle late data in aggregations:

```python
from pyspark.sql.functions import window, count, sum, max, current_timestamp

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

# Use update mode for corrected results when late data arrives
windowed.writeStream \
    .outputMode("update") \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/windowed") \
    .start("/delta/windowed_metrics")
```

### Pattern 4: Late Data Handling with Dead Letter Queue

Route very late data to DLQ:

```python
from pyspark.sql.functions import unix_timestamp, current_timestamp, when, lit

def handle_late_data_with_dlq(batch_df, batch_id):
    """Route late data to dead letter queue"""
    
    # Calculate lateness
    df_with_lateness = batch_df.withColumn(
        "processing_delay_seconds",
        unix_timestamp(current_timestamp()) - unix_timestamp(col("event_time"))
    )
    
    # On-time data (< watermark)
    on_time = df_with_lateness.filter(col("processing_delay_seconds") <= 600)  # 10 minutes
    
    # Late but acceptable (watermark to 2×watermark)
    late = df_with_lateness.filter(
        (col("processing_delay_seconds") > 600) & 
        (col("processing_delay_seconds") <= 1800)  # 30 minutes
    )
    
    # Very late (> 2×watermark)
    very_late = df_with_lateness.filter(col("processing_delay_seconds") > 1800)
    
    # Write on-time to main table
    if on_time.count() > 0:
        (on_time
            .drop("processing_delay_seconds")
            .write
            .format("delta")
            .mode("append")
            .option("txnVersion", batch_id)
            .option("txnAppId", "streaming_job")
            .saveAsTable("events")
        )
    
    # Write late to late table (can be merged later)
    if late.count() > 0:
        (late
            .withColumn("late_reason", lit("LATE_ARRIVAL"))
            .write
            .format("delta")
            .mode("append")
            .saveAsTable("late_events")
        )
    
    # Write very late to DLQ for investigation
    if very_late.count() > 0:
        (very_late
            .withColumn("dlq_reason", lit("VERY_LATE"))
            .withColumn("dlq_timestamp", current_timestamp())
            .write
            .format("delta")
            .mode("append")
            .saveAsTable("dlq_events")
        )

df.writeStream \
    .foreachBatch(handle_late_data_with_dlq) \
    .option("checkpointLocation", "/checkpoints/late_data") \
    .start()
```

### Pattern 5: Time-Travel Merge for Late Data Correction

Merge late data into existing aggregations:

```python
from delta.tables import DeltaTable

def merge_late_data_correction(batch_df, batch_id):
    """Merge late data into existing aggregations"""
    
    target_table = DeltaTable.forName(spark, "hourly_aggregations")
    
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

windowed.writeStream \
    .foreachBatch(merge_late_data_correction) \
    .option("checkpointLocation", "/checkpoints/late_merge") \
    .start()
```

## State Store Impact

### Watermark and State Size

```python
# Watermark directly affects state store size
# State kept for watermark duration + processing time

# Example calculation:
# - 10 minute watermark
# - 1M events/min
# - State size = ~10M keys × key_size

# Reduce watermark to reduce state size
.withWatermark("event_time", "5 minutes")  # Smaller state
```

### State Cleanup

```python
# State automatically expires after watermark duration
.withWatermark("event_time", "10 minutes")

# State for events older than watermark is cleaned up
# No manual cleanup needed
```

## Late Data Classification

| Delay | Category | Handling |
|-------|----------|----------|
| < Watermark | On-time | Normal processing |
| Watermark < delay < 2×Watermark | Late | Join with inner match; may still process |
| > 2×Watermark | Very late | DLQ for manual handling |

## Monitoring Late Data

### Track Late Data Rates

```python
# Monitor late data impact
late_data_stats = spark.sql("""
    SELECT 
        date_trunc('hour', event_time) as hour,
        COUNT(*) as total_events,
        SUM(CASE 
            WHEN unix_timestamp(processing_time) - unix_timestamp(event_time) > 600 
            THEN 1 ELSE 0 
        END) as late_events,
        AVG(unix_timestamp(processing_time) - unix_timestamp(event_time)) as avg_delay_seconds,
        MAX(unix_timestamp(processing_time) - unix_timestamp(event_time)) as max_delay_seconds
    FROM events
    WHERE processing_time >= current_timestamp() - interval 24 hours
    GROUP BY 1
    ORDER BY 1 DESC
""")

late_data_stats.show()

# Alert if late event rate > threshold
```

### Measure Watermark Effectiveness

```python
# Check if watermark is appropriate
watermark_analysis = spark.sql("""
    SELECT 
        COUNT(*) as total_events,
        SUM(CASE 
            WHEN unix_timestamp(processing_time) - unix_timestamp(event_time) > 600 
            THEN 1 ELSE 0 
        END) as late_count,
        SUM(CASE 
            WHEN unix_timestamp(processing_time) - unix_timestamp(event_time) > 600 
            THEN 1 ELSE 0 
        END) * 100.0 / COUNT(*) as late_rate_pct,
        percentile_approx(
            unix_timestamp(processing_time) - unix_timestamp(event_time), 
            0.95
        ) as p95_delay_seconds
    FROM events
    WHERE processing_time >= current_timestamp() - interval 1 hour
""")

# If late_rate_pct > 5%: Consider increasing watermark
# If p95_delay > watermark: Increase watermark
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **State store explosion** | Watermark too long | Reduce watermark; archive old state |
| **Late data dropped** | Watermark too short | Increase watermark; analyze latency patterns |
| **Inconsistent results** | Varying lateness | Use event-time processing + consistent watermarks |
| **DLQ overflow** | Systematic delays | Investigate upstream systems; adjust SLA |

## Advanced Patterns

### Adaptive Watermark Tuning

Adjust watermark based on observed lateness:

```python
def adaptive_watermark(df):
    """Calculate watermark based on p95 latency"""
    # Get latency statistics from metrics
    latency_df = spark.sql("""
        SELECT percentile_approx(processing_delay_seconds, 0.95) as p95_latency
        FROM event_metrics
        WHERE timestamp > current_timestamp() - interval 1 hour
    """)
    
    p95 = latency_df.collect()[0]["p95_latency"]
    
    # Set watermark = p95 × 1.5 (safety margin)
    watermark_duration = f"{int(p95 * 1.5)} minutes"
    
    return df.withWatermark("event_time", watermark_duration)
```

### Cross-Stream Watermark Synchronization

Ensure consistent watermarks across related streams:

```python
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

## Production Best Practices

### Watermark Selection

```python
# Rule of thumb: 2-3× p95 latency
# Example: p95 latency = 5 minutes → watermark = 10-15 minutes

# Start conservative, adjust based on monitoring
.withWatermark("event_time", "10 minutes")  # Start here
# Monitor late data rate
# Increase if too many late events
# Decrease if state too large
```

### Always Use Watermarks for Stateful Operations

```python
# REQUIRED: Watermark for stateful operations
df.withWatermark("event_time", "10 minutes").dropDuplicates(["id"])

# REQUIRED: Watermark for aggregations
df.withWatermark("event_time", "10 minutes").groupBy(...).agg(...)

# REQUIRED: Watermark for stream-stream joins
stream1.withWatermark("ts", "10 min").join(stream2.withWatermark("ts", "10 min"))
```

## Production Checklist

- [ ] Watermark configured for all stateful operations
- [ ] Watermark duration matches latency requirements (2-3× p95)
- [ ] Late data monitoring configured
- [ ] DLQ strategy defined for very late data
- [ ] State store size monitored
- [ ] Watermark effectiveness tracked
- [ ] Alerting on high late data rates

## Related Skills

- `stream-stream-joins` - Late data in joins
- `state-store-management` - State management with watermarks
- `error-handling-recovery` - DLQ patterns for late data
