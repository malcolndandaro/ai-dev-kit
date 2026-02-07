---
name: cost-tuning-streaming
description: Optimize costs for Spark Structured Streaming workloads on Databricks. Use when reducing streaming job costs, right-sizing clusters, optimizing trigger intervals, implementing scheduled streaming, or managing multi-stream clusters efficiently.
---

# Cost Tuning for Streaming

Optimize streaming job costs through trigger tuning, cluster right-sizing, multi-stream clusters, storage optimization, and scheduled execution patterns.

## Quick Start

```python
# Cost-optimized: Scheduled streaming instead of continuous
df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/stream") \
    .trigger(availableNow=True) \  # Process all, then stop
    .start("/delta/target")

# Schedule via Databricks Jobs: Every 15 minutes
# Cost: ~$20/day for 100 tables on 8-core cluster
```

## Cost Optimization Strategies

### Strategy 1: Trigger Interval Tuning

Balance latency and cost:

```python
# Shorter interval = higher cost
.trigger(processingTime="5 seconds")   # Expensive - continuous processing

# Longer interval = lower cost
.trigger(processingTime="5 minutes")   # Cheaper - less frequent processing

# Use availableNow for batch-style (cheapest)
.trigger(availableNow=True)            # Process backlog, then stop

# Rule of thumb: SLA / 3
# Example: 1 hour SLA → 20 minute trigger
```

### Strategy 2: Cluster Right-Sizing

Right-size clusters based on workload:

```python
# Don't oversize:
# - Monitor CPU utilization (target 60-80%)
# - Check for idle time
# - Use fixed-size clusters (no autoscaling for streaming)

# Scale test approach:
# 1. Start small
# 2. Monitor lag (max offsets behind latest)
# 3. Scale up if falling behind
# 4. Right-size based on steady state
```

### Strategy 3: Multi-Stream Clusters

Run multiple streams on one cluster:

```python
# Run multiple streams on one cluster
# Tested: 100 streams on 8-core single-node cluster
# Cost: ~$20/day for 100 tables

# Example: Multiple streams on same cluster
stream1.writeStream.option("checkpointLocation", "/checkpoints/stream1").start()
stream2.writeStream.option("checkpointLocation", "/checkpoints/stream2").start()
stream3.writeStream.option("checkpointLocation", "/checkpoints/stream3").start()
# ... up to 100+ streams

# Monitor: CPU/memory per stream
# Scale cluster if aggregate utilization > 80%
```

### Strategy 4: Scheduled vs Continuous

Choose execution pattern based on SLA:

| Pattern | Cost | Latency | Use Case |
|---------|------|---------|----------|
| Continuous | $$$ | < 1 minute | Real-time requirements |
| 15-min schedule | $$ | 15-30 minutes | Near real-time |
| 4-hour schedule | $ | 4-5 hours | Batch-style SLA |

```python
# Continuous (expensive)
.trigger(processingTime="30 seconds")

# Scheduled (cost-effective)
.trigger(availableNow=True)  # Schedule via Jobs: Every 15 minutes

# Batch-style (cheapest)
.trigger(availableNow=True)  # Schedule via Jobs: Every 4 hours
```

### Strategy 5: Storage Optimization

Reduce storage costs:

```sql
-- VACUUM old files
VACUUM table RETAIN 24 HOURS;

-- Enable auto-optimize to reduce small files
ALTER TABLE table SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = true,
    'delta.autoOptimize.autoCompact' = true
);

-- Archive old data to cheaper storage
-- Use data retention policies
```

## Cost Formula

```
Daily Cost = 
    (Cluster DBU/hour × Hours running) +
    (Storage GB × Storage rate) +
    (Network egress if applicable)

Optimization levers:
- Reduce hours running (scheduled triggers)
- Reduce cluster size (right-sizing)
- Reduce storage (VACUUM, compression)
- Reduce network egress (co-locate compute and storage)
```

## Common Patterns

### Pattern 1: Scheduled Streaming

Convert continuous to scheduled:

```python
# Before: Continuous (expensive)
df.writeStream \
    .trigger(processingTime="30 seconds") \
    .start()

# After: Scheduled (cost-effective)
df.writeStream \
    .trigger(availableNow=True) \  # Process all, then stop
    .start()

# Schedule via Databricks Jobs:
# - Every 15 minutes: Near real-time
# - Every 4 hours: Batch-style
# Same code, different schedule
```

### Pattern 2: Multi-Stream Cluster

Optimize cluster utilization:

```python
# Run multiple streams on one cluster
def start_all_streams():
    streams = []
    
    # Start multiple streams
    for i in range(100):
        stream = (spark
            .readStream
            .table(f"source_{i}")
            .writeStream
            .format("delta")
            .option("checkpointLocation", f"/checkpoints/stream_{i}")
            .trigger(availableNow=True)
            .start(f"/delta/target_{i}")
        )
        streams.append(stream)
    
    return streams

# Monitor aggregate CPU/memory
# Scale cluster if needed
```

### Pattern 3: Right-Sizing Workflow

```python
# Step 1: Start small
cluster_config = {
    "num_workers": 2,
    "node_type_id": "i3.xlarge"
}

# Step 2: Monitor lag
for stream in spark.streams.active:
    progress = stream.lastProgress
    lag = progress.get("sources", [{}])[0].get("maxOffsetsBehindLatest", 0)
    print(f"Stream {stream.name}: Lag = {lag}")

# Step 3: Scale up if falling behind
# If lag increasing: increase cluster size
# If lag stable: current size is optimal
```

## Cost Monitoring

### Track Per-Stream Costs

```python
# Tag jobs with stream name
job_tags = {
    "stream_name": "orders_stream",
    "environment": "prod",
    "cost_center": "analytics"
}

# Use DBU consumption metrics
# Monitor by workspace/cluster
# Track cost per stream over time
```

### Monitor Cluster Utilization

```python
# Check CPU utilization
# Target: 60-80% utilization
# Below 60%: Consider downsizing
# Above 80%: Consider upsizing

# Check memory utilization
# Monitor for OOM errors
# Adjust cluster size accordingly
```

## Quick Wins

1. **Change from continuous to 15-minute schedule** - Significant cost reduction
2. **Run multiple streams per cluster** - Better cluster utilization
3. **Enable auto-optimize** - Reduce storage costs
4. **Use Spot instances** - For non-critical streams (with caution)
5. **Archive old data** - Move to cheaper storage tiers

## Trade-offs

| Cost Reduction | Impact | Mitigation |
|----------------|--------|------------|
| Longer trigger | Higher latency | Acceptable if SLA allows |
| Smaller cluster | May fall behind | Monitor lag; scale if needed |
| Aggressive VACUUM | Less time travel | Balance retention vs cost |
| Spot instances | Possible interruptions | Use for non-critical streams |
| Scheduled vs continuous | Higher latency | Match to business SLA |

## Production Best Practices

### Cluster Configuration

```python
# Fixed-size cluster (no autoscaling for streaming)
cluster_config = {
    "num_workers": 4,
    "node_type_id": "i3.xlarge",
    "autotermination_minutes": 60,  # Terminate if idle
    "enable_elastic_disk": True  # Reduce storage costs
}
```

### Trigger Selection

```python
# Match trigger to SLA
# SLA: 1 hour → trigger: 20 minutes (SLA / 3)
# SLA: 15 minutes → trigger: 5 minutes
# SLA: Real-time → continuous or RTM

trigger_interval = business_sla / 3
.trigger(processingTime=f"{trigger_interval} minutes")
```

### Storage Management

```sql
-- Enable auto-optimize
ALTER TABLE table SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = true,
    'delta.autoOptimize.autoCompact' = true
);

-- Periodic VACUUM
VACUUM table RETAIN 7 DAYS;  -- Balance retention vs cost

-- Archive old partitions
-- Move to cheaper storage tier
```

## Production Checklist

- [ ] Trigger interval optimized (match to SLA)
- [ ] Cluster right-sized (60-80% utilization)
- [ ] Multiple streams per cluster (if applicable)
- [ ] Scheduled execution (if SLA allows)
- [ ] Auto-optimize enabled
- [ ] Storage costs monitored
- [ ] Cost per stream tracked
- [ ] Trade-offs documented

## Related Skills

- `trigger-tuning` - Deep dive on trigger selection
- `checkpoint-best-practices` - Checkpoint management
- `kafka-to-delta` - Ingestion patterns
