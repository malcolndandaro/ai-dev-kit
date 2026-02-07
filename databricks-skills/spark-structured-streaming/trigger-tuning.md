---
name: trigger-tuning
description: Select and tune triggers for Spark Structured Streaming to balance latency and cost. Use when choosing between processingTime, availableNow, and Real-Time Mode (RTM), calculating optimal trigger intervals, optimizing latency vs cost trade-offs, or configuring sub-second latency pipelines.
---

# Trigger Tuning

Select and tune triggers to balance latency requirements with cost. Choose between processingTime, availableNow, and Real-Time Mode (RTM) based on your use case.

## Quick Start

```python
# Continuous processing (low latency, higher cost)
.trigger(processingTime="30 seconds")

# Scheduled processing (batch-style, lower cost)
.trigger(availableNow=True)  # Schedule via Jobs: Every 15 minutes

# Real-Time Mode (sub-second latency, requires Photon)
.trigger(realTime=True)  # < 800ms latency
```

## Trigger Types

### ProcessingTime Trigger

Process at fixed intervals:

```python
# Process every 30 seconds
.trigger(processingTime="30 seconds")

# Process every 5 minutes
.trigger(processingTime="5 minutes")

# Latency: Trigger interval + processing time
# Cost: Continuous cluster running
```

### AvailableNow Trigger

Process all available data, then stop:

```python
# Process all available data, then stop
.trigger(availableNow=True)

# Schedule via Databricks Jobs:
# - Every 15 minutes: Near real-time
# - Every 4 hours: Batch-style

# Latency: Schedule interval + processing time
# Cost: Cluster runs only during processing
```

### Real-Time Mode (RTM)

Sub-second latency with Photon:

```python
# Real-Time Mode (Databricks 13.3+)
.trigger(realTime=True)

# Requirements:
# - Photon enabled
# - Fixed-size cluster (no autoscaling)
# - Latency: < 800ms

# Cost: Continuous cluster with Photon
```

## Trigger Selection Guide

| Latency Requirement | Trigger | Cost | Use Case |
|---------------------|---------|------|----------|
| < 800ms | RTM | $$$ | Real-time analytics, alerts |
| 1-30 seconds | processingTime | $$ | Near real-time dashboards |
| 15-60 minutes | availableNow (scheduled) | $ | Batch-style SLA |
| > 1 hour | availableNow (scheduled) | $ | ETL pipelines |

## Trigger Interval Calculation

### Rule of Thumb: SLA / 3

```python
# Calculate trigger interval from SLA
business_sla_minutes = 60  # 1 hour SLA
trigger_interval_minutes = business_sla_minutes / 3  # 20 minutes

.trigger(processingTime=f"{trigger_interval_minutes} minutes")

# Why /3?
# - Processing time buffer
# - Recovery time buffer
# - Safety margin
```

### Example Calculations

```python
# Example 1: 1 hour SLA
sla = 60  # minutes
trigger = sla / 3  # 20 minutes
.trigger(processingTime="20 minutes")

# Example 2: 15 minute SLA
sla = 15  # minutes
trigger = sla / 3  # 5 minutes
.trigger(processingTime="5 minutes")

# Example 3: Real-time requirement
.trigger(realTime=True)  # < 800ms
```

## Real-Time Mode (RTM) Configuration

### Enable RTM

```python
# Enable Real-Time Mode
.trigger(realTime=True)

# Required configurations:
spark.conf.set("spark.databricks.photon.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
               "com.databricks.sql.streaming.state.RocksDBStateProvider")

# Cluster requirements:
# - Fixed-size cluster (no autoscaling)
# - Photon enabled
# - Driver: Minimum 4 cores
```

### RTM Use Cases

```python
# Good for RTM:
# - Sub-second latency requirements
# - Simple transformations
# - Stateless operations
# - Kafka-to-Kafka pipelines

# Not recommended for RTM:
# - Stateful operations (aggregations, joins)
# - Complex transformations
# - Large batch sizes
```

## Common Patterns

### Pattern 1: Cost-Optimized Scheduled Streaming

Convert continuous to scheduled:

```python
# Before: Continuous (expensive)
df.writeStream \
    .trigger(processingTime="30 seconds") \
    .start()

# After: Scheduled (cost-effective)
df.writeStream \
    .trigger(availableNow=True) \
    .start()

# Schedule via Databricks Jobs:
# - Every 15 minutes: Near real-time
# - Every 4 hours: Batch-style
# Same code, different schedule
```

### Pattern 2: Latency-Optimized Continuous

Optimize for low latency:

```python
# Short trigger interval for low latency
df.writeStream \
    .trigger(processingTime="5 seconds") \
    .start()

# Trade-off: Higher cost, lower latency
# Monitor: Ensure processing time < trigger interval
```

### Pattern 3: RTM for Sub-Second Latency

Use RTM for real-time requirements:

```python
# Real-Time Mode for sub-second latency
df.writeStream \
    .format("kafka")
    .option("topic", "output")
    .trigger(realTime=True) \
    .start()

# Latency: < 800ms
# Cost: Continuous cluster with Photon
```

## Latency vs Cost Trade-offs

### Continuous Processing

```python
# High cost, low latency
.trigger(processingTime="30 seconds")

# Cost: Continuous cluster running
# Latency: 30 seconds + processing time
# Use when: Real-time requirements
```

### Scheduled Processing

```python
# Lower cost, higher latency
.trigger(availableNow=True)  # Schedule: Every 15 minutes

# Cost: Cluster runs only during processing
# Latency: Schedule interval + processing time
# Use when: Batch-style SLA acceptable
```

### Real-Time Mode

```python
# Highest cost, lowest latency
.trigger(realTime=True)

# Cost: Continuous cluster with Photon
# Latency: < 800ms
# Use when: Sub-second latency required
```

## Performance Considerations

### Batch Duration vs Trigger Interval

```python
# Batch duration should be < trigger interval
# Example:
trigger_interval = 30  # seconds
batch_duration = 10  # seconds

# Healthy: batch_duration < trigger_interval
# Unhealthy: batch_duration >= trigger_interval

# Monitor in Spark UI:
# - Batch duration
# - Trigger interval
# - Alert if batch duration >= trigger interval
```

### Trigger Interval Tuning

```python
# Start conservative, optimize based on monitoring
# Step 1: Start with SLA / 3
trigger_interval = business_sla / 3

# Step 2: Monitor batch duration
# If batch duration < trigger_interval / 2: Can increase trigger
# If batch duration >= trigger_interval: Decrease trigger

# Step 3: Optimize for cost vs latency
# Increase trigger interval to reduce cost
# Decrease trigger interval to reduce latency
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **High latency** | Trigger interval too long | Decrease trigger interval or use RTM |
| **High cost** | Continuous processing | Use scheduled (availableNow) |
| **Batch duration > trigger** | Processing too slow | Optimize processing or increase trigger |
| **RTM not working** | Photon not enabled | Enable Photon and configure cluster |

## Production Best Practices

### Match Trigger to SLA

```python
# Calculate trigger from business SLA
def calculate_trigger_interval(sla_minutes):
    """Calculate optimal trigger interval"""
    return max(30, sla_minutes / 3)  # Minimum 30 seconds

trigger_interval = calculate_trigger_interval(business_sla_minutes)
.trigger(processingTime=f"{trigger_interval} seconds")
```

### Monitor Trigger Performance

```python
# Monitor batch duration vs trigger interval
for stream in spark.streams.active:
    progress = stream.lastProgress
    if progress:
        duration_ms = progress.get("durationMs", {}).get("triggerExecution", 0)
        trigger_ms = get_trigger_interval_ms(stream)
        
        if duration_ms >= trigger_ms:
            send_alert(f"Batch duration {duration_ms}ms >= trigger {trigger_ms}ms")
```

### Use Scheduled for Cost Optimization

```python
# Use availableNow + scheduling for cost optimization
df.writeStream \
    .trigger(availableNow=True) \
    .start()

# Schedule via Databricks Jobs:
# - Every 15 minutes: Near real-time
# - Every 4 hours: Batch-style
# Cost: Cluster runs only during processing
```

## Production Checklist

- [ ] Trigger type selected based on latency requirements
- [ ] Trigger interval calculated from SLA (SLA / 3)
- [ ] Batch duration monitored (< trigger interval)
- [ ] RTM configured if sub-second latency required
- [ ] Scheduled execution considered for cost optimization
- [ ] Performance metrics tracked
- [ ] Alerting configured for batch duration issues

## Related Skills

- `cost-tuning` - Cost optimization with trigger tuning
- `monitoring-observability` - Monitor trigger performance
- `kafka-to-kafka` - RTM configuration for Kafka pipelines
