---
name: monitoring-observability
description: Monitor and observe Spark Structured Streaming jobs for production reliability. Use when tracking stream health, setting up alerting, monitoring key metrics (input rate, lag, batch duration), navigating Spark UI, or implementing programmatic monitoring.
---

# Monitoring and Observability

Monitor Spark Structured Streaming jobs to ensure reliable production operation. Track key metrics, set up alerting, and navigate Spark UI for troubleshooting.

## Quick Start

```python
# Programmatic monitoring
for stream in spark.streams.active:
    status = stream.status
    progress = stream.lastProgress
    
    if progress:
        print(f"Stream: {stream.name}")
        print(f"Input rate: {progress.get('inputRowsPerSecond', 0)} rows/sec")
        print(f"Processing rate: {progress.get('processedRowsPerSecond', 0)} rows/sec")
        
        # Kafka-specific: Check lag
        sources = progress.get("sources", [])
        for source in sources:
            end_offset = source.get("endOffset", {})
            latest_offset = source.get("latestOffset", {})
            # Calculate lag per partition
```

## Key Metrics

### Input Rate vs Processing Rate

```python
# Critical metric: Processing must exceed input
for stream in spark.streams.active:
    progress = stream.lastProgress
    if progress:
        input_rate = progress.get('inputRowsPerSecond', 0)
        processing_rate = progress.get('processedRowsPerSecond', 0)
        
        print(f"Input rate: {input_rate} rows/sec")
        print(f"Processing rate: {processing_rate} rows/sec")
        
        # Alert if processing < input
        if processing_rate < input_rate:
            send_alert(f"Stream {stream.name} falling behind: {processing_rate} < {input_rate}")
```

### Max Offsets Behind Latest

```python
# Kafka-specific lag metric
for stream in spark.streams.active:
    progress = stream.lastProgress
    if progress:
        sources = progress.get("sources", [])
        for source in sources:
            end_offset = source.get("endOffset", {})
            latest_offset = source.get("latestOffset", {})
            
            # Calculate lag per partition
            max_lag = 0
            for topic, partitions in end_offset.items():
                for partition, end in partitions.items():
                    latest = latest_offset.get(topic, {}).get(partition, end)
                    lag = int(latest) - int(end)
                    max_lag = max(max_lag, lag)
                    print(f"Topic {topic}, Partition {partition}: Lag = {lag}")
            
            # Alert if lag increasing
            if max_lag > lag_threshold:
                send_alert(f"Stream {stream.name} lag: {max_lag}")
```

### Batch Duration vs Trigger Interval

```python
# Batch duration should be < trigger interval
for stream in spark.streams.active:
    progress = stream.lastProgress
    if progress:
        duration_ms = progress.get("durationMs", {}).get("triggerExecution", 0)
        trigger_interval = stream.trigger  # Get trigger interval
        
        print(f"Batch duration: {duration_ms} ms")
        print(f"Trigger interval: {trigger_interval}")
        
        # Alert if batch duration > trigger interval
        if duration_ms > trigger_interval_ms:
            send_alert(f"Stream {stream.name} batch duration exceeds trigger interval")
```

### State Store Metrics

```python
# Monitor state store size
for stream in spark.streams.active:
    progress = stream.lastProgress
    if progress and "stateOperators" in progress:
        for op in progress["stateOperators"]:
            print(f"Operator: {op.get('operatorName', 'unknown')}")
            print(f"State rows: {op.get('numRowsTotal', 0)}")
            print(f"State memory: {op.get('memoryUsedBytes', 0)}")
            print(f"State on disk: {op.get('diskBytesUsed', 0)}")
            
            # Alert if state too large
            if op.get('numRowsTotal', 0) > 10_000_000:
                send_alert(f"State size: {op.get('numRowsTotal', 0)} rows")
```

## Programmatic Monitoring

### Comprehensive Stream Health Check

```python
def check_stream_health(stream_name=None):
    """Comprehensive stream health check"""
    streams = spark.streams.active
    if stream_name:
        streams = [s for s in streams if s.name == stream_name]
    
    health_report = []
    
    for stream in streams:
        status = stream.status
        progress = stream.lastProgress
        
        health = {
            "stream_name": stream.name,
            "status": status.get("message", "unknown"),
            "is_active": status.get("isTriggerActive", False)
        }
        
        if progress:
            # Input/processing rates
            health["input_rate"] = progress.get('inputRowsPerSecond', 0)
            health["processing_rate"] = progress.get('processedRowsPerSecond', 0)
            health["is_healthy"] = health["processing_rate"] >= health["input_rate"]
            
            # Batch duration
            duration_ms = progress.get("durationMs", {}).get("triggerExecution", 0)
            health["batch_duration_ms"] = duration_ms
            
            # Kafka lag
            sources = progress.get("sources", [])
            max_lag = 0
            for source in sources:
                end_offset = source.get("endOffset", {})
                latest_offset = source.get("latestOffset", {})
                for topic, partitions in end_offset.items():
                    for partition, end in partitions.items():
                        latest = latest_offset.get(topic, {}).get(partition, end)
                        lag = int(latest) - int(end)
                        max_lag = max(max_lag, lag)
            health["max_lag"] = max_lag
            
            # State metrics
            if "stateOperators" in progress:
                state_ops = progress["stateOperators"]
                total_state_rows = sum([op.get('numRowsTotal', 0) for op in state_ops])
                health["state_rows"] = total_state_rows
            
            # Watermark
            if "eventTime" in progress:
                health["watermark"] = progress["eventTime"].get("watermark", "N/A")
        
        health_report.append(health)
    
    return health_report

# Run health check
health = check_stream_health()
for h in health:
    print(f"{h['stream_name']}: {'✓' if h.get('is_healthy', False) else '✗'}")
```

### Real-Time Monitoring Dashboard

```python
import time
from datetime import datetime

def monitor_streams_continuous(interval_seconds=60):
    """Continuous monitoring loop"""
    while True:
        print(f"\n=== Stream Health Check {datetime.now()} ===")
        
        for stream in spark.streams.active:
            progress = stream.lastProgress
            if progress:
                input_rate = progress.get('inputRowsPerSecond', 0)
                processing_rate = progress.get('processedRowsPerSecond', 0)
                
                print(f"\nStream: {stream.name}")
                print(f"  Input: {input_rate:.2f} rows/sec")
                print(f"  Processing: {processing_rate:.2f} rows/sec")
                print(f"  Health: {'✓' if processing_rate >= input_rate else '✗'}")
        
        time.sleep(interval_seconds)

# Run in background thread
import threading
monitor_thread = threading.Thread(target=monitor_streams_continuous, daemon=True)
monitor_thread.start()
```

## Alerting Patterns

### Alert on Falling Behind

```python
def alert_on_lag(stream, lag_threshold=10000):
    """Alert if stream lag exceeds threshold"""
    progress = stream.lastProgress
    if progress:
        sources = progress.get("sources", [])
        for source in sources:
            end_offset = source.get("endOffset", {})
            latest_offset = source.get("latestOffset", {})
            
            max_lag = 0
            for topic, partitions in end_offset.items():
                for partition, end in partitions.items():
                    latest = latest_offset.get(topic, {}).get(partition, end)
                    lag = int(latest) - int(end)
                    max_lag = max(max_lag, lag)
            
            if max_lag > lag_threshold:
                send_alert(
                    f"Stream {stream.name} lag: {max_lag} (threshold: {lag_threshold})",
                    severity="warning"
                )

# Check all streams
for stream in spark.streams.active:
    alert_on_lag(stream)
```

### Alert on Processing Rate

```python
def alert_on_processing_rate(stream):
    """Alert if processing rate < input rate"""
    progress = stream.lastProgress
    if progress:
        input_rate = progress.get('inputRowsPerSecond', 0)
        processing_rate = progress.get('processedRowsPerSecond', 0)
        
        if processing_rate < input_rate:
            send_alert(
                f"Stream {stream.name} falling behind: "
                f"Processing {processing_rate:.2f} < Input {input_rate:.2f} rows/sec",
                severity="critical"
            )

for stream in spark.streams.active:
    alert_on_processing_rate(stream)
```

### Alert on Batch Duration

```python
def alert_on_batch_duration(stream, trigger_interval_ms):
    """Alert if batch duration exceeds trigger interval"""
    progress = stream.lastProgress
    if progress:
        duration_ms = progress.get("durationMs", {}).get("triggerExecution", 0)
        
        if duration_ms > trigger_interval_ms:
            send_alert(
                f"Stream {stream.name} batch duration {duration_ms}ms "
                f"exceeds trigger interval {trigger_interval_ms}ms",
                severity="warning"
            )

for stream in spark.streams.active:
    # Get trigger interval from stream configuration
    alert_on_batch_duration(stream, trigger_interval_ms=30000)
```

## Spark UI Navigation

### Key Tabs to Monitor

1. **Streaming Tab**
   - Input rate vs processing rate
   - Max offsets behind latest
   - Batch duration
   - Active queries

2. **SQL Tab**
   - Query execution plans
   - Look for BroadcastHashJoin (efficient)
   - Check for shuffle operations
   - Monitor task skew

3. **Jobs Tab**
   - Job duration
   - Task distribution
   - Shuffle read/write
   - Spill to disk (should be 0)

4. **Stages Tab**
   - Task execution times
   - Data skew indicators
   - Failed tasks

### Key Metrics in Spark UI

- **Input Rate**: Messages per second from source
- **Processing Rate**: Must be >= Input Rate
- **Max Offsets Behind Latest**: Should decrease over time
- **Batch Duration**: Should be < trigger interval
- **State Rows**: Monitor for growth
- **Shuffle Spill**: Should be 0

## Common Monitoring Patterns

### Pattern 1: Stream Health Dashboard

```python
def create_health_dashboard():
    """Create stream health dashboard"""
    health_data = []
    
    for stream in spark.streams.active:
        progress = stream.lastProgress
        if progress:
            health_data.append({
                "stream": stream.name,
                "input_rate": progress.get('inputRowsPerSecond', 0),
                "processing_rate": progress.get('processedRowsPerSecond', 0),
                "lag": calculate_max_lag(progress),
                "batch_duration": progress.get("durationMs", {}).get("triggerExecution", 0),
                "status": "healthy" if is_healthy(progress) else "unhealthy"
            })
    
    # Create DataFrame for visualization
    health_df = spark.createDataFrame(health_data)
    health_df.display()  # Databricks display

create_health_dashboard()
```

### Pattern 2: Historical Metrics Tracking

```python
def track_historical_metrics():
    """Track metrics over time"""
    from pyspark.sql.functions import current_timestamp
    
    metrics = []
    
    for stream in spark.streams.active:
        progress = stream.lastProgress
        if progress:
            metrics.append({
                "timestamp": current_timestamp(),
                "stream_name": stream.name,
                "input_rate": progress.get('inputRowsPerSecond', 0),
                "processing_rate": progress.get('processedRowsPerSecond', 0),
                "lag": calculate_max_lag(progress),
                "batch_duration": progress.get("durationMs", {}).get("triggerExecution", 0)
            })
    
    # Write to metrics table
    if metrics:
        metrics_df = spark.createDataFrame(metrics)
        metrics_df.write.format("delta").mode("append").saveAsTable("streaming_metrics")

# Schedule this function to run periodically
```

## Production Checklist

- [ ] Input rate vs processing rate monitored
- [ ] Max offsets behind latest tracked
- [ ] Batch duration vs trigger interval monitored
- [ ] State store size tracked
- [ ] Alerting configured for critical metrics
- [ ] Spark UI navigation understood
- [ ] Historical metrics tracked
- [ ] Health dashboard available

## Related Skills

- `checkpoint-best-practices` - Checkpoint monitoring
- `state-store-management` - State store monitoring
- `error-handling-recovery` - Error monitoring and alerting
