---
name: state-store-management
description: Manage state stores for Spark Structured Streaming stateful operations. Use when configuring RocksDB for large state, monitoring state store size and partition balance, optimizing state performance, or recovering from state store issues.
---

# State Store Management

Manage state stores for stateful streaming operations. Configure RocksDB for large state, monitor state size and partition balance, and optimize state performance.

## Quick Start

```python
# Enable RocksDB for large state stores
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateProvider"
)

# Stateful operation with watermark
df = (spark.readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"])  # Stateful operation
)
```

## State Store Configuration

### Enable RocksDB

Use RocksDB for state stores exceeding memory capacity:

```python
# Enable RocksDB state store provider
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateProvider"
)

# Benefits:
# - State stored on disk, reducing memory pressure
# - Recommended for: High cardinality keys, long watermark durations
# - Better performance for large state stores
```

### State Store Configuration

```python
# State store batch retention
spark.conf.set("spark.sql.streaming.stateStore.minBatchesToRetain", "2")

# State maintenance interval
spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "5m")

# State store location (default: checkpoint/state)
# Automatically managed by Spark
```

## Monitoring State Stores

### Read State Store Directly

```python
# Query state store directly
state_df = (spark
    .read
    .format("statestore")
    .load("/checkpoints/stream/state")
)

state_df.show()
# Shows: key, value, partitionId, expiration timestamp

# Check partition balance
partition_balance = state_df.groupBy("partitionId").count().orderBy(desc("count"))
partition_balance.show()

# Look for skew - one partition with 10x others = problem
```

### Read State Metadata

```python
# Read state metadata
state_metadata = (spark
    .read
    .format("state-metadata")
    .load("/checkpoints/stream")
)
state_metadata.show()
# Shows: operatorName, numPartitions, minBatchId, maxBatchId
```

### Programmatic Monitoring

```python
# Monitor state size programmatically
for stream in spark.streams.active:
    progress = stream.lastProgress
    
    if progress and "stateOperators" in progress:
        for op in progress["stateOperators"]:
            print(f"Operator: {op.get('operatorName', 'unknown')}")
            print(f"State rows: {op.get('numRowsTotal', 0)}")
            print(f"State memory: {op.get('memoryUsedBytes', 0)}")
            print(f"State on disk: {op.get('diskBytesUsed', 0)}")
```

## State Size Control

### Use Watermarks

Watermarks automatically clean up expired state:

```python
# State expires after watermark duration
.withWatermark("event_time", "10 minutes")

# State size = f(watermark duration, key cardinality)
# 10 min watermark × 1M events/min = manageable
# 72 hour watermark × 1M events/min = very large
```

### Reduce Key Cardinality

```python
# Bad: High cardinality keys
.dropDuplicates(["user_id"])  # Millions of distinct values

# Good: Lower cardinality or expiring keys
.dropDuplicates(["session_id"])  # Sessions expire naturally
.dropDuplicates(["event_id", "date"])  # Partition by date reduces cardinality
```

### State TTL Configuration

```python
# For mapGroupsWithState, configure TTL
def state_function(key, values, state):
    # State expires after TTL
    if state.hasTimedOut():
        return None
    # Process state
    return result

# Configure state timeout
df.groupByKey(...).mapGroupsWithState(
    GroupStateTimeout.ProcessingTimeTimeout,
    state_function,
    outputMode="update"
)
```

## Common Patterns

### Pattern 1: Monitor State Partition Balance

Check for state store skew:

```python
def check_state_balance(checkpoint_path):
    """Check state store partition balance"""
    state_df = spark.read.format("statestore").load(f"{checkpoint_path}/state")
    
    partition_counts = state_df.groupBy("partitionId").count().orderBy(desc("count"))
    partition_counts.show()
    
    # Calculate skew
    counts = [row['count'] for row in partition_counts.collect()]
    if counts:
        max_count = max(counts)
        min_count = min(counts)
        skew_ratio = max_count / min_count if min_count > 0 else float('inf')
        
        print(f"State skew ratio: {skew_ratio:.2f}")
        if skew_ratio > 10:
            print("WARNING: High state skew detected")
            return False
    return True

# Check balance
check_state_balance("/checkpoints/stream")
```

### Pattern 2: Monitor State Growth

Track state store growth over time:

```python
def monitor_state_growth(checkpoint_path):
    """Track state store growth"""
    state_df = spark.read.format("statestore").load(f"{checkpoint_path}/state")
    
    # Current state size
    total_rows = state_df.count()
    total_memory = state_df.agg(sum("value").cast("long")).collect()[0][0]
    
    print(f"State rows: {total_rows}")
    print(f"Estimated memory: {total_memory / (1024*1024):.2f} MB")
    
    # Check expiration
    from pyspark.sql.functions import current_timestamp, col
    expired = state_df.filter(col("expirationMs") < current_timestamp().cast("long") * 1000)
    expired_count = expired.count()
    
    print(f"Expired state rows: {expired_count}")
    print(f"Active state rows: {total_rows - expired_count}")

monitor_state_growth("/checkpoints/stream")
```

### Pattern 3: State Store Recovery

Recover from state store issues:

```python
# Scenario 1: State store corruption
# Solution: Delete state folder, restart stream
# State will rebuild from watermark

dbutils.fs.rm("/checkpoints/stream/state", recurse=True)

# Restart stream - state rebuilds automatically
# Note: May reprocess some data within watermark window

# Scenario 2: State store too large
# Solution: Reduce watermark duration
.withWatermark("event_time", "5 minutes")  # Reduced from 10 minutes

# Scenario 3: State partition imbalance
# Solution: Ensure keys are evenly distributed
# Consider salting keys if needed
```

## Performance Optimization

### RocksDB Configuration

```python
# RocksDB is automatically configured for optimal performance
# No manual tuning typically needed

# Monitor RocksDB metrics:
# - Disk usage
# - Read/write performance
# - Compaction activity
```

### State Store Tuning

```python
# State store batch retention
spark.conf.set("spark.sql.streaming.stateStore.minBatchesToRetain", "2")

# State maintenance interval
spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "5m")

# Adjust based on:
# - State size
# - Checkpoint frequency
# - Recovery requirements
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **State too large** | High cardinality keys or long watermark | Reduce key cardinality; decrease watermark duration |
| **State partition skew** | Uneven key distribution | Ensure keys are evenly distributed; consider salting |
| **OOM errors** | State exceeds memory | Enable RocksDB; increase memory; reduce watermark |
| **Slow state operations** | Large state store | Enable RocksDB; optimize state queries |
| **State not expiring** | Watermark not configured | Add watermark to stateful operations |

## Production Best Practices

### Always Use Watermarks

```python
# REQUIRED: Watermark for stateful operations
df.withWatermark("event_time", "10 minutes").dropDuplicates(["id"])

# State automatically expires after watermark
# Prevents infinite state growth
```

### Monitor State Size

```python
# Regular monitoring
def monitor_state(checkpoint_path):
    state_df = spark.read.format("statestore").load(f"{checkpoint_path}/state")
    row_count = state_df.count()
    
    # Alert if state exceeds threshold
    if row_count > 10_000_000:  # 10M rows
        send_alert(f"State size: {row_count} rows")
    
    return row_count

# Schedule monitoring job
```

### Use RocksDB for Large State

```python
# Enable RocksDB if state > memory capacity
# Typical threshold: > 100M keys or > 10GB state

spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateProvider"
)
```

## Production Checklist

- [ ] RocksDB enabled for large state stores
- [ ] Watermark configured for state cleanup
- [ ] State size monitored and alerts configured
- [ ] State partition balance checked regularly
- [ ] State growth tracked over time
- [ ] Recovery procedure documented

## Related Skills

- `watermark-configuration` - Watermark tuning for state cleanup
- `stream-stream-joins` - State management in joins
- `checkpoint-best-practices` - Checkpoint and state recovery
