---
name: streaming-backfill-patterns
description: Implement backfill and replay patterns for Spark Structured Streaming. Use when reprocessing historical data, fixing data quality issues, replaying events after code changes, or recovering from checkpoint loss.
---

# Backfill and Replay Patterns

Reprocess historical data or replay events from specific time ranges or offsets. Use batch reads for one-time backfills or streaming with specific offsets for incremental replay.

## Quick Start

```python
# Backfill specific Kafka offset range
backfill_df = (spark
    .read  # Batch read (faster for one-time backfills)
    .format("kafka")
    .option("startingOffsets", """{"topic": {"0": 10000, "1": 10000}}""")
    .option("endingOffsets", """{"topic": {"0": 20000, "1": 20000}}""")
    .load()
)

# Process and write
backfill_df.transform(process_logic).write.format("delta").save("target")
```

## Backfill Scenarios

### Scenario 1: Specific Offset Range (Kafka)

Backfill from specific Kafka offsets:

```python
# Backfill specific offset range
backfill_df = (spark
    .read  # Batch read for one-time backfill
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "topic")
    .option("startingOffsets", """{"topic": {"0": 10000, "1": 10000}}""")
    .option("endingOffsets", """{"topic": {"0": 20000, "1": 20000}}""")
    .load()
)

# Process as batch
processed = backfill_df.transform(process_logic)
processed.write.format("delta").mode("append").save("/delta/target")
```

### Scenario 2: Time-Based Backfill (Auto Loader)

Backfill Auto Loader from specific date:

```python
# Backfill Auto Loader from specific date
backfill_df = (spark
    .readStream  # Streaming for incremental backfill
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.startingFiles", "2024-01-01")
    .load("/path/to/files")
)

# Or use batch read for one-time backfill
backfill_df = (spark
    .read
    .format("json")
    .option("pathGlobFilter", "2024-01-*/*.json")
    .load("/path/to/files")
)
```

### Scenario 3: Full Replay

Replay all data from beginning:

```python
# Steps for full replay:
# 1. Stop stream
query.stop()

# 2. Delete checkpoint location
dbutils.fs.rm("/checkpoints/stream", recurse=True)

# 3. Restart with startingOffsets=earliest
df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/stream") \
    .option("startingOffsets", "earliest") \
    .start()

# 4. Delta sink handles deduplication (if idempotent writes configured)
```

## Backfill Patterns

### Pattern 1: Batch Backfill Job

Use batch reads for one-time backfills (faster):

```python
# Separate job for backfill (batch mode)
backfill_df = (spark
    .read  # Note: read (batch), not readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "topic")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

# Process as batch
processed = backfill_df.transform(process_logic)

# Write with idempotency
processed.write \
    .format("delta") \
    .mode("append") \
    .option("txnVersion", 1) \
    .option("txnAppId", "backfill_job") \
    .save("/delta/target")
```

### Pattern 2: Streaming Backfill with Offsets

Use streaming for incremental backfill:

```python
# Streaming backfill with specific offsets
backfill_stream = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "topic")
    .option("startingOffsets", """{"topic": {"0": 10000, "1": 10000}}""")
    .option("endingOffsets", """{"topic": {"0": 20000, "1": 20000}}""")
    .load()
)

# Process and write
backfill_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/backfill") \
    .trigger(availableNow=True) \  # Process all, then stop
    .start("/delta/target")
```

### Pattern 3: Time-Range Backfill

Backfill specific time range:

```python
# Filter by timestamp after reading
backfill_df = (spark
    .read
    .format("kafka")
    .option("subscribe", "topic")
    .option("startingOffsets", "earliest")
    .load()
    .filter(
        (col("timestamp") >= "2024-01-01") &
        (col("timestamp") < "2024-01-02")
    )
)

# Process and write
backfill_df.transform(process_logic).write.format("delta").save("target")
```

## Checkpoint Management

### Backup Before Backfill

```python
# Save checkpoint before major changes
def backup_checkpoint(checkpoint_path, backup_suffix):
    backup_path = f"{checkpoint_path}_backup_{backup_suffix}"
    dbutils.fs.cp(checkpoint_path, backup_path, recurse=True)
    return backup_path

# Before backfill
backup_path = backup_checkpoint("/checkpoints/stream", "20240101")
```

### Restore Checkpoint

```python
# Restore checkpoint if needed
def restore_checkpoint(backup_path, checkpoint_path):
    dbutils.fs.cp(backup_path, checkpoint_path, recurse=True)

# Restore from backup
restore_checkpoint("/checkpoints/stream_backup_20240101", "/checkpoints/stream")
```

## Idempotency

### Ensure Idempotent Writes

```python
# Use Delta with txnVersion for idempotency
backfill_df.write \
    .format("delta") \
    .mode("append") \
    .option("txnVersion", backfill_batch_id) \
    .option("txnAppId", "backfill_job") \
    .save("/delta/target")

# Or use MERGE with idempotent keys
spark.sql("""
    MERGE INTO target t
    USING backfill_source s ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT *
""")
```

## Best Practices

### Use Batch Reads for One-Time Backfills

```python
# FASTER: Batch read for one-time backfill
backfill_df = spark.read.format("kafka").load(...)

# SLOWER: Streaming read for one-time backfill
backfill_df = spark.readStream.format("kafka").load(...)
```

### Monitor Cluster Capacity

```python
# Backfills are bursty - monitor cluster capacity
# Consider separate cluster for large backfills
# Use larger cluster for backfill, smaller for streaming
```

### Verify Backfill Results

```python
# Use Delta time travel to verify backfill results
before_count = spark.read.format("delta").option("versionAsOf", before_version).load("/delta/target").count()
after_count = spark.read.format("delta").option("versionAsOf", after_version).load("/delta/target").count()
backfilled_count = after_count - before_count

print(f"Backfilled {backfilled_count} records")
```

## Common Patterns

### Pattern 1: Incremental Backfill

Backfill in chunks to avoid overwhelming the cluster:

```python
# Backfill in chunks
chunk_size = 10000
start_offset = 0
end_offset = 100000

for chunk_start in range(start_offset, end_offset, chunk_size):
    chunk_end = min(chunk_start + chunk_size, end_offset)
    
    chunk_df = (spark
        .read
        .format("kafka")
        .option("startingOffsets", f'{{"topic": {{"0": {chunk_start}}}}}')
        .option("endingOffsets", f'{{"topic": {{"0": {chunk_end}}}}}')
        .load()
    )
    
    chunk_df.transform(process_logic).write.format("delta").mode("append").save("target")
    print(f"Backfilled offsets {chunk_start} to {chunk_end}")
```

### Pattern 2: Backfill with Validation

Validate backfill results:

```python
def backfill_with_validation(start_offset, end_offset):
    # Backfill
    backfill_df = (spark
        .read
        .format("kafka")
        .option("startingOffsets", f'{{"topic": {{"0": {start_offset}}}}}')
        .option("endingOffsets", f'{{"topic": {{"0": {end_offset}}}}}')
        .load()
    )
    
    processed = backfill_df.transform(process_logic)
    
    # Validate before writing
    record_count = processed.count()
    if record_count == 0:
        print("WARNING: No records in backfill range")
        return
    
    # Write
    processed.write.format("delta").mode("append").save("target")
    
    # Verify
    written_count = spark.read.format("delta").load("target").filter(
        col("offset") >= start_offset & col("offset") < end_offset
    ).count()
    
    assert written_count == record_count, f"Count mismatch: {written_count} != {record_count}"
    print(f"Backfilled {record_count} records successfully")
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **Slow backfill** | Using streaming instead of batch | Use batch read for one-time backfills |
| **Duplicate data** | No idempotency configured | Use txnVersion/txnAppId or MERGE |
| **Cluster overload** | Too large backfill | Backfill in chunks; use separate cluster |
| **Checkpoint conflicts** | Backfill uses same checkpoint | Use separate checkpoint for backfill |

## Production Checklist

- [ ] Use batch reads for one-time backfills
- [ ] Checkpoint backup created before backfill
- [ ] Idempotent writes configured (txnVersion/txnAppId)
- [ ] Cluster capacity verified
- [ ] Backfill results validated
- [ ] Separate checkpoint for backfill (if using streaming)
- [ ] Monitoring configured for backfill progress

## Related Skills

- `checkpoint-best-practices` - Checkpoint management for backfills
- `kafka-to-delta` - Kafka ingestion patterns
- `error-handling-recovery` - Recovery procedures
