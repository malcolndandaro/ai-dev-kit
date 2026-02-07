---
name: kafka-to-delta
description: Ingest data from Kafka into Delta Lake using Spark Structured Streaming. Use when building bronze layer ingestion, implementing scheduled streaming jobs, handling schema evolution, or creating production Kafka-to-Delta pipelines with checkpoint management and monitoring.
---

# Kafka to Delta Streaming

Ingest data from Kafka topics into Delta Lake tables using Spark Structured Streaming. Supports continuous streaming, scheduled batch-style processing, and automatic schema evolution.

## Quick Start

```python
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType

# Read from Kafka
df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "topic_name")
    .option("startingOffsets", "earliest")  # or "latest"
    .option("minPartitions", "6")  # Match Kafka partitions
    .load()
)

# Parse JSON value
df_parsed = df.select(
    col("key").cast("string"),
    from_json(col("value").cast("string"), event_schema).alias("data"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
).select("key", "data.*", "topic", "partition", "offset", "kafka_timestamp")

# Write to Delta with checkpointing
query = (df_parsed
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/catalog/checkpoints/kafka_stream")
    .queryName("kafka_to_delta_bronze")  # Human-readable name
    .trigger(processingTime="30 seconds")
    .start("/delta/bronze_events")
)
```

## Core Concepts

### Streaming = Incremental Processing

Streaming doesn't mean continuous - it means incremental processing:

```python
# Continuous (runs forever)
.trigger(processingTime="30 seconds")

# Scheduled (processes backlog then stops)
.trigger(availableNow=True)

# Both use the same streaming API
```

### Checkpoint Semantics

Checkpoints track progress and enable exactly-once processing:

```python
# Checkpoint location tied to TARGET table
def get_checkpoint_location(table_name):
    """Checkpoint should be tied to TARGET, not source"""
    return f"/Volumes/catalog/checkpoints/{table_name}"

# Why target-tied? Checkpoint already contains source information
# Benefits: Systematic organization, easy backup/restore
```

## Common Patterns

### Pattern 1: Bronze Layer (Raw Ingestion)

Minimal transformation, preserve original columns:

```python
# Best practice: Minimal transformation, preserve original columns
# Why: Kafka retention is expensive (default 7 days)
# Delta provides permanent storage with full history

df_bronze = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", servers)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)  # Control batch size
    .load()
    .select(
        col("key").cast("string"),
        col("value").cast("string"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingestion_timestamp")
    )
)

# Write raw to bronze
df_bronze.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/Volumes/catalog/checkpoints/bronze_events") \
    .trigger(processingTime="30 seconds") \
    .start("/delta/bronze_events")
```

### Pattern 2: Scheduled Streaming (Cost-Optimized)

Run periodically instead of continuously:

```python
# Run every 4 hours, not continuously
# Same code, just change trigger in job scheduler

df_bronze.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/Volumes/catalog/checkpoints/bronze_events") \
    .trigger(availableNow=True) \  # Process all available, then stop
    .start("/delta/bronze_events")

# In Databricks Jobs:
# - Schedule: Every 4 hours
# - Cluster: Fixed size (no autoscaling for streaming)
# - Same streaming code, batch-style execution
```

### Pattern 3: Schema Evolution

Handle evolving schemas automatically:

```python
# Enable schema evolution
df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("mergeSchema", "true") \  # Allow schema changes
    .option("checkpointLocation", "/checkpoints/bronze_events") \
    .start("/delta/bronze_events")

# New columns are automatically added
# Existing columns maintain compatibility
```

### Pattern 4: Multi-Topic Ingestion

Read from multiple Kafka topics:

```python
# Option 1: Comma-separated topics
df = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "topic1,topic2,topic3")
    .load()
)

# Option 2: Topic pattern
df = (spark
    .readStream
    .format("kafka")
    .option("subscribePattern", "events-.*")  # All topics matching pattern
    .load()
)

# Add topic name to output
df_with_topic = df.select(
    col("topic"),
    col("key").cast("string"),
    col("value").cast("string"),
    col("partition"),
    col("offset"),
    col("timestamp")
)

df_with_topic.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/multi_topic") \
    .start("/delta/multi_topic_events")
```

### Pattern 5: Secure Credential Management

Use Databricks secrets for Kafka credentials:

```python
# Store secrets in Databricks secret scope
kafka_servers = dbutils.secrets.get("kafka-scope", "bootstrap-servers")
kafka_username = dbutils.secrets.get("kafka-scope", "username")
kafka_password = dbutils.secrets.get("kafka-scope", "password")

# SASL/PLAIN authentication
df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_servers)
    .option("subscribe", topic)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", 
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";')
    .load()
)
```

### Pattern 6: Error Handling with Dead Letter Queue

Route invalid records to DLQ:

```python
from pyspark.sql.functions import from_json, col, lit, to_json, struct, current_timestamp

def ingest_with_dlq(batch_df, batch_id):
    """Ingest valid records, route invalid to DLQ"""
    
    # Try to parse JSON
    parsed = batch_df.withColumn(
        "parsed",
        from_json(col("value").cast("string"), event_schema)
    )
    
    # Valid records
    valid = parsed.filter(col("parsed").isNotNull()).select(
        col("key"),
        col("parsed.*"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
    )
    
    # Invalid records → DLQ
    invalid = parsed.filter(col("parsed").isNull()).select(
        col("key"),
        col("value"),
        col("topic"),
        col("partition"),
        col("offset"),
        lit("PARSE_ERROR").alias("error_reason"),
        current_timestamp().alias("error_timestamp")
    )
    
    # Write valid to bronze
    if valid.count() > 0:
        (valid
            .write
            .format("delta")
            .mode("append")
            .option("txnVersion", batch_id)
            .option("txnAppId", "kafka_ingestion")
            .saveAsTable("bronze.events")
        )
    
    # Write invalid to DLQ
    if invalid.count() > 0:
        (invalid
            .write
            .format("delta")
            .mode("append")
            .saveAsTable("bronze.dlq_events")
        )

df.writeStream \
    .foreachBatch(ingest_with_dlq) \
    .option("checkpointLocation", "/checkpoints/bronze_events") \
    .start()
```

## Checkpoint Management

### Checkpoint Structure

```
checkpoint_location/
├── metadata/          # Stream query ID
├── offsets/           # Intent: what to process
│   ├── 0
│   ├── 1
│   └── ...
├── commits/           # Confirmation: what completed
│   ├── 0
│   ├── 1
│   └── ...
└── sources/           # Source metadata
    └── 0/
        └── 0
```

### Offset Semantics

```python
# Offset file structure:
{
  "startOffset": {"topic": {"0": 100}},  # Inclusive
  "endOffset": {"topic": {"0": 200}}     # Exclusive
}

# Processes: 100, 101, ..., 199
# Does NOT process: 200
# Next batch starts at: 200
```

### Recovery After Failure

```python
# When stream restarts:
# 1. Read latest offset file (e.g., offset 223)
# 2. Check if commit 223 exists
# 3. If yes: proceed to offset 224
# 4. If no: reprocess offset 223 (exactly-once guarantee)

# Delta handles duplicate detection via:
# - Query ID (from metadata)
# - Epoch ID (batch ID)
# - Check Delta log before writing
```

## Performance Tuning

### Key Parameters

| Parameter | Recommendation | Why |
|-----------|---------------|-----|
| minPartitions | Match Kafka partitions | Optimal parallelism |
| maxOffsetsPerTrigger | 10,000-100,000 | Balance latency vs throughput |
| shufflePartitions | 200 (default) | Usually fine |
| trigger interval | Business SLA / 3 | Recovery time buffer |

### Configuration

```python
# Match Kafka partitions for optimal parallelism
.option("minPartitions", "6")  # If Kafka has 6 partitions

# Control batch size
.option("maxOffsetsPerTrigger", "10000")

# Shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Trigger interval (rule: SLA / 3)
# Example: 1 hour SLA → 20 minute trigger
.trigger(processingTime="20 minutes")
```

## Monitoring

### Key Metrics

```python
# Programmatic monitoring
for stream in spark.streams.active:
    status = stream.status
    progress = stream.lastProgress
    
    if progress:
        print(f"Stream: {stream.name}")
        print(f"Status: {status['message']}")
        print(f"Input rate: {progress.get('inputRowsPerSecond', 0)} rows/sec")
        print(f"Processing rate: {progress.get('processedRowsPerSecond', 0)} rows/sec")
        
        # Kafka-specific metrics
        sources = progress.get("sources", [])
        for source in sources:
            end_offset = source.get("endOffset", {})
            latest_offset = source.get("latestOffset", {})
            
            # Calculate lag per partition
            for topic, partitions in end_offset.items():
                for partition, end in partitions.items():
                    latest = latest_offset.get(topic, {}).get(partition, end)
                    lag = int(latest) - int(end)
                    print(f"Topic {topic}, Partition {partition}: Lag = {lag}")
```

### Spark UI Checks

- **Input Rate vs Processing Rate**: Processing must be > Input
- **Max Offsets Behind Latest**: Should be consistent or dropping
- **Batch Duration**: Should be < trigger interval
- **Checkpoint Size**: Monitor for growth

### Alerting

```python
# Key metrics to alert on:
# - Max Offsets Behind Latest (increasing = problem)
# - Batch Duration > Trigger Interval
# - Input Rate >> Processing Rate
# - Checkpoint access failures
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **No data being read** | `startingOffsets` default is "latest" | Use "earliest" for existing data |
| **Falling behind** | Processing < Input rate | Increase cluster size; reduce maxOffsetsPerTrigger |
| **Small files problem** | Trigger too frequent | Increase trigger interval (e.g., 2 minutes) |
| **Duplicate data after restart** | Checkpoint deleted | Don't delete checkpoints; Delta handles deduplication |
| **Can't use autoscaling** | Streaming requirement | Use fixed-size clusters (no autoscaling) |
| **Kafka retention expired** | Data not ingested in time | Ingest to Delta immediately; Kafka is temporary |
| **Schema evolution errors** | Schema changes not handled | Enable `mergeSchema` option |

## Production Best Practices

### Checkpoint Best Practices

```python
# Create a function for consistent checkpoint locations
def get_checkpoint_location(table_name):
    """Checkpoint should be tied to TARGET, not source"""
    return f"/Volumes/catalog/checkpoints/{table_name}"

# Example:
# Table: prod.analytics.orders
# Checkpoint: /Volumes/prod/checkpoints/orders

# Why target-tied? Checkpoint already contains source information
# Benefits: All checkpoints in one place, systematic organization
```

### Idempotent Writes

```python
# Delta automatically handles idempotency via checkpoint
# For ForEachBatch, use transaction version:

def idempotent_write(batch_df, batch_id):
    (batch_df
        .write
        .format("delta")
        .mode("append")
        .option("txnVersion", batch_id)
        .option("txnAppId", "kafka_ingestion")
        .saveAsTable("bronze.events")
    )
```

### Cluster Configuration

```python
# Fixed-size cluster recommended for streaming
# - No autoscaling (streaming requirement)
# - Right-size based on throughput requirements
# - Monitor CPU/memory utilization

# Tested: Up to 100 streams on 8-core single-node cluster
# Monitor CPU/memory per stream
```

## Production Checklist

- [ ] Checkpoint location is persistent (S3/ADLS, not DBFS)
- [ ] Unique checkpoint per stream
- [ ] Target-tied checkpoint organization
- [ ] Fixed-size cluster (no autoscaling for streaming)
- [ ] Monitoring configured (input rate, lag, batch duration)
- [ ] Alerting for falling behind
- [ ] Recovery procedure documented
- [ ] Exactly-once verified (Delta handles automatically)
- [ ] Schema evolution enabled if needed
- [ ] Security configured (SASL/SSL for production Kafka)

## Related Skills

- `kafka-to-kafka` - Kafka-to-Kafka streaming pipelines
- `stream-static-joins` - Enrichment patterns after ingestion
- `checkpoint-best-practices` - Deep dive on checkpoint management
- `write-multiple-tables` - Fan-out patterns for multiple sinks
