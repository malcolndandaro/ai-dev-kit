---
name: spark-structured-streaming
description: Comprehensive guide to Spark Structured Streaming for production workloads. Use when building streaming pipelines, implementing real-time data processing, handling stateful operations, or optimizing streaming performance.
---

# Spark Structured Streaming

Build production-ready streaming pipelines with Spark Structured Streaming. This skill provides an overview and navigation to detailed patterns and best practices.

## Quick Start

```python
from pyspark.sql.functions import col, from_json

# Basic Kafka to Delta streaming
df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "topic")
    .option("startingOffsets", "earliest")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/Volumes/catalog/checkpoints/stream") \
    .trigger(processingTime="30 seconds") \
    .start("/delta/target_table")
```

## Core Concepts

### Streaming = Incremental Processing

Streaming doesn't mean continuous - it means incremental processing:

```python
# Continuous (runs forever)
.trigger(processingTime="30 seconds")

# Scheduled (processes backlog then stops)
.trigger(availableNow=True)  # Schedule via Jobs

# Both use the same streaming API
```

### Exactly-Once Semantics

Achieved via checkpoint + Delta idempotent writes:

```python
# Checkpoint tracks progress
.option("checkpointLocation", "/checkpoints/stream")

# Delta handles deduplication
.option("txnVersion", batch_id)
.option("txnAppId", "stream_job")
```

## End-to-End Patterns

### Stream-Stream Joins

Join two streaming sources with event-time semantics:

```python
# See: stream-stream-joins
stream1.withWatermark("ts", "10 min").join(
    stream2.withWatermark("ts", "10 min"),
    join_condition,
    "inner"
)
```

### Stream-Static Joins

Enrich streams with Delta dimension tables:

```python
# See: stream-static-joins
stream.join(dim_table, "key", "left")  # Left join recommended
```

### Kafka to Kafka

Build low-latency Kafka-to-Kafka pipelines:

```python
# See: kafka-to-kafka
source_df.writeStream.format("kafka").option("topic", "output").start()
```

### Kafka to Delta

Ingest Kafka into Delta Lake:

```python
# See: kafka-to-delta
kafka_df.writeStream.format("delta").start("/delta/target")
```

### Write to Multiple Tables

Fan out single stream to multiple sinks:

```python
# See: write-multiple-tables
def write_multiple(batch_df, batch_id):
    batch_df.write.saveAsTable("table1")
    batch_df.filter(...).write.saveAsTable("table2")

stream.writeStream.foreachBatch(write_multiple).start()
```

### Merge into Multiple Tables

Parallel MERGE operations:

```python
# See: merge-multiple-tables-parallel
# Enable Liquid Clustering + DV + RLC for conflict-free merges
```

## Configuration and Management

### Checkpoint Management

```python
# See: checkpoint-best-practices
def get_checkpoint_location(table_name):
    return f"/Volumes/catalog/checkpoints/{table_name}"
```

### Watermark Configuration

```python
# See: watermark-configuration
.withWatermark("event_time", "10 minutes")  # Late data threshold
```

### State Store Management

```python
# See: state-store-management
# Enable RocksDB for large state stores
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "com.databricks.sql.streaming.state.RocksDBStateProvider")
```

### Trigger Tuning

```python
# See: trigger-tuning
.trigger(processingTime="30 seconds")  # Continuous
.trigger(availableNow=True)            # Scheduled
.trigger(realTime=True)                # Sub-second latency
```

## Performance Optimization

### Partitioning Strategy

```python
# See: partitioning-strategy
# Time-based partitioning or Liquid Clustering
.partitionBy("date")  # or CLUSTER BY (user_id)
```

### Merge Performance

```python
# See: merge-performance
# Enable Liquid Clustering + DV + RLC
ALTER TABLE target SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = true,
    'delta.enableRowLevelConcurrency' = true,
    'delta.liquid.clustering' = true
);
```

### Cost Tuning

```python
# See: cost-tuning
# Use scheduled streaming, multi-stream clusters, right-sizing
```

## Operations

### Monitoring and Observability

```python
# See: monitoring-observability
# Track input rate, processing rate, lag, batch duration
for stream in spark.streams.active:
    progress = stream.lastProgress
    # Monitor key metrics
```

### Error Handling and Recovery

```python
# See: error-handling-recovery
# Dead letter queues, exception handling, checkpoint recovery
def write_with_dlq(batch_df, batch_id):
    try:
        valid.write.saveAsTable("target")
    except Exception as e:
        invalid.write.saveAsTable("dlq")
```

### Backfill Patterns

```python
# See: backfill-patterns
# Reprocess historical data or specific time ranges
backfill_df = spark.read.format("kafka").option("startingOffsets", ...).load()
```

## Ingestion

### Auto Loader Schema Drift

```python
# See: auto-loader-schema-drift
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
.option("rescuedDataColumn", "_rescued_data")
```

### DLT vs Jobs

```python
# See: dlt-vs-jobs
# Choose between Delta Live Tables and Databricks Jobs
```

## Governance

### Unity Catalog Integration

```python
# See: unity-catalog-streaming
# Unified access control, lineage tracking, audit logging
checkpoint_path = "/Volumes/catalog/checkpoints/stream"
df.writeStream.start("catalog.schema.table")
```

## Production Checklist

- [ ] Checkpoint location is persistent (UC volumes, not DBFS)
- [ ] Unique checkpoint per stream
- [ ] Target-tied checkpoint organization
- [ ] Fixed-size cluster (no autoscaling for streaming)
- [ ] Monitoring configured (input rate, lag, batch duration)
- [ ] Alerting for falling behind
- [ ] Recovery procedure documented
- [ ] Exactly-once verified (txnVersion/txnAppId)
- [ ] Watermark configured for stateful operations
- [ ] Left joins for stream-static (not inner)

## Related Skills

- `kafka-to-delta` - Kafka ingestion patterns
- `stream-stream-joins` - Event correlation
- `stream-static-joins` - Dimension enrichment
- `checkpoint-best-practices` - Checkpoint management
- `watermark-configuration` - Late data handling
- `monitoring-observability` - Stream health monitoring
