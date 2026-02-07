---
name: "kafka-to-kafka-streaming-realtime"
description: "End-to-end guide for streaming from Kafka to Kafka using Spark Structured Streaming with Real-Time Mode (RTM) for sub-second latency pipelines."
author: "Databricks Expert"
date: "2025-02-07"
tags: ["spark-streaming", "kafka", "real-time-mode", "rtm", "sub-second", "streaming-pipeline"]
---

# Kafka-to-Kafka Streaming with Real-Time Mode

## Overview

Build low-latency streaming pipelines that read from Kafka, process in Spark, and write back to Kafka. Use Real-Time Mode (RTM) for sub-second latency requirements.

**Key Use Cases:**
- Event enrichment (add context to events in real-time)
- Format transformation (Avro → JSON, protobuf cleanup)
- Filtering and routing (high-priority events to separate topic)
- Real-time validation (schema validation, data quality checks)

**Latency Targets:**
- Microbatch (default): 1-30 seconds
- **Real-Time Mode (RTM)**: < 800ms (requires Photon)

## Quick Start

### Basic Kafka-to-Kafka Pipeline

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1. Read from source Kafka
source_df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "input-events")
    .option("startingOffsets", "latest")
    .option("minPartitions", "6")
    .load()
)

# 2. Parse and transform
parsed_df = source_df.select(
    col("key").cast("string"),
    from_json(col("value").cast("string"), event_schema).alias("data"),
    col("topic").alias("source_topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
).select("key", "data.*", "source_topic", "partition", "offset", "kafka_timestamp")

# 3. Apply business logic
enriched_df = parsed_df.withColumn(
    "processed_at", current_timestamp()
).withColumn(
    "value", to_json(struct(
        col("event_id"),
        col("user_id"),
        col("event_type"),
        col("processed_at"),
        col("kafka_timestamp")
    ))
)

# 4. Write to output Kafka topic
query = (enriched_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("topic", "output-events")
    .option("checkpointLocation", "/Volumes/catalog/checkpoints/kafka-to-kafka-pipeline")
    .start()
)
```

## Real-Time Mode (RTM) Configuration

### When to Use RTM

| Latency Requirement | Mode | Photon Required |
|---------------------|------|-----------------|
| < 800ms | RTM | Yes |
| 1-30 seconds | Microbatch | Optional |
| > 30 seconds | Microbatch | No |

### Enabling RTM

```python
# Option 1: Real-time trigger (Databricks 13.3+)
query = (enriched_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("topic", "output-events")
    .trigger(realTime=True)  # Enable RTM
    .option("checkpointLocation", checkpoint_path)
    .start()
)

# Option 2: Continuous trigger (legacy, still supported)
query = (enriched_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("topic", "output-events")
    .trigger(continuous="1 second")  # Legacy continuous mode
    .option("checkpointLocation", checkpoint_path)
    .start()
)
```

### RTM Cluster Requirements

```python
# Required configurations for RTM
spark.conf.set("spark.databricks.photon.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
               "com.databricks.sql.streaming.state.RocksDBStateProvider")

# Fixed-size cluster recommended
# - Driver: Minimum 4 cores
# - Workers: Fixed size (no autoscaling for RTM)
# - Enable Photon
```

## Common Patterns

### Pattern 1: Event Enrichment (Stream-Static Join)

```python
# Read reference data (Delta table - auto-refreshed each microbatch)
user_dim = spark.table("users.dimension")

# Stream-static join for enrichment
enriched = (parsed_df
    .join(user_dim, "user_id", "left")
    .withColumn("enriched_value", to_json(struct(
        col("event_id"),
        col("user_id"),
        col("user_name"),  # From dimension table
        col("user_segment"),  # From dimension table
        col("event_type"),
        col("timestamp")
    )))
)

# Write enriched events to Kafka
(enriched
    .select(col("key"), col("enriched_value").alias("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("topic", "enriched-events")
    .trigger(realTime=True)
    .option("checkpointLocation", "/checkpoints/enrichment")
    .start()
)
```

### Pattern 2: Event Filtering and Routing

```python
# Route to different topics based on event type
def route_events(batch_df, batch_id):
    """Route events to different Kafka topics"""
    
    # High priority → urgent topic
    high_priority = batch_df.filter(col("priority") == "high")
    if high_priority.count() > 0:
        (high_priority
            .select("key", "value")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("topic", "urgent-events")
            .save()
        )
    
    # Errors → DLQ topic
    errors = batch_df.filter(col("event_type") == "error")
    if errors.count() > 0:
        (errors
            .select("key", "value")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("topic", "error-events")
            .save()
        )
    
    # All events → standard topic
    (batch_df
        .select("key", "value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("topic", "standard-events")
        .save()
    )

# Use ForEachBatch for multi-topic routing
(parsed_df
    .writeStream
    .foreachBatch(route_events)
    .trigger(realTime=True)
    .option("checkpointLocation", "/checkpoints/routing")
    .start()
)
```

### Pattern 3: Stream-Stream Join (Event Correlation)

```python
# Read two Kafka topics
orders = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "orders")
    .load()
    .select(from_json(col("value").cast("string"), order_schema).alias("o"))
    .select("o.*")
    .withWatermark("order_time", "10 minutes")
)

payments = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "payments")
    .load()
    .select(from_json(col("value").cast("string"), payment_schema).alias("p"))
    .select("p.*")
    .withWatermark("payment_time", "10 minutes")
)

# Join orders with payments
matched = (orders
    .join(
        payments,
        expr("""
            order_id = payment_id AND
            payment_time >= order_time - interval 5 minutes AND
            payment_time <= order_time + interval 10 minutes
        """),
        "leftOuter"
    )
    .withColumn("joined_value", to_json(struct("*")))
)

# Output matched events
(matched
    .select(col("order_id").cast("string").alias("key"), 
            col("joined_value").alias("value"))
    .writeStream
    .format("kafka")
    .option("topic", "order-payments-matched")
    .trigger(processingTime="5 seconds")  # RTM not recommended for stateful joins
    .option("checkpointLocation", "/checkpoints/join-pipeline")
    .start()
)
```

### Pattern 4: Schema Validation and Dead Letter Queue

```python
# Schema validation with DLQ
validated_schema = StructType([...])

def validate_and_route(batch_df, batch_id):
    """Validate schema, route bad records to DLQ"""
    from pyspark.sql.functions import from_json
    
    # Try to parse with strict schema
    parsed = batch_df.withColumn(
        "parsed",
        from_json(col("value").cast("string"), validated_schema)
    )
    
    # Valid records
    valid = parsed.filter(col("parsed").isNotNull())
    
    # Invalid records → DLQ
    invalid = parsed.filter(col("parsed").isNull()).withColumn(
        "dlq_reason", lit("SCHEMA_VALIDATION_FAILED")
    ).withColumn(
        "dlq_timestamp", current_timestamp()
    )
    
    # Write valid to main topic
    (valid
        .select("key", "value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("topic", "valid-events")
        .save()
    )
    
    # Write invalid to DLQ
    (invalid
        .select(
            col("key"),
            to_json(struct("value", "dlq_reason", "dlq_timestamp")).alias("value")
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("topic", "dlq-events")
        .save()
    )

(source_df
    .writeStream
    .foreachBatch(validate_and_route)
    .trigger(realTime=True)
    .option("checkpointLocation", "/checkpoints/validation")
    .start()
)
```

### Pattern 5: Aggregations with Windowed Output

```python
# Real-time aggregations → Kafka metrics topic
windowed_metrics = (parsed_df
    .withWatermark("timestamp", "5 minutes")
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("event_type")
    )
    .agg(
        count("*").alias("event_count"),
        avg("value").alias("avg_value"),
        max("timestamp").alias("last_seen")
    )
    .withColumn("metric_payload", to_json(struct("*")))
)

# Note: Use Update or Complete output mode for aggregations
(windowed_metrics
    .select(
        concat(col("event_type"), lit("-"), col("window.start").cast("string")).alias("key"),
        col("metric_payload").alias("value")
    )
    .writeStream
    .outputMode("update")  # Only changed windows
    .format("kafka")
    .option("topic", "metrics-output")
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", "/checkpoints/metrics")
    .start()
)
```

## Kafka Configuration Reference

### Producer Options (Writing to Kafka)

```python
(df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:9092,host2:9092")
    .option("topic", "target-topic")
    # Producer configs
    .option("kafka.acks", "all")  # Durability: all, 1, 0
    .option("kafka.retries", "3")
    .option("kafka.batch.size", "16384")
    .option("kafka.linger.ms", "5")
    .option("kafka.compression.type", "lz4")  # lz4, snappy, gzip
    .option("kafka.max.in.flight.requests.per.connection", "5")
    .start()
)
```

### Consumer Options (Reading from Kafka)

```python
(spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:9092,host2:9092")
    .option("subscribe", "source-topic")
    # Consumer configs
    .option("startingOffsets", "latest")  # latest, earliest, or specific
    .option("maxOffsetsPerTrigger", "10000")  # Control batch size
    .option("minPartitions", "6")  # Match Kafka partitions
    .option("kafka.auto.offset.reset", "latest")
    .option("kafka.enable.auto.commit", "false")  # Spark manages offsets
    .load()
)
```

### Security (SASL/SSL)

```python
# SASL/PLAIN Authentication
(df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("topic", target_topic)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", 
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" password="{password}";')
    .option("checkpointLocation", checkpoint_path)
    .start()
)
```

## Monitoring Kafka Streams

### Key Metrics

```python
# Programmatic monitoring
for stream in spark.streams.active:
    status = stream.status
    progress = stream.lastProgress
    
    if progress:
        # Input rate
        input_rows = progress.get("numInputRows", 0)
        
        # Processing rate
        duration_ms = progress.get("durationMs", {}).get("triggerExecution", 0)
        
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

### Spark UI Metrics

- **Input Rate**: Messages per second
- **Processing Rate**: Must be >= Input Rate
- **Max Offsets Behind Latest**: Should decrease over time
- **Batch Duration**: Should be < trigger interval

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **High latency** | Microbatch overhead | Use RTM (trigger(realTime=True)) |
| **Consumer lag** | Processing < Input rate | Scale cluster; reduce maxOffsetsPerTrigger |
| **Duplicate messages** | Exactly-once not configured | Enable idempotent producer |
| **Schema mismatch** | Evolving schemas | Use schema registry (Confluent) |
| **OOM on driver** | Too many partitions | Reduce minPartitions; increase driver memory |
| **Small batches** | Trigger too frequent | Increase processingTime interval |

## Best Practices

1. **Use RTM only when needed** — Microbatch is more cost-effective for >1s latency
2. **Fixed-size clusters** — No autoscaling for streaming/RTM
3. **One checkpoint per pipeline** — Never share checkpoints
4. **Monitor consumer lag** — Alert when increasing
5. **Use left joins** — For stream-static joins to prevent data loss
6. **Configure watermarks** — For stateful operations
7. **Schema registry** — For Avro/Protobuf with schema evolution

## Related Skills

- `spark-streaming-master-class-kafka-to-delta` — Kafka ingestion patterns
- `stream-static-joins-expert-pack` — Dimension enrichment
- `stream-stream-joins-expert-pack` — Event correlation
- `streaming-best-practices-expert-pack` — Production guidelines
- `multi-sink-streaming-expert-pack` — ForEachBatch patterns
