---
name: kafka-to-kafka
description: Build low-latency streaming pipelines that read from Kafka, process in Spark, and write back to Kafka. Use when implementing event enrichment, format transformation, filtering and routing, real-time validation, or any Kafka-to-Kafka streaming pipeline requiring sub-second to seconds latency.
---

# Kafka-to-Kafka Streaming

Build streaming pipelines that read from Kafka, process events in Spark, and write results back to Kafka. Use Real-Time Mode (RTM) for sub-second latency or microbatch for cost-effective processing.

## Quick Start

```python
from pyspark.sql.functions import col, from_json, to_json, struct, current_timestamp
from pyspark.sql.types import StructType

# 1. Read from source Kafka topic
source_df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "input-events")
    .option("startingOffsets", "latest")
    .option("minPartitions", "6")
    .load()
)

# 2. Parse JSON value
parsed_df = source_df.select(
    col("key").cast("string"),
    from_json(col("value").cast("string"), event_schema).alias("data"),
    col("topic").alias("source_topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
).select("key", "data.*", "source_topic", "partition", "offset", "kafka_timestamp")

# 3. Transform events
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
    .select(col("key"), col("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("topic", "output-events")
    .option("checkpointLocation", "/Volumes/catalog/checkpoints/kafka-to-kafka-pipeline")
    .trigger(processingTime="30 seconds")
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
# Real-time trigger (Databricks 13.3+)
query = (enriched_df
    .select(col("key"), col("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("topic", "output-events")
    .trigger(realTime=True)  # Enable RTM
    .option("checkpointLocation", checkpoint_path)
    .start()
)

# RTM Cluster Requirements
spark.conf.set("spark.databricks.photon.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
               "com.databricks.sql.streaming.state.RocksDBStateProvider")

# Fixed-size cluster recommended
# - Driver: Minimum 4 cores
# - Workers: Fixed size (no autoscaling for RTM)
# - Enable Photon
```

## Common Patterns

### Pattern 1: Event Enrichment with Stream-Static Join

Enrich events with dimension data from Delta tables:

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

### Pattern 2: Event Filtering and Multi-Topic Routing

Route events to different Kafka topics based on criteria:

```python
from pyspark.sql.functions import col, lit, to_json, struct

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
            .select(
                col("key"),
                to_json(struct("value", lit("ERROR").alias("dlq_reason"))).alias("value")
            )
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("topic", "error-events-dlq")
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

### Pattern 3: Schema Validation and Dead Letter Queue

Validate schema and route invalid records to DLQ:

```python
from pyspark.sql.functions import from_json, col, lit, to_json, struct, current_timestamp
from pyspark.sql.types import StructType

validated_schema = StructType([
    # Define your schema
])

def validate_and_route(batch_df, batch_id):
    """Validate schema, route bad records to DLQ"""
    
    # Try to parse with strict schema
    parsed = batch_df.withColumn(
        "parsed",
        from_json(col("value").cast("string"), validated_schema)
    )
    
    # Valid records
    valid = parsed.filter(col("parsed").isNotNull()).select(
        col("key"),
        col("value")
    )
    
    # Invalid records → DLQ
    invalid = parsed.filter(col("parsed").isNull()).select(
        col("key"),
        to_json(struct(
            col("value"),
            lit("SCHEMA_VALIDATION_FAILED").alias("dlq_reason"),
            current_timestamp().alias("dlq_timestamp")
        )).alias("value")
    )
    
    # Write valid to main topic
    if valid.count() > 0:
        (valid
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("topic", "valid-events")
            .save()
        )
    
    # Write invalid to DLQ
    if invalid.count() > 0:
        (invalid
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

### Pattern 4: Format Transformation (Avro to JSON)

Transform events from Avro to JSON format:

```python
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import to_json, struct

# Read Avro-encoded events
avro_df = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "avro-events")
    .load()
)

# Parse Avro schema
avro_schema = """
{
  "type": "record",
  "name": "Event",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}
"""

# Convert Avro to JSON
json_df = avro_df.select(
    col("key"),
    from_avro(col("value"), avro_schema).alias("data")
).select(
    col("key"),
    to_json(struct("data.*")).alias("value")
)

# Write JSON to output topic
json_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("topic", "json-events") \
    .option("checkpointLocation", "/checkpoints/avro-to-json") \
    .start()
```

### Pattern 5: Aggregations with Windowed Output

Real-time aggregations written to Kafka metrics topic:

```python
from pyspark.sql.functions import window, count, avg, max, to_json, struct, concat, lit

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

# Use Update or Complete output mode for aggregations
(windowed_metrics
    .select(
        concat(col("event_type"), lit("-"), col("window.start").cast("string")).alias("key"),
        col("metric_payload").alias("value")
    )
    .writeStream
    .outputMode("update")  # Only changed windows
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("topic", "metrics-output")
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", "/checkpoints/metrics")
    .start()
)
```

## Kafka Configuration

### Producer Options (Writing to Kafka)

```python
(df
    .select("key", "value")
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
    .option("checkpointLocation", checkpoint_path)
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
    .option("startingOffsets", "latest")  # latest, earliest, or specific JSON
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

# Using Databricks secrets
kafka_username = dbutils.secrets.get("kafka-scope", "username")
kafka_password = dbutils.secrets.get("kafka-scope", "password")
```

## Monitoring

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
        processing_rate = input_rows / (duration_ms / 1000.0) if duration_ms > 0 else 0
        
        print(f"Stream: {stream.name}")
        print(f"Input rate: {input_rows} rows")
        print(f"Processing rate: {processing_rate:.2f} rows/sec")
        
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

- **Input Rate**: Messages per second from Kafka
- **Processing Rate**: Must be >= Input Rate
- **Max Offsets Behind Latest**: Should decrease over time
- **Batch Duration**: Should be < trigger interval

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **High latency** | Microbatch overhead | Use RTM (trigger(realTime=True)) |
| **Consumer lag** | Processing < Input rate | Scale cluster; reduce maxOffsetsPerTrigger |
| **Duplicate messages** | Exactly-once not configured | Enable idempotent producer (acks=all) |
| **Schema mismatch** | Evolving schemas | Use schema registry (Confluent) |
| **OOM on driver** | Too many partitions | Reduce minPartitions; increase driver memory |
| **Small batches** | Trigger too frequent | Increase processingTime interval |
| **Connection errors** | Network issues | Check broker connectivity; retry configuration |

## Production Best Practices

1. **Use RTM only when needed** — Microbatch is more cost-effective for >1s latency
2. **Fixed-size clusters** — No autoscaling for streaming/RTM
3. **One checkpoint per pipeline** — Never share checkpoints
4. **Monitor consumer lag** — Alert when increasing
5. **Use left joins** — For stream-static joins to prevent data loss
6. **Configure watermarks** — For stateful operations (aggregations, joins)
7. **Schema registry** — For Avro/Protobuf with schema evolution
8. **Idempotent writes** — Use acks=all for exactly-once semantics
9. **Dead letter queues** — Route invalid records for manual review
10. **Security** — Use SASL/SSL for production Kafka clusters

## Production Checklist

- [ ] RTM enabled only if latency < 800ms required
- [ ] Fixed-size cluster configured (no autoscaling)
- [ ] Checkpoint location is unique per pipeline
- [ ] Consumer lag monitored and alerts configured
- [ ] Producer acks=all for durability
- [ ] Schema validation with DLQ configured
- [ ] Security (SASL/SSL) configured for production
- [ ] Performance metrics tracked (input rate, lag, batch duration)
- [ ] Error handling and retry logic implemented
- [ ] Exactly-once semantics verified

## Related Skills

- `kafka-to-delta` - Kafka ingestion to Delta Lake
- `stream-static-joins` - Enrichment patterns with Delta tables
- `stream-stream-joins` - Event correlation across Kafka topics
- `write-multiple-tables` - Multi-sink patterns
- `checkpoint-best-practices` - Checkpoint configuration
