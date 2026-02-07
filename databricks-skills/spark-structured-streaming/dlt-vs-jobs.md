---
name: dlt-vs-jobs-comparison
description: Choose between Delta Live Tables (DLT) and Databricks Jobs for streaming workloads. Use when designing new streaming pipelines, deciding on orchestration approach, implementing medallion architecture, or needing custom streaming logic with fine-grained control.
---

# DLT vs Jobs for Streaming

Choose between Delta Live Tables (DLT) and Databricks Jobs based on pipeline complexity, data quality requirements, and control needs.

## Quick Comparison

| Aspect | Delta Live Tables (DLT) | Databricks Jobs |
|--------|-------------------------|-----------------|
| **Best for** | Standard medallion pipelines | Custom logic, external integrations |
| **Orchestration** | Automatic | Manual |
| **Data Quality** | Built-in expectations | Self-managed |
| **Code Flexibility** | SQL/Python API | Full Spark API |
| **Dependency Management** | Automatic | Manual |

## Delta Live Tables (DLT)

### Best For

- Multi-stage pipelines (bronze → silver → gold)
- Data quality enforcement
- Automatic dependency management
- Built-in monitoring and lineage

### Benefits

```python
# Declarative pipeline definition
@dlt.table(
    name="bronze_events",
    comment="Raw events from Kafka"
)
def bronze_events():
    return spark.readStream.format("kafka").load()

@dlt.table(
    name="silver_events",
    comment="Cleansed events"
)
@dlt.expect("valid_timestamp", "timestamp IS NOT NULL")
def silver_events():
    return dlt.read("bronze_events").filter(...)

# Automatic orchestration
# Built-in quality expectations
# Automatic recovery and retry
```

### Limitations

- Requires SQL or Python API (not arbitrary code)
- Less control over execution
- Limited custom error handling

## Databricks Jobs

### Best For

- Custom streaming logic
- Complex transformations
- Integration with external systems
- Fine-grained control

### Benefits

```python
# Full Spark API access
def custom_streaming_logic():
    df = spark.readStream.format("kafka").load()
    
    # Custom transformations
    transformed = complex_custom_logic(df)
    
    # External API calls
    enriched = call_external_api(transformed)
    
    # Custom error handling
    try:
        enriched.writeStream.format("delta").start()
    except Exception as e:
        handle_error(e)
    
    return enriched

# Flexible scheduling
# Direct control over clusters
# Custom monitoring and alerting
```

### Limitations

- Manual dependency management
- Self-managed quality checks
- More code to maintain

## Decision Matrix

| Factor | Use DLT | Use Jobs |
|--------|---------|----------|
| Standard medallion architecture | ✓ | |
| Complex custom logic | | ✓ |
| Need data quality enforcement | ✓ | |
| External API calls | | ✓ |
| Multiple interdependent streams | ✓ | |
| Fine-grained cost control | | ✓ |
| Custom error handling | | ✓ |
| Full Spark API needed | | ✓ |

## Common Patterns

### Pattern 1: DLT for Medallion Architecture

Use DLT for standard bronze-silver-gold pipelines:

```python
import dlt

@dlt.table(
    name="bronze_events",
    comment="Raw events"
)
def bronze_events():
    return (spark
        .readStream
        .format("kafka")
        .option("subscribe", "events")
        .load()
    )

@dlt.table(
    name="silver_events",
    comment="Cleansed events"
)
@dlt.expect("valid_id", "id IS NOT NULL")
@dlt.expect("valid_timestamp", "timestamp IS NOT NULL")
def silver_events():
    return (dlt.read_stream("bronze_events")
        .dropDuplicates(["id"])
        .filter(col("status") == "active")
    )

@dlt.table(
    name="gold_metrics",
    comment="Business metrics"
)
def gold_metrics():
    return (dlt.read_stream("silver_events")
        .groupBy(window(col("timestamp"), "1 hour"), "category")
        .agg(count("*").alias("event_count"))
    )
```

### Pattern 2: Jobs for Custom Logic

Use Jobs for custom transformations and external integrations:

```python
def custom_streaming_pipeline():
    # Read from Kafka
    df = (spark
        .readStream
        .format("kafka")
        .option("subscribe", "events")
        .load()
    )
    
    # Custom transformation logic
    transformed = complex_custom_transformation(df)
    
    # External API enrichment
    enriched = enrich_with_external_api(transformed)
    
    # Custom error handling
    def write_with_error_handling(batch_df, batch_id):
        try:
            batch_df.write.format("delta").mode("append").save("/delta/target")
        except Exception as e:
            # Custom error handling
            log_error(e, batch_id)
            send_alert(e)
            raise
    
    enriched.writeStream \
        .foreachBatch(write_with_error_handling) \
        .option("checkpointLocation", "/checkpoints/custom") \
        .start()

# Schedule via Databricks Jobs
```

### Pattern 3: Hybrid Approach

Use DLT for standard layers, Jobs for custom preprocessing:

```python
# Job 1: Custom preprocessing
def custom_preprocessing():
    df = spark.readStream.format("kafka").load()
    # Custom logic
    df.writeStream.format("delta").start("/delta/preprocessed")

# DLT Pipeline: Standard medallion
@dlt.table(name="bronze")
def bronze():
    return dlt.read_stream("preprocessed")  # Read from Job 1 output

@dlt.table(name="silver")
def silver():
    return dlt.read_stream("bronze").filter(...)
```

## When to Choose DLT

Choose DLT when:
- Building standard medallion architecture
- Need automatic data quality enforcement
- Want automatic dependency management
- Prefer declarative pipeline definition
- Need built-in monitoring and lineage

## When to Choose Jobs

Choose Jobs when:
- Need custom streaming logic
- Integrating with external systems
- Require fine-grained control
- Need custom error handling
- Want full Spark API access
- Need custom scheduling logic

## Production Considerations

### DLT Considerations

```python
# DLT handles:
# - Automatic retries
# - Dependency management
# - Quality expectations
# - Monitoring and alerts

# You manage:
# - Cluster configuration
# - Cost optimization
# - Custom transformations (within DLT API)
```

### Jobs Considerations

```python
# You manage:
# - Dependency orchestration
# - Error handling and retries
# - Data quality checks
# - Monitoring and alerts
# - Cluster configuration

# Benefits:
# - Full control
# - Custom logic
# - External integrations
```

## Migration Patterns

### Migrating from Jobs to DLT

```python
# Before: Jobs with manual orchestration
# After: DLT with automatic orchestration

# Step 1: Convert to DLT table functions
@dlt.table(name="bronze")
def bronze():
    return spark.readStream.format("kafka").load()

# Step 2: Add quality expectations
@dlt.expect("valid_data", "id IS NOT NULL")

# Step 3: Let DLT handle orchestration
```

### Migrating from DLT to Jobs

```python
# Before: DLT declarative
# After: Jobs with custom logic

# Step 1: Extract DLT logic to functions
def bronze_logic():
    return spark.readStream.format("kafka").load()

# Step 2: Add custom error handling
def bronze_with_error_handling():
    try:
        return bronze_logic()
    except Exception as e:
        handle_error(e)

# Step 3: Schedule via Jobs
```

## Production Checklist

### DLT Checklist

- [ ] Standard medallion architecture
- [ ] Data quality expectations defined
- [ ] Dependencies are clear
- [ ] Monitoring configured
- [ ] Cluster configuration optimized

### Jobs Checklist

- [ ] Custom logic required
- [ ] External integrations needed
- [ ] Error handling implemented
- [ ] Dependency management configured
- [ ] Monitoring and alerting set up
- [ ] Cluster configuration optimized

## Related Skills

- `kafka-to-delta` - Kafka ingestion patterns
- `write-multiple-tables` - Multi-sink patterns
- `checkpoint-best-practices` - Checkpoint management
