---
name: error-handling-recovery
description: Implement error handling and recovery patterns for Spark Structured Streaming. Use when setting up dead letter queues, handling exceptions in ForEachBatch, recovering from checkpoint failures, implementing data quality checks, or building resilient streaming pipelines.
---

# Error Handling and Recovery

Implement robust error handling and recovery patterns for production streaming pipelines. Handle exceptions gracefully, route invalid data to dead letter queues, and recover from failures.

## Quick Start

```python
from pyspark.sql.functions import col, lit, current_timestamp, to_json, struct

def write_with_error_handling(batch_df, batch_id):
    """Write with error handling and DLQ"""
    try:
        # Validate data
        valid = batch_df.filter(
            col("required_field").isNotNull() & 
            col("timestamp").isNotNull()
        )
        
        # Write valid data
        valid.write.format("delta").mode("append").saveAsTable("target")
        
        # Route invalid to DLQ
        invalid = batch_df.filter(
            col("required_field").isNull() | 
            col("timestamp").isNull()
        )
        if invalid.count() > 0:
            (invalid
                .withColumn("error_reason", lit("VALIDATION_FAILED"))
                .withColumn("error_timestamp", current_timestamp())
                .write.format("delta").mode("append").saveAsTable("dlq")
            )
    except Exception as e:
        # Log error and fail batch
        log_error(e, batch_id)
        raise

stream.writeStream \
    .foreachBatch(write_with_error_handling) \
    .option("checkpointLocation", "/checkpoints/stream") \
    .start()
```

## Exception Handling Patterns

### Pattern 1: Try-Except in ForEachBatch

Handle exceptions gracefully:

```python
def robust_write(batch_df, batch_id):
    """Robust write with error handling"""
    try:
        # Main processing logic
        processed = batch_df.transform(process_logic)
        processed.write.format("delta").mode("append").saveAsTable("target")
        
    except Exception as e:
        # Log error details
        error_details = {
            "batch_id": batch_id,
            "error": str(e),
            "timestamp": current_timestamp(),
            "row_count": batch_df.count()
        }
        
        # Write error to error log
        error_df = spark.createDataFrame([error_details])
        error_df.write.format("delta").mode("append").saveAsTable("error_log")
        
        # Send alert
        send_alert(f"Stream error in batch {batch_id}: {e}")
        
        # Option 1: Fail batch (default)
        raise
        
        # Option 2: Route to DLQ and continue
        # batch_df.write.format("delta").mode("append").saveAsTable("dlq")
        # return  # Continue processing

stream.writeStream \
    .foreachBatch(robust_write) \
    .option("checkpointLocation", "/checkpoints/stream") \
    .start()
```

### Pattern 2: Per-Row Error Handling

Handle errors at row level:

```python
from pyspark.sql.functions import when, lit

def process_with_row_level_errors(batch_df, batch_id):
    """Process with row-level error handling"""
    
    # Add error tracking columns
    processed = batch_df.withColumn(
        "processing_error",
        when(col("required_field").isNull(), lit("MISSING_REQUIRED_FIELD"))
        .when(col("timestamp").isNull(), lit("MISSING_TIMESTAMP"))
        .otherwise(lit(None))
    )
    
    # Valid rows
    valid = processed.filter(col("processing_error").isNull())
    if valid.count() > 0:
        (valid
            .drop("processing_error")
            .write.format("delta").mode("append").saveAsTable("target")
        )
    
    # Invalid rows → DLQ
    invalid = processed.filter(col("processing_error").isNotNull())
    if invalid.count() > 0:
        (invalid
            .withColumn("dlq_timestamp", current_timestamp())
            .withColumn("batch_id", lit(batch_id))
            .write.format("delta").mode("append").saveAsTable("dlq")
        )

stream.writeStream \
    .foreachBatch(process_with_row_level_errors) \
    .option("checkpointLocation", "/checkpoints/stream") \
    .start()
```

## Dead Letter Queue Patterns

### Pattern 1: Schema Validation DLQ

Route schema validation failures to DLQ:

```python
from pyspark.sql.functions import from_json, col, lit, to_json, struct, current_timestamp
from pyspark.sql.types import StructType

def validate_schema_with_dlq(batch_df, batch_id):
    """Validate schema, route failures to DLQ"""
    
    validated_schema = StructType([...])  # Define schema
    
    # Try to parse with strict schema
    parsed = batch_df.withColumn(
        "parsed",
        from_json(col("value").cast("string"), validated_schema)
    )
    
    # Valid records
    valid = parsed.filter(col("parsed").isNotNull())
    if valid.count() > 0:
        (valid
            .select("parsed.*")
            .write.format("delta").mode("append").saveAsTable("target")
        )
    
    # Invalid records → DLQ
    invalid = parsed.filter(col("parsed").isNull())
    if invalid.count() > 0:
        (invalid
            .withColumn("dlq_reason", lit("SCHEMA_VALIDATION_FAILED"))
            .withColumn("dlq_timestamp", current_timestamp())
            .withColumn("batch_id", lit(batch_id))
            .select(
                col("key"),
                col("value"),
                col("dlq_reason"),
                col("dlq_timestamp"),
                col("batch_id")
            )
            .write.format("delta").mode("append").saveAsTable("dlq")
        )

source_df.writeStream \
    .foreachBatch(validate_schema_with_dlq) \
    .option("checkpointLocation", "/checkpoints/validation") \
    .start()
```

### Pattern 2: Data Quality DLQ

Route data quality failures to DLQ:

```python
from pyspark.sql.functions import when

def data_quality_with_dlq(batch_df, batch_id):
    """Data quality checks with DLQ"""
    
    # Apply quality checks
    quality_checked = batch_df.withColumn(
        "quality_error",
        when(col("id").isNull(), lit("MISSING_ID"))
        .when(col("timestamp").isNull(), lit("MISSING_TIMESTAMP"))
        .when(col("amount") < 0, lit("NEGATIVE_AMOUNT"))
        .when(col("email").rlike("^[A-Za-z0-9+_.-]+@(.+)$") == False, lit("INVALID_EMAIL"))
        .otherwise(lit(None))
    )
    
    # Valid data
    valid = quality_checked.filter(col("quality_error").isNull())
    if valid.count() > 0:
        (valid
            .drop("quality_error")
            .write.format("delta").mode("append").saveAsTable("target")
        )
    
    # Invalid data → DLQ
    invalid = quality_checked.filter(col("quality_error").isNotNull())
    if invalid.count() > 0:
        (invalid
            .withColumn("dlq_timestamp", current_timestamp())
            .withColumn("batch_id", lit(batch_id))
            .write.format("delta").mode("append").saveAsTable("dlq")
        )

stream.writeStream \
    .foreachBatch(data_quality_with_dlq) \
    .option("checkpointLocation", "/checkpoints/quality") \
    .start()
```

### Pattern 3: Retry Logic with DLQ

Implement retry logic before DLQ:

```python
import time

def write_with_retry(batch_df, batch_id, max_retries=3):
    """Write with retry logic"""
    
    for attempt in range(max_retries):
        try:
            batch_df.write.format("delta").mode("append").saveAsTable("target")
            return  # Success
        except Exception as e:
            if attempt < max_retries - 1:
                # Retry with exponential backoff
                wait_time = 2 ** attempt
                print(f"Retry {attempt + 1}/{max_retries} after {wait_time}s")
                time.sleep(wait_time)
            else:
                # Final attempt failed → DLQ
                (batch_df
                    .withColumn("dlq_reason", lit(f"RETRY_FAILED: {str(e)}"))
                    .withColumn("dlq_timestamp", current_timestamp())
                    .write.format("delta").mode("append").saveAsTable("dlq")
                )
                raise

stream.writeStream \
    .foreachBatch(lambda df, bid: write_with_retry(df, bid)) \
    .option("checkpointLocation", "/checkpoints/retry") \
    .start()
```

## Checkpoint Recovery

### Recovery from Checkpoint Loss

```python
# Scenario: Lost checkpoint
# Steps to recover:

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

### Recovery from Checkpoint Corruption

```python
# Scenario: Corrupted checkpoint
# Same as lost checkpoint:

# 1. Backup corrupted checkpoint (for investigation)
dbutils.fs.cp(
    "/checkpoints/stream",
    "/checkpoints/stream_corrupted_backup",
    recurse=True
)

# 2. Delete checkpoint
dbutils.fs.rm("/checkpoints/stream", recurse=True)

# 3. Restart stream
df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/stream") \
    .option("startingOffsets", "earliest") \
    .start()
```

### Recovery from Partial Failure

```python
# Scenario: Stream crashed during batch
# Spark automatically handles:

# 1. On restart, Spark checks latest offset
# 2. Checks if commit exists for that offset
# 3. If commit missing: reprocesses batch
# 4. Delta deduplication prevents duplicates

# No manual intervention needed if:
# - Checkpoint is intact
# - Delta sink uses idempotent writes (txnVersion/txnAppId)
```

## Data Quality Checks

### Pattern 1: Comprehensive Quality Checks

```python
from pyspark.sql.functions import when, lit, col

def comprehensive_quality_checks(batch_df, batch_id):
    """Comprehensive data quality checks"""
    
    quality_checked = batch_df.withColumn(
        "quality_errors",
        when(col("id").isNull(), lit("MISSING_ID"))
        .when(col("timestamp").isNull(), lit("MISSING_TIMESTAMP"))
        .when(col("amount").isNull(), lit("MISSING_AMOUNT"))
        .when(col("amount") < 0, lit("NEGATIVE_AMOUNT"))
        .when(col("email").isNull(), lit("MISSING_EMAIL"))
        .when(col("status").isin(["active", "pending", "completed"]) == False, lit("INVALID_STATUS"))
        .otherwise(lit(None))
    )
    
    # Valid data
    valid = quality_checked.filter(col("quality_errors").isNull())
    
    # Invalid data
    invalid = quality_checked.filter(col("quality_errors").isNotNull())
    
    # Write valid
    if valid.count() > 0:
        (valid
            .drop("quality_errors")
            .write.format("delta").mode("append").saveAsTable("target")
        )
    
    # Write invalid to DLQ with details
    if invalid.count() > 0:
        (invalid
            .withColumn("dlq_timestamp", current_timestamp())
            .withColumn("batch_id", lit(batch_id))
            .write.format("delta").mode("append").saveAsTable("dlq")
        )

stream.writeStream \
    .foreachBatch(comprehensive_quality_checks) \
    .option("checkpointLocation", "/checkpoints/quality") \
    .start()
```

### Pattern 2: Quality Metrics Tracking

```python
def track_quality_metrics(batch_df, batch_id):
    """Track data quality metrics"""
    
    total_rows = batch_df.count()
    valid_rows = batch_df.filter(
        col("id").isNotNull() & 
        col("timestamp").isNotNull()
    ).count()
    invalid_rows = total_rows - valid_rows
    
    quality_rate = valid_rows / total_rows if total_rows > 0 else 0
    
    # Write metrics
    metrics_df = spark.createDataFrame([{
        "batch_id": batch_id,
        "timestamp": current_timestamp(),
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "quality_rate": quality_rate
    }])
    
    metrics_df.write.format("delta").mode("append").saveAsTable("quality_metrics")
    
    # Alert if quality rate < threshold
    if quality_rate < 0.95:  # 95% threshold
        send_alert(f"Data quality below threshold: {quality_rate:.2%}")

stream.writeStream \
    .foreachBatch(track_quality_metrics) \
    .option("checkpointLocation", "/checkpoints/quality") \
    .start()
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **Stream fails on error** | No error handling | Implement try-except in ForEachBatch |
| **Invalid data lost** | No DLQ configured | Route invalid data to DLQ |
| **Checkpoint corruption** | File system issues | Delete checkpoint, restart from earliest |
| **Partial batch failures** | One sink fails | Use idempotent writes; Spark retries |
| **Data quality issues** | No validation | Implement quality checks with DLQ |

## Production Best Practices

### Always Use Idempotent Writes

```python
# Ensure exactly-once semantics
def idempotent_write(batch_df, batch_id):
    (batch_df.write
        .format("delta")
        .mode("append")
        .option("txnVersion", batch_id)
        .option("txnAppId", "streaming_job")
        .saveAsTable("target")
    )

stream.writeStream \
    .foreachBatch(idempotent_write) \
    .option("checkpointLocation", "/checkpoints/stream") \
    .start()
```

### Implement Comprehensive Error Handling

```python
def comprehensive_error_handling(batch_df, batch_id):
    """Comprehensive error handling pattern"""
    try:
        # Validate
        valid = validate_data(batch_df)
        
        # Process
        processed = process_data(valid)
        
        # Write
        write_data(processed, batch_id)
        
    except ValidationError as e:
        # Route to DLQ
        route_to_dlq(batch_df, batch_id, str(e))
    except ProcessingError as e:
        # Log and retry
        log_error(e, batch_id)
        raise  # Retry batch
    except Exception as e:
        # Unexpected error
        log_error(e, batch_id)
        send_alert(f"Unexpected error: {e}")
        raise

stream.writeStream \
    .foreachBatch(comprehensive_error_handling) \
    .option("checkpointLocation", "/checkpoints/stream") \
    .start()
```

## Production Checklist

- [ ] Error handling implemented in ForEachBatch
- [ ] Dead letter queue configured
- [ ] Data quality checks implemented
- [ ] Idempotent writes configured
- [ ] Error logging implemented
- [ ] Alerting configured for errors
- [ ] Recovery procedures documented
- [ ] DLQ monitoring and processing configured

## Related Skills

- `monitoring-observability` - Error monitoring and alerting
- `checkpoint-best-practices` - Checkpoint recovery
- `kafka-to-delta` - Ingestion error handling
