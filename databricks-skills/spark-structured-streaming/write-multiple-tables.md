---
name: write-multiple-tables
description: Write a single Spark stream to multiple Delta tables using ForEachBatch. Use when fanning out streaming data to multiple sinks, implementing medallion architecture (bronze/silver/gold), conditional routing, CDC patterns, or creating materialized views from a single stream.
---

# Write to Multiple Tables

Write a single streaming source to multiple Delta tables efficiently using ForEachBatch. Read once, write many - avoiding reprocessing the source multiple times.

## Quick Start

```python
from pyspark.sql.functions import col, current_timestamp

def write_multiple_tables(batch_df, batch_id):
    """Write batch to multiple sinks"""
    # Bronze - raw data
    batch_df.write \
        .format("delta") \
        .mode("append") \
        .option("txnVersion", batch_id) \
        .option("txnAppId", "multi_sink_job") \
        .save("/delta/bronze_events")
    
    # Silver - cleansed
    cleansed = batch_df.dropDuplicates(["event_id"])
    cleansed.write \
        .format("delta") \
        .mode("append") \
        .option("txnVersion", batch_id) \
        .option("txnAppId", "multi_sink_job_silver") \
        .save("/delta/silver_events")
    
    # Gold - aggregated
    aggregated = batch_df.groupBy("category").count()
    aggregated.write \
        .format("delta") \
        .mode("append") \
        .option("txnVersion", batch_id) \
        .option("txnAppId", "multi_sink_job_gold") \
        .save("/delta/category_counts")

stream.writeStream \
    .foreachBatch(write_multiple_tables) \
    .option("checkpointLocation", "/checkpoints/multi_sink") \
    .start()
```

## Core Concepts

### One Source, One Checkpoint

Use a single checkpoint for the entire multi-sink stream:

```python
# CORRECT: One checkpoint for all sinks
stream.writeStream \
    .foreachBatch(multi_sink_function) \
    .option("checkpointLocation", "/checkpoints/single_source_multi_sink") \
    .start()

# WRONG: Don't create separate streams
# Each stream would reprocess the source independently
```

### Transactional Guarantees

Each ForEachBatch call represents one epoch. All writes within the batch:
- See the same input data
- Share the same batch_id
- Are idempotent if using txnVersion

## Common Patterns

### Pattern 1: Bronze-Silver-Gold Medallion Architecture

Single stream feeding all three medallion layers:

```python
from pyspark.sql.functions import window, count, sum, current_timestamp

def medallion_architecture(batch_df, batch_id):
    """Single stream feeding all three medallion layers"""
    
    # Bronze: Raw ingestion
    (batch_df.write
        .format("delta")
        .mode("append")
        .option("txnVersion", batch_id)
        .option("txnAppId", "medallion_bronze")
        .saveAsTable("bronze.events")
    )
    
    # Silver: Cleansed and validated
    silver_df = (batch_df
        .dropDuplicates(["event_id"])
        .filter(col("status").isin(["active", "pending"]))
        .withColumn("processed_at", current_timestamp())
    )
    
    (silver_df.write
        .format("delta")
        .mode("append")
        .option("txnVersion", batch_id)
        .option("txnAppId", "medallion_silver")
        .saveAsTable("silver.events")
    )
    
    # Gold: Business aggregates
    gold_df = (silver_df
        .groupBy(window(col("timestamp"), "5 minutes"), "category")
        .agg(
            count("*").alias("event_count"),
            sum("amount").alias("total_amount")
        )
    )
    
    (gold_df.write
        .format("delta")
        .mode("append")
        .option("txnVersion", batch_id)
        .option("txnAppId", "medallion_gold")
        .saveAsTable("gold.category_metrics")
    )

# Single stream, multiple outputs
stream.writeStream \
    .foreachBatch(medallion_architecture) \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", "/checkpoints/medallion") \
    .start()
```

### Pattern 2: Conditional Routing

Route events to different tables based on criteria:

```python
def route_by_type(batch_df, batch_id):
    """Route events to different tables based on type"""
    
    # Split by event type
    orders = batch_df.filter(col("event_type") == "order")
    refunds = batch_df.filter(col("event_type") == "refund")
    reviews = batch_df.filter(col("event_type") == "review")
    
    # Write to respective tables
    if orders.count() > 0:
        (orders.write
            .format("delta")
            .mode("append")
            .option("txnVersion", batch_id)
            .option("txnAppId", "router_orders")
            .saveAsTable("orders")
        )
    
    if refunds.count() > 0:
        (refunds.write
            .format("delta")
            .mode("append")
            .option("txnVersion", batch_id)
            .option("txnAppId", "router_refunds")
            .saveAsTable("refunds")
        )
    
    if reviews.count() > 0:
        (reviews.write
            .format("delta")
            .mode("append")
            .option("txnVersion", batch_id)
            .option("txnAppId", "router_reviews")
            .saveAsTable("reviews")
        )

stream.writeStream \
    .foreachBatch(route_by_type) \
    .option("checkpointLocation", "/checkpoints/routing") \
    .start()
```

### Pattern 3: Parallel Fan-Out

Write to multiple sinks in parallel for independent tables:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def parallel_write(batch_df, batch_id):
    """Write to multiple sinks in parallel"""
    
    # Cache to avoid recomputation
    batch_df.cache()
    
    def write_table(table_name, filter_expr=None):
        """Write filtered data to table"""
        df = batch_df.filter(filter_expr) if filter_expr else batch_df
        (df.write
            .format("delta")
            .mode("append")
            .option("txnVersion", batch_id)
            .option("txnAppId", f"parallel_{table_name}")
            .saveAsTable(table_name)
        )
        return f"Wrote {table_name}"
    
    # Define tables and filters
    tables = [
        ("bronze.all_events", None),
        ("silver.errors", col("level") == "ERROR"),
        ("silver.warnings", col("level") == "WARN"),
        ("gold.metrics", col("type") == "metric")
    ]
    
    # Parallel writes
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(write_table, table_name, filter_expr): table_name 
            for table_name, filter_expr in tables
        }
        
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                result = future.result()
                print(result)
            except Exception as e:
                print(f"Error writing {table_name}: {e}")
                raise  # Fail batch if any write fails
    
    # Release cache
    batch_df.unpersist()

stream.writeStream \
    .foreachBatch(parallel_write) \
    .option("checkpointLocation", "/checkpoints/parallel") \
    .start()
```

### Pattern 4: Transactional Multi-Table Writes

Ensure atomicity across multiple tables using transaction IDs:

```python
def transactional_write(batch_df, batch_id):
    """Ensure atomicity across multiple tables using transaction IDs"""
    
    # Use batch_id as transaction version
    txn_version = batch_id
    app_id = "multi_sink_stream"
    
    # Table 1
    (batch_df.write
        .format("delta")
        .option("txnVersion", txn_version)
        .option("txnAppId", f"{app_id}_table1")
        .mode("append")
        .saveAsTable("table1")
    )
    
    # Table 2
    (batch_df.write
        .format("delta")
        .option("txnVersion", txn_version)
        .option("txnAppId", f"{app_id}_table2")
        .mode("append")
        .saveAsTable("table2")
    )
    
    # Table 3
    (batch_df.write
        .format("delta")
        .option("txnVersion", txn_version)
        .option("txnAppId", f"{app_id}_table3")
        .mode("append")
        .saveAsTable("table3")
    )
    
    # If one fails, Spark will retry the entire batch
    # Delta's idempotent writes ensure no duplicates

stream.writeStream \
    .foreachBatch(transactional_write) \
    .option("checkpointLocation", "/checkpoints/transactional") \
    .start()
```

### Pattern 5: CDC Multi-Target Pattern

Apply CDC changes to both current state table and audit log:

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

def cdc_multi_target_apply(batch_df, batch_id):
    """Apply CDC changes to both current state table and audit log"""
    
    # Split by operation type
    deletes = batch_df.filter(col("_op") == "DELETE")
    upserts = batch_df.filter(col("_op").isin(["INSERT", "UPDATE"]))
    
    # Target 1: Current state table with MERGE
    target_table = DeltaTable.forName(spark, "silver.customers")
    
    if upserts.count() > 0:
        (target_table.alias("target")
            .merge(upserts.alias("source"), "target.customer_id = source.customer_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    if deletes.count() > 0:
        (target_table.alias("target")
            .merge(deletes.alias("source"), "target.customer_id = source.customer_id")
            .whenMatchedDelete()
            .execute()
        )
    
    # Target 2: Audit log (append all changes)
    audit_df = (batch_df
        .withColumn("_processed_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    
    (audit_df.write
        .format("delta")
        .mode("append")
        .option("txnVersion", batch_id)
        .option("txnAppId", "cdc_audit")
        .saveAsTable("audit.customer_changes")
    )

stream.writeStream \
    .foreachBatch(cdc_multi_target_apply) \
    .option("checkpointLocation", "/checkpoints/cdc") \
    .start()
```

### Pattern 6: Materialized Views

Create multiple derived views from the same stream:

```python
from pyspark.sql.functions import window, count, sum

def create_materialized_views(batch_df, batch_id):
    """Create multiple derived views from the same stream"""
    
    # Base: All events
    (batch_df.write
        .format("delta")
        .mode("append")
        .option("txnVersion", batch_id)
        .option("txnAppId", "views_raw")
        .save("/delta/views/raw")
    )
    
    # View 1: Hourly aggregations
    hourly = (batch_df
        .withWatermark("event_time", "1 hour")
        .groupBy(window(col("event_time"), "1 hour"), col("category"))
        .agg(
            count("*").alias("event_count"),
            sum("value").alias("total_value")
        )
    )
    
    (hourly.write
        .format("delta")
        .mode("append")
        .option("txnVersion", batch_id)
        .option("txnAppId", "views_hourly")
        .save("/delta/views/hourly")
    )
    
    # View 2: User sessions (15 min window)
    sessions = (batch_df
        .withWatermark("event_time", "15 minutes")
        .groupBy(window(col("event_time"), "15 minutes"), col("user_id"))
        .agg(count("*").alias("actions"))
    )
    
    (sessions.write
        .format("delta")
        .mode("append")
        .option("txnVersion", batch_id)
        .option("txnAppId", "views_sessions")
        .save("/delta/views/sessions")
    )

stream.writeStream \
    .foreachBatch(create_materialized_views) \
    .option("checkpointLocation", "/checkpoints/views") \
    .start()
```

### Pattern 7: Error Handling with Dead Letter Queue

Route invalid records to DLQ:

```python
from pyspark.sql.functions import when

def write_with_dlq(batch_df, batch_id):
    """Write valid records to target, invalid to dead letter queue"""
    
    # Validation
    valid = batch_df.filter(
        col("required_field").isNotNull() & 
        col("timestamp").isNotNull()
    )
    invalid = batch_df.filter(
        col("required_field").isNull() | 
        col("timestamp").isNull()
    )
    
    # Write valid data
    if valid.count() > 0:
        (valid.write
            .format("delta")
            .mode("append")
            .option("txnVersion", batch_id)
            .option("txnAppId", "multi_sink_valid")
            .saveAsTable("silver.valid_events")
        )
    
    # Write invalid to DLQ with metadata
    if invalid.count() > 0:
        dlq_df = (invalid
            .withColumn("_error_reason", 
                when(col("required_field").isNull(), "missing_required_field")
                .otherwise("missing_timestamp"))
            .withColumn("_batch_id", lit(batch_id))
            .withColumn("_processed_at", current_timestamp())
        )
        
        (dlq_df.write
            .format("delta")
            .mode("append")
            .saveAsTable("errors.dead_letter_queue")
        )

stream.writeStream \
    .foreachBatch(write_with_dlq) \
    .option("checkpointLocation", "/checkpoints/dlq") \
    .start()
```

## Performance Optimization

### Minimize Recomputation

Cache the batch DataFrame to avoid recomputation:

```python
def optimized_multi_sink(batch_df, batch_id):
    """Cache to avoid recomputation"""
    
    # Cache the batch
    batch_df.cache()
    
    # Multiple writes from cached data
    batch_df.write...  # Sink 1
    batch_df.filter(...).write...  # Sink 2
    batch_df.filter(...).write...  # Sink 3
    
    # Unpersist when done
    batch_df.unpersist()
```

### Parallel Writes

Use ThreadPoolExecutor for independent writes:

```python
from concurrent.futures import ThreadPoolExecutor

def parallel_write(batch_df, batch_id):
    """Write to independent tables in parallel"""
    
    def write_table(table_name, df):
        df.write.format("delta").mode("append").saveAsTable(table_name)
    
    # Parallel writes
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.submit(write_table, "table1", batch_df)
        executor.submit(write_table, "table2", batch_df.filter(...))
        executor.submit(write_table, "table3", batch_df.filter(...))
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **Slow writes** | Sequential processing | Use parallel ThreadPoolExecutor |
| **Recomputation** | Multiple actions on same DataFrame | Cache the batch DataFrame |
| **Partial failures** | One sink fails | Use idempotent writes; Spark retries entire batch |
| **Schema conflicts** | Tables have different schemas | Transform before each write |
| **Resource contention** | Too many concurrent writes | Limit parallelism; batch writes |

## Production Best Practices

### Idempotent Writes

Always use txnVersion with batch_id:

```python
.write
    .format("delta")
    .option("txnVersion", batch_id)
    .option("txnAppId", "unique_app_id_per_table")
    .mode("append")
```

### Keep Batch Processing Fast

```python
# GOOD: Simple filters and writes
def efficient_write(df, batch_id):
    df.filter(...).write.save("/delta/table1")
    df.filter(...).write.save("/delta/table2")

# BAD: Expensive aggregations (move to stream definition)
def inefficient_write(df, batch_id):
    df.groupBy(...).agg(...).write.save("/delta/table3")  # Move to stream!
```

### Monitor Each Sink

```python
import time

def monitored_multi_write(df, batch_id):
    start = time.time()
    
    # Write 1
    t1_start = time.time()
    df.write.save("/delta/table1")
    print(f"Table1: {time.time() - t1_start:.2f}s, rows: {df.count()}")
    
    # Write 2
    t2_start = time.time()
    df2 = df.filter(...)
    df2.write.save("/delta/table2")
    print(f"Table2: {time.time() - t2_start:.2f}s, rows: {df2.count()}")
    
    print(f"Total batch time: {time.time() - start:.2f}s")
```

### Handle Schema Evolution

```python
def schema_aware_write(df, batch_id):
    """Handle schemas that may evolve over time"""
    try:
        (df.write
            .format("delta")
            .option("mergeSchema", "true")
            .option("txnVersion", batch_id)
            .mode("append")
            .save("/delta/table")
        )
    except AnalysisException as e:
        # Log schema mismatch for investigation
        print(f"Schema error in batch {batch_id}: {e}")
        # Write to fallback location
        df.write.json(f"/fallback/batch_{batch_id}")
```

## Production Checklist

- [ ] One checkpoint per multi-sink stream
- [ ] Idempotent writes configured (txnVersion/txnAppId)
- [ ] Cache used to avoid recomputation
- [ ] Parallel writes for independent tables
- [ ] Error handling and DLQ configured
- [ ] Schema evolution handled
- [ ] Performance monitoring per sink
- [ ] Checkpoint location is unique and persistent

## Related Skills

- `merge-multiple-tables-parallel` - Parallel MERGE operations
- `kafka-to-delta` - Kafka ingestion patterns
- `stream-static-joins` - Enrichment before multi-sink writes
- `checkpoint-best-practices` - Checkpoint configuration
