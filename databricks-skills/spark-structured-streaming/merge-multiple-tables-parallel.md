---
name: merge-multiple-tables-parallel
description: Perform MERGE operations into multiple Delta tables in parallel using ForEachBatch with Liquid Clustering, Deletion Vectors, and Row-Level Concurrency. Use when upserting streaming data to multiple targets concurrently, implementing CDC patterns across multiple tables, or optimizing merge performance with conflict-free concurrent operations.
---

# Merge into Multiple Tables in Parallel

Perform MERGE operations into multiple Delta tables in parallel using ForEachBatch. Enable Liquid Clustering + Deletion Vectors + Row-Level Concurrency for conflict-free concurrent merges.

## Quick Start

```python
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import col

def parallel_merge_multiple_tables(batch_df, batch_id):
    """Merge into multiple tables in parallel"""
    
    # Cache to avoid recomputation
    batch_df.cache()
    
    def merge_table(table_name, merge_key):
        """Merge into one table"""
        target = DeltaTable.forName(spark, table_name)
        source = batch_df.alias("source")
        
        (target.alias("target")
            .merge(
                source,
                f"target.{merge_key} = source.{merge_key}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return f"Merged {table_name}"
    
    # Define tables and merge keys
    tables = [
        ("silver.customers", "customer_id"),
        ("silver.orders", "order_id"),
        ("silver.products", "product_id")
    ]
    
    # Parallel merges
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(merge_table, table_name, merge_key): table_name
            for table_name, merge_key in tables
        }
        
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                result = future.result()
                print(result)
            except Exception as e:
                print(f"Error merging {table_name}: {e}")
                raise
    
    batch_df.unpersist()

stream.writeStream \
    .foreachBatch(parallel_merge_multiple_tables) \
    .option("checkpointLocation", "/checkpoints/parallel_merge") \
    .start()
```

## Enable Liquid Clustering + DV + RLC

Enable modern Delta features for conflict-free concurrent merges:

```sql
-- Enable for each target table
ALTER TABLE silver.customers SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = true,
  'delta.enableRowLevelConcurrency' = true,
  'delta.liquid.clustering' = true
);

ALTER TABLE silver.orders SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = true,
  'delta.enableRowLevelConcurrency' = true,
  'delta.liquid.clustering' = true
);

ALTER TABLE silver.products SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = true,
  'delta.enableRowLevelConcurrency' = true,
  'delta.liquid.clustering' = true
);
```

**Benefits:**
- **Deletion Vectors**: Soft deletes without file rewrite
- **Row-Level Concurrency**: Concurrent updates to different rows
- **Liquid Clustering**: Automatic optimization without pauses
- **Result**: Eliminates optimize pauses, lower P99 latency, simpler code

## Common Patterns

### Pattern 1: Basic Parallel MERGE

Merge into multiple tables concurrently:

```python
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed

def parallel_merge(batch_df, batch_id):
    """Merge into multiple tables in parallel"""
    
    batch_df.cache()
    
    def merge_one_table(table_name, merge_key):
        target = DeltaTable.forName(spark, table_name)
        source = batch_df.alias("source")
        
        (target.alias("target")
            .merge(
                source,
                f"target.{merge_key} = source.{merge_key}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return table_name
    
    tables = [
        ("silver.customers", "customer_id"),
        ("silver.orders", "order_id"),
        ("silver.products", "product_id")
    ]
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(merge_one_table, table_name, merge_key): table_name
            for table_name, merge_key in tables
        }
        
        errors = []
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                future.result()
            except Exception as e:
                errors.append((table_name, str(e)))
    
    batch_df.unpersist()
    
    if errors:
        raise Exception(f"Merge failures: {errors}")

stream.writeStream \
    .foreachBatch(parallel_merge) \
    .option("checkpointLocation", "/checkpoints/parallel_merge") \
    .start()
```

### Pattern 2: CDC Multi-Target with Parallel MERGE

Apply CDC changes to multiple targets concurrently:

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp

def cdc_parallel_merge(batch_df, batch_id):
    """Apply CDC changes to multiple tables in parallel"""
    
    batch_df.cache()
    
    # Split by operation type
    deletes = batch_df.filter(col("_op") == "DELETE")
    upserts = batch_df.filter(col("_op").isin(["INSERT", "UPDATE"]))
    
    def merge_cdc_table(table_name, merge_key):
        """Merge CDC changes into one table"""
        target = DeltaTable.forName(spark, table_name)
        
        # Upserts
        if upserts.count() > 0:
            (target.alias("target")
                .merge(
                    upserts.alias("source"),
                    f"target.{merge_key} = source.{merge_key}"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        
        # Deletes
        if deletes.count() > 0:
            (target.alias("target")
                .merge(
                    deletes.alias("source"),
                    f"target.{merge_key} = source.{merge_key}"
                )
                .whenMatchedDelete()
                .execute()
            )
        
        return table_name
    
    # Parallel merges to multiple tables
    tables = [
        ("silver.customers", "customer_id"),
        ("silver.orders", "order_id")
    ]
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(merge_cdc_table, table_name, merge_key): table_name
            for table_name, merge_key in tables
        }
        
        for future in as_completed(futures):
            future.result()  # Raise on error
    
    # Audit log (sequential, not parallel)
    audit_df = (batch_df
        .withColumn("_processed_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    
    (audit_df.write
        .format("delta")
        .mode("append")
        .option("txnVersion", batch_id)
        .option("txnAppId", "cdc_audit")
        .saveAsTable("audit.cdc_changes")
    )
    
    batch_df.unpersist()

stream.writeStream \
    .foreachBatch(cdc_parallel_merge) \
    .option("checkpointLocation", "/checkpoints/cdc_parallel") \
    .start()
```

### Pattern 3: Conditional MERGE Routing

Route records to different tables based on criteria, then merge:

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col

def conditional_parallel_merge(batch_df, batch_id):
    """Route and merge based on criteria"""
    
    batch_df.cache()
    
    # Split by region
    na_data = batch_df.filter(col("region") == "NA")
    eu_data = batch_df.filter(col("region") == "EU")
    apac_data = batch_df.filter(col("region") == "APAC")
    
    def merge_region_table(region_data, table_name, merge_key):
        """Merge region-specific data"""
        if region_data.count() == 0:
            return table_name
        
        target = DeltaTable.forName(spark, table_name)
        source = region_data.alias("source")
        
        (target.alias("target")
            .merge(
                source,
                f"target.{merge_key} = source.{merge_key}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return table_name
    
    # Parallel merges
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(merge_region_table, na_data, "silver.customers_na", "customer_id"): "NA",
            executor.submit(merge_region_table, eu_data, "silver.customers_eu", "customer_id"): "EU",
            executor.submit(merge_region_table, apac_data, "silver.customers_apac", "customer_id"): "APAC"
        }
        
        for future in as_completed(futures):
            future.result()
    
    batch_df.unpersist()

stream.writeStream \
    .foreachBatch(conditional_parallel_merge) \
    .option("checkpointLocation", "/checkpoints/conditional_merge") \
    .start()
```

### Pattern 4: Optimized MERGE with Z-Ordering

Use Z-Ordering on merge keys for faster lookups:

```sql
-- Optimize tables with Z-Ordering on merge keys
OPTIMIZE silver.customers ZORDER BY (customer_id);
OPTIMIZE silver.orders ZORDER BY (order_id);
OPTIMIZE silver.products ZORDER BY (product_id);
```

```python
def optimized_parallel_merge(batch_df, batch_id):
    """Parallel merge with optimized tables"""
    
    batch_df.cache()
    
    def merge_optimized_table(table_name, merge_key):
        target = DeltaTable.forName(spark, table_name)
        source = batch_df.alias("source")
        
        # Z-Ordering enables faster lookups
        (target.alias("target")
            .merge(
                source,
                f"target.{merge_key} = source.{merge_key}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return table_name
    
    tables = [
        ("silver.customers", "customer_id"),
        ("silver.orders", "order_id")
    ]
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(merge_optimized_table, table_name, merge_key): table_name
            for table_name, merge_key in tables
        }
        
        for future in as_completed(futures):
            future.result()
    
    batch_df.unpersist()
```

### Pattern 5: MERGE with Partition Pruning

Include partition columns in merge condition:

```python
def partition_pruned_merge(batch_df, batch_id):
    """Merge with partition pruning"""
    
    batch_df.cache()
    
    def merge_partitioned_table(table_name, merge_key, partition_col):
        target = DeltaTable.forName(spark, table_name)
        source = batch_df.alias("source")
        
        # Include partition column in merge condition
        (target.alias("target")
            .merge(
                source,
                f"target.{merge_key} = source.{merge_key} AND target.{partition_col} = source.{partition_col}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return table_name
    
    tables = [
        ("silver.events", "event_id", "date"),
        ("silver.metrics", "metric_id", "date")
    ]
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(merge_partitioned_table, table_name, merge_key, partition_col): table_name
            for table_name, merge_key, partition_col in tables
        }
        
        for future in as_completed(futures):
            future.result()
    
    batch_df.unpersist()
```

## Performance Optimization

### Liquid Clustering Configuration

```sql
-- Create table with Liquid Clustering
CREATE TABLE silver.customers (
    customer_id STRING,
    name STRING,
    email STRING,
    updated_at TIMESTAMP
) USING DELTA
CLUSTER BY (customer_id)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = true,
    'delta.enableRowLevelConcurrency' = true
);
```

### File Size Tuning

```sql
-- Target file size for optimal merge
ALTER TABLE silver.customers 
SET TBLPROPERTIES (
    'delta.targetFileSize' = '128mb'
);
```

### Optimal Thread Count

```python
# Formula: min(number_of_tables, cluster_cores / 2)
# Example: 4 tables, 8 cores → 4 workers
# Example: 2 tables, 4 cores → 2 workers

max_workers = min(len(tables), max(2, total_cores // 2))
```

## Error Handling

### Collect All Errors Before Failing

```python
def robust_parallel_merge(batch_df, batch_id):
    """Collect all errors before failing batch"""
    
    batch_df.cache()
    errors = []
    
    def merge_with_error_handling(table_name, merge_key):
        try:
            target = DeltaTable.forName(spark, table_name)
            source = batch_df.alias("source")
            
            (target.alias("target")
                .merge(
                    source,
                    f"target.{merge_key} = source.{merge_key}"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            return None
        except Exception as e:
            return (table_name, str(e))
    
    tables = [
        ("silver.customers", "customer_id"),
        ("silver.orders", "order_id")
    ]
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(merge_with_error_handling, table_name, merge_key): table_name
            for table_name, merge_key in tables
        }
        
        for future in as_completed(futures):
            error = future.result()
            if error:
                errors.append(error)
    
    batch_df.unpersist()
    
    # Fail batch if any merge failed
    if errors:
        raise Exception(f"Merge failures: {errors}")
```

## Monitoring

### Track Merge Performance

```python
import time
from datetime import datetime

def monitored_parallel_merge(batch_df, batch_id):
    """Track merge performance per table"""
    
    batch_df.cache()
    start_time = time.time()
    
    def merge_with_timing(table_name, merge_key):
        table_start = time.time()
        target = DeltaTable.forName(spark, table_name)
        source = batch_df.alias("source")
        
        (target.alias("target")
            .merge(
                source,
                f"target.{merge_key} = source.{merge_key}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        
        table_duration = time.time() - table_start
        print(f"{table_name}: {table_duration:.2f}s")
        return table_name
    
    tables = [
        ("silver.customers", "customer_id"),
        ("silver.orders", "order_id")
    ]
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(merge_with_timing, table_name, merge_key): table_name
            for table_name, merge_key in tables
        }
        
        for future in as_completed(futures):
            future.result()
    
    total_duration = time.time() - start_time
    print(f"Total parallel merge time: {total_duration:.2f}s")
    
    batch_df.unpersist()
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **Merge conflicts** | Concurrent updates to same rows | Enable Row-Level Concurrency |
| **Slow merges** | Large files, no optimization | Enable Liquid Clustering; Z-Order on merge key |
| **High P99 latency** | OPTIMIZE pauses | Enable Liquid Clustering (no pauses needed) |
| **Too many threads** | Resource contention | Reduce max_workers; match to cluster capacity |
| **Partial failures** | One merge fails | Collect all errors; fail batch if any error |

## Production Best Practices

### Table Configuration

```sql
-- Enable for all target tables
ALTER TABLE target_table SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = true,
    'delta.enableRowLevelConcurrency' = true,
    'delta.liquid.clustering' = true,
    'delta.targetFileSize' = '128mb'
);

-- Z-Order on merge keys
OPTIMIZE target_table ZORDER BY (merge_key);
```

### Thread Management

```python
# Start conservative (2 workers), increase based on monitoring
max_workers = min(len(tables), max(2, total_cores // 2))

# Monitor cluster CPU/memory
# Adjust based on target system capacity
```

### Idempotent Writes

```python
# For audit logs or append-only tables
(audit_df.write
    .format("delta")
    .mode("append")
    .option("txnVersion", batch_id)
    .option("txnAppId", "merge_job_audit")
    .saveAsTable("audit.merge_log")
)
```

## Production Checklist

- [ ] Liquid Clustering + DV + RLC enabled on all target tables
- [ ] Z-Ordering configured on merge keys
- [ ] Optimal thread count configured (start with 2)
- [ ] Error handling implemented (collect all errors)
- [ ] Performance monitoring per table
- [ ] Cache used to avoid recomputation
- [ ] Unpersist after writes
- [ ] Checkpoint location is unique and persistent
- [ ] File size tuned (128MB target)

## Related Skills

- `write-multiple-tables` - Multi-sink write patterns
- `merge-performance` - MERGE optimization strategies
- `stream-static-joins` - Enrichment before merge
- `checkpoint-best-practices` - Checkpoint configuration
