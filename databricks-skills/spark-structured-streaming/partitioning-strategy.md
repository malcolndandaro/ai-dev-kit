---
name: partitioning-strategy-streaming
description: Design optimal partitioning strategies for streaming Delta tables. Use when creating new streaming tables, optimizing query performance, handling late-arriving data, or choosing between partitioning and Liquid Clustering for high-cardinality columns.
---

# Partitioning Strategy for Streaming

Design partitioning strategies that balance query performance, file management, and streaming characteristics. Choose between traditional partitioning and Liquid Clustering based on data patterns.

## Quick Start

```python
from pyspark.sql.functions import col

# Time-based partitioning (most common)
(df
    .withColumn("date", col("timestamp").cast("date"))
    .writeStream
    .partitionBy("date")
    .start("/delta/events")
)

# Liquid Clustering (for high-cardinality)
spark.sql("""
    CREATE TABLE events (
        user_id STRING,
        event_type STRING,
        timestamp TIMESTAMP
    ) USING DELTA
    CLUSTER BY (user_id)
""")
```

## Partitioning Strategies

### Time-Based Partitioning

Most common for streaming - partition by date:

```python
# Partition by date for time-series data
(df
    .withColumn("date", col("timestamp").cast("date"))
    .writeStream
    .partitionBy("date")
    .start("/delta/events")
)

# Good for:
# - Time-series queries
# - Retention policies (drop old partitions)
# - Query patterns filtering by date
```

### Low-Cardinality Partitioning

Partition by columns with < 1000 distinct values:

```python
# Partition by region and date
(df
    .withColumn("date", col("timestamp").cast("date"))
    .writeStream
    .partitionBy("region", "date")
    .start("/delta/events")
)

# Good for:
# - Region, country, status columns
# - Query patterns filtering by these columns
# - Rule of thumb: < 100,000 total partitions
```

### Liquid Clustering (No Partitioning)

For high-cardinality columns - use Liquid Clustering instead:

```python
# Create table with Liquid Clustering
spark.sql("""
    CREATE TABLE events (
        user_id STRING,
        event_type STRING,
        timestamp TIMESTAMP
    ) USING DELTA
    CLUSTER BY (user_id)
""")

# Good for:
# - High-cardinality columns (user_id, device_id, UUID)
# - Late-arriving data (no partition boundaries)
# - Flexible query patterns
```

## Decision Matrix

| Data Pattern | Strategy | Example |
|--------------|----------|---------|
| Time-series queries | Date partitioning | IoT, logs, events |
| High-cardinality keys | Liquid Clustering | user_id, device_id |
| Low-cardinality filters | Partition by value | region, country, status |
| Mixed query patterns | Date + Liquid Clustering | date partition + user_id cluster |

## Anti-Patterns

### Don't Partition by High-Cardinality Columns

```python
# WRONG: Partition by user_id (millions of distinct values)
(df
    .writeStream
    .partitionBy("user_id")  # Creates millions of partitions!
    .start("/delta/events")
)

# CORRECT: Use Liquid Clustering
spark.sql("""
    CREATE TABLE events (...) USING DELTA
    CLUSTER BY (user_id)
""")
```

### Don't Create Too Many Small Partitions

```python
# Rule of thumb: < 100,000 partitions total
# Example calculation:
# 10 years × 365 days × 20 countries = 73,000 partitions ✅

# If approaching limit:
# - Reduce partition columns
# - Use Liquid Clustering for high-cardinality
```

### Don't Partition by Unused Columns

```python
# WRONG: Partition by column not used in queries
(df
    .writeStream
    .partitionBy("unused_column")  # No query benefit
    .start("/delta/events")
)

# CORRECT: Partition by columns used in WHERE clauses
# Query: SELECT * FROM events WHERE date = '2024-01-01'
# Partition: partitionBy("date")
```

## Query Pattern Alignment

Partition by columns used in WHERE clauses:

```sql
-- Good: Partition by date, query filters by date
-- Scans only relevant partition
SELECT * FROM events WHERE date = '2024-01-01'

-- Bad: Partition by user_id, query filters by date
-- Scans all partitions
SELECT * FROM events WHERE date = '2024-01-01'
```

## Streaming-Specific Considerations

### Small Files Problem

Streaming creates many small files:

```python
# Solution 1: Enable auto-optimize
spark.sql("""
    ALTER TABLE events SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = true,
        'delta.autoOptimize.autoCompact' = true
    )
""")

# Solution 2: Periodic OPTIMIZE
spark.sql("OPTIMIZE events")
```

### Late-Arriving Data

Late data may write to old partitions:

```python
# Traditional partitioning: Late data writes to old partition
# - May create small files in old partitions
# - Requires periodic OPTIMIZE

# Liquid Clustering: Better for late data
# - No partition boundaries
# - Automatic optimization
# - Handles late-arriving data gracefully
```

### Checkpoint Growth

More partitions = more metadata:

```python
# Monitor checkpoint size
# More partitions increase checkpoint metadata
# Consider Liquid Clustering if partition count is high
```

## File Size Target

Configure target file size for optimal performance:

```sql
-- Aim for 128MB files in partitions
ALTER TABLE events SET TBLPROPERTIES (
    'delta.targetFileSize' = '134217728'  -- 128MB
);

-- Enable auto-optimize to maintain file size
ALTER TABLE events SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = true,
    'delta.autoOptimize.autoCompact' = true
);
```

## Production Patterns

### Pattern 1: Time-Series with Region

```python
# Partition by date and region
(df
    .withColumn("date", col("timestamp").cast("date"))
    .writeStream
    .partitionBy("date", "region")
    .start("/delta/events")
)

# Query pattern:
# SELECT * FROM events WHERE date = '2024-01-01' AND region = 'NA'
```

### Pattern 2: High-Cardinality with Liquid Clustering

```python
# Use Liquid Clustering for user_id
spark.sql("""
    CREATE TABLE user_events (
        user_id STRING,
        event_type STRING,
        timestamp TIMESTAMP,
        payload STRING
    ) USING DELTA
    CLUSTER BY (user_id)
""")

# Query pattern:
# SELECT * FROM user_events WHERE user_id = 'user123'
# Liquid Clustering optimizes automatically
```

### Pattern 3: Hybrid Approach

```python
# Partition by date, cluster by user_id
(df
    .withColumn("date", col("timestamp").cast("date"))
    .writeStream
    .partitionBy("date")
    .start("/delta/events")
)

# Then enable Liquid Clustering
spark.sql("""
    ALTER TABLE events SET TBLPROPERTIES (
        'delta.liquid.clustering' = true
    )
""")
spark.sql("ALTER TABLE events CLUSTER BY (user_id)")
```

## Monitoring

### Check Partition Count

```python
# Count partitions
partition_count = spark.sql("""
    SELECT COUNT(DISTINCT date, region) as partition_count
    FROM events
""").collect()[0]['partition_count']

print(f"Partition count: {partition_count}")
# Alert if > 100,000
```

### Check File Sizes

```python
# Check file sizes per partition
spark.sql("""
    SELECT 
        date,
        COUNT(*) as file_count,
        AVG(size) as avg_file_size_mb,
        MIN(size) as min_file_size_mb,
        MAX(size) as max_file_size_mb
    FROM (
        SELECT 
            date,
            length(content) / (1024*1024) as size
        FROM delta.`/delta/events`
    )
    GROUP BY date
    ORDER BY date DESC
""").show()
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **Too many partitions** | High-cardinality partitioning | Use Liquid Clustering instead |
| **Small files** | Streaming creates many small files | Enable auto-optimize or periodic OPTIMIZE |
| **Slow queries** | Partitioning doesn't match query patterns | Align partitions with WHERE clauses |
| **Late data issues** | Late data writes to old partitions | Consider Liquid Clustering |

## Production Checklist

- [ ] Partition count < 100,000
- [ ] Partitions align with query WHERE clauses
- [ ] File size target configured (128MB)
- [ ] Auto-optimize enabled for streaming
- [ ] Liquid Clustering considered for high-cardinality
- [ ] Partition monitoring configured
- [ ] Late data handling strategy defined

## Related Skills

- `merge-performance` - MERGE optimization with partitioning
- `write-multiple-tables` - Multi-sink patterns with partitioning
- `checkpoint-best-practices` - Checkpoint management
