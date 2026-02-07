---
name: auto-loader-schema-drift
description: Handle schema drift in Auto Loader with schema evolution and rescue options. Use when ingesting files with evolving schemas, enforcing strict schema validation, capturing unexpected data for audit, or configuring Auto Loader for production file ingestion.
---

# Auto Loader Schema Drift

Handle schema evolution and unexpected data in Auto Loader. Configure schema evolution mode, rescue columns, and schema hints for production file ingestion.

## Quick Start

```python
# Auto Loader with schema evolution
df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load("/path/to/data")
)

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/autoloader") \
    .start("/delta/target")
```

## Schema Evolution Modes

### Mode 1: Add New Columns (Default)

Automatically add new columns to schema:

```python
# Default behavior - add new columns automatically
df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Default
    .load("/path/to/data")
)

# New columns are added as nullable
# Existing columns maintain their types
```

### Mode 2: Rescue Mode

Capture unexpected data in rescue column:

```python
# Put unexpected data in _rescued_data column
df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "_rescued_data")
    .load("/path/to/data")
)

# _rescued_data contains:
# - Columns not in schema
# - Type mismatches
# - Malformed data
```

### Mode 3: Fail on New Columns

Fail when new columns detected:

```python
# Strict schema enforcement
df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
    .load("/path/to/data")
)

# Fails if new columns detected
# Use for strict schema requirements
```

## Common Patterns

### Pattern 1: Evolving Schema with Rescue

Handle evolving schemas while capturing unexpected data:

```python
df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load("/path/to/data")
)

# Process normal data
normal_data = df.filter(col("_rescued_data").isNull())

# Process rescued data separately
rescued_data = df.filter(col("_rescued_data").isNotNull())

# Write normal data
normal_data.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/normal") \
    .start("/delta/normal_data")

# Write rescued data to audit table
rescued_data.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/rescued") \
    .start("/delta/rescued_data")
```

### Pattern 2: Schema Hints

Provide hints for type inference:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Define schema hints
schema_hints = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("value", IntegerType(), True)
])

df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaHints", schema_hints.json())
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/path/to/data")
)
```

### Pattern 3: Strict Schema Validation

Enforce strict schema with rescue for audit:

```python
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define strict schema
strict_schema = StructType([
    StructField("id", StringType(), False),  # Required
    StructField("timestamp", TimestampType(), False),  # Required
    StructField("value", IntegerType(), True)
])

df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schema", strict_schema.json())
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "_rescued_data")
    .load("/path/to/data")
)

# Valid records
valid = df.filter(col("_rescued_data").isNull())

# Invalid records (missing required fields, type mismatches)
invalid = df.filter(col("_rescued_data").isNotNull())
```

## Rescue Column Usage

### Accessing Rescue Data

```python
# Rescue column contains JSON string of unexpected data
rescued_df = df.filter(col("_rescued_data").isNotNull())

# Parse rescue data
from pyspark.sql.functions import from_json

rescue_schema = "map<string,string>"
parsed_rescue = rescued_df.withColumn(
    "rescue_map",
    from_json(col("_rescued_data"), rescue_schema)
)

# Extract specific fields from rescue
parsed_rescue.select(
    col("id"),  # Normal column
    col("rescue_map.new_field"),  # From rescue
    col("rescue_map.another_field")  # From rescue
).show()
```

### Monitoring Rescue Data

```python
# Monitor rescue data volume
rescue_stats = spark.sql("""
    SELECT 
        date_trunc('hour', ingestion_timestamp) as hour,
        COUNT(*) as total_records,
        SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) as rescued_count,
        SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as rescue_rate_pct
    FROM target_table
    GROUP BY 1
    ORDER BY 1 DESC
""")

rescue_stats.show()

# Alert if rescue rate > threshold
```

## Best Practices

### Use addNewColumns for Evolving Schemas

```python
# RECOMMENDED: For most production use cases
df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")  # Capture unexpected data
    .load("/path/to/data")
)
```

### Use Rescue for Strict Enforcement

```python
# For strict schema requirements with audit trail
df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "_rescued_data")
    .load("/path/to/data")
)
```

### Monitor Schema Changes

```python
# Track schema evolution
schema_history = []

def track_schema(batch_df, batch_id):
    current_schema = batch_df.schema
    schema_history.append({
        "batch_id": batch_id,
        "schema": current_schema.json()
    })
    
    # Alert on schema changes
    if len(schema_history) > 1:
        prev_schema = schema_history[-2]["schema"]
        if prev_schema != current_schema.json():
            print(f"Schema changed in batch {batch_id}")

df.writeStream \
    .foreachBatch(track_schema) \
    .option("checkpointLocation", "/checkpoints/schema_tracking") \
    .start()
```

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| **Schema mismatch errors** | New columns not handled | Use addNewColumns or rescue mode |
| **Type inference errors** | Ambiguous types | Provide schema hints |
| **Missing rescue data** | Rescue column not configured | Enable rescuedDataColumn |
| **High rescue rate** | Schema drift or data quality issues | Investigate source data; update schema |

## Production Checklist

- [ ] Schema evolution mode configured (addNewColumns recommended)
- [ ] Rescue column enabled for audit trail
- [ ] Schema hints provided (if needed)
- [ ] Rescue data monitoring configured
- [ ] Alerting on high rescue rates
- [ ] Schema change tracking implemented

## Related Skills

- `kafka-to-delta` - Kafka ingestion patterns
- `error-handling-recovery` - DLQ patterns for rescued data
- `checkpoint-best-practices` - Checkpoint management
