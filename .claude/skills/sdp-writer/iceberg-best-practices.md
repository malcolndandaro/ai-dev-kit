# Apache Iceberg Best Practices for SDP

## Overview

This guide covers best practices for using Apache Iceberg tables with Lakeflow Spark Declarative Pipelines (SDP) on Databricks. Iceberg support is available in Databricks Runtime 16.4 LTS and above.

---

## When to Use Iceberg vs Delta Lake

### Use Iceberg When

- **Multi-engine interoperability required**: Need to share tables with Snowflake, Trino, Flink, Spark, or other Iceberg-compatible engines
- **External client access**: External systems need to query tables using Iceberg REST Catalog API
- **Standardization on open formats**: Organization-wide commitment to Apache Iceberg as the table format
- **Iceberg v3 features needed**: Deletion vectors, row-level lineage, Variant data type

### Use Delta Lake When

- **Real-time streaming pipelines**: Need change data feed (CDF) for incremental processing
- **Lakeflow SDP streaming features**: AUTO CDC, materialized views with incremental refresh
- **Full Databricks platform integration**: Need data profiling, online tables, Lakebase integration
- **Databricks-native workflows**: Primarily using Databricks tools and services

**Default recommendation**: Use Delta Lake for SDP pipelines unless you have specific requirements for multi-engine interoperability or Iceberg-specific features.

---

## Key Limitations with Iceberg in SDP

### Critical Constraint: No Change Data Feed (CDF)

**Iceberg tables do NOT support change data feed**, which has significant implications for SDP:

```sql
-- ❌ NOT SUPPORTED: Streaming tables cannot read from Iceberg sources incrementally
CREATE OR REPLACE STREAMING TABLE silver_orders AS
SELECT * FROM STREAM iceberg_catalog.schema.bronze_orders;  -- FAILS

-- ❌ NOT SUPPORTED: Materialized views cannot incrementally refresh from Iceberg
CREATE OR REPLACE MATERIALIZED VIEW gold_summary AS
SELECT customer_id, SUM(amount) AS total
FROM iceberg_catalog.schema.silver_orders  -- Cannot use incremental refresh
GROUP BY customer_id;
```

**Impact**:
- Streaming tables **cannot** consume Iceberg tables as sources
- Materialized views **cannot** use incremental refresh with Iceberg sources (full recomputation required)
- AUTO CDC flows **cannot** write to Iceberg tables
- Change tracking and time travel queries are limited

### Additional Unsupported Features

- **Data profiling operations**: Cannot use Databricks data profiling on Iceberg tables
- **Online tables**: Cannot create online tables from Iceberg sources
- **Lakebase integration**: Not available for Iceberg tables
- **Data classification**: Automatic data classification not supported

### Mandatory Requirements

- **Predictive optimization**: Must be enabled for managed Iceberg table maintenance
- **Unity Catalog**: Required for managed Iceberg tables
- **Parquet format**: Iceberg tables only support Apache Parquet file format
- **DBR 16.4 LTS or above**: Required for managed Iceberg table support

---

## Recommended Architecture Patterns

### Pattern 1: Delta for Pipelines, Iceberg for Sharing

Use Delta Lake for SDP pipelines, then share as Iceberg to external systems:

```sql
-- Bronze/Silver/Gold layers use Delta Lake (streaming + AUTO CDC)
CREATE OR REPLACE STREAMING TABLE catalog.schema.bronze_orders AS
SELECT * FROM read_files('/data/orders/', format => 'json');

CREATE OR REPLACE STREAMING TABLE catalog.schema.silver_orders AS
SELECT * FROM STREAM catalog.schema.bronze_orders
WHERE order_id IS NOT NULL;

CREATE OR REPLACE MATERIALIZED VIEW catalog.schema.gold_daily_sales AS
SELECT DATE(order_date) AS sale_date, SUM(amount) AS total
FROM catalog.schema.silver_orders
GROUP BY DATE(order_date);

-- Share to external Iceberg clients (Snowflake, Trino, Flink)
-- Use Delta Sharing to expose Delta tables to Iceberg engines with zero-copy access
```

**Benefits**:
- Full SDP feature set (streaming, AUTO CDC, incremental MVs)
- External systems access via Iceberg REST Catalog API
- No data duplication (zero-copy sharing)
- Best of both worlds

### Pattern 2: Iceberg for Ingestion, Delta for Processing

Ingest data into Iceberg (from external systems), then copy to Delta for SDP processing:

```sql
-- External system writes to Iceberg table
-- (Created and maintained by Flink, Trino, or other Iceberg engine)

-- Batch copy from Iceberg to Delta for SDP processing
CREATE OR REFRESH MATERIALIZED VIEW catalog.schema.bronze_orders_delta AS
SELECT * FROM iceberg_catalog.external_schema.orders;  -- Full scan

-- Now use Delta table in streaming pipeline
CREATE OR REPLACE STREAMING TABLE catalog.schema.silver_orders AS
SELECT * FROM STREAM catalog.schema.bronze_orders_delta
WHERE order_id IS NOT NULL;
```

**When to use**:
- External systems write to Iceberg
- Need SDP features for downstream processing
- Can tolerate batch synchronization delay

**Limitation**: The copy from Iceberg to Delta requires full scans (no incremental processing).

### Pattern 3: Iceberg-Only for Batch Workloads

Use Iceberg throughout when streaming features are not required:

```sql
-- Batch ingestion to Iceberg
CREATE OR REPLACE TABLE iceberg_catalog.schema.orders
USING iceberg
CLUSTER BY (order_date, region)
AS
SELECT * FROM read_files('/data/orders/', format => 'parquet');

-- Batch transformations
CREATE OR REPLACE TABLE iceberg_catalog.schema.silver_orders
USING iceberg
CLUSTER BY (customer_id, order_date)
AS
SELECT
  order_id,
  customer_id,
  CAST(amount AS DECIMAL(10,2)) AS amount,
  CAST(order_date AS DATE) AS order_date
FROM iceberg_catalog.schema.orders
WHERE order_id IS NOT NULL;

-- Batch aggregations (full recomputation on refresh)
CREATE OR REPLACE MATERIALIZED VIEW iceberg_catalog.schema.gold_summary
CLUSTER BY (order_day)
AS
SELECT
  DATE(order_date) AS order_day,
  SUM(amount) AS daily_sales
FROM iceberg_catalog.schema.silver_orders
GROUP BY DATE(order_date);
```

**When to use**:
- Batch-only processing (no real-time requirements)
- Multi-engine access required (Snowflake, Trino, etc.)
- No need for AUTO CDC or streaming tables

**Limitation**: No incremental processing - materialized views do full recomputation.

---

## Iceberg-Specific Best Practices

### Use Liquid Clustering

Liquid clustering is available in Public Preview for managed Iceberg tables (DBR 16.4 LTS+):

```sql
CREATE OR REPLACE TABLE iceberg_catalog.schema.events
USING iceberg
CLUSTER BY (event_type, event_date)
AS
SELECT * FROM source;

-- To enable clustering on existing Iceberg table from external engines:
-- Specify partition columns during creation, Unity Catalog interprets them as clustering keys

-- Force reclustering after enabling
OPTIMIZE iceberg_catalog.schema.events FULL;
```

**Important**: When enabling liquid clustering on existing Iceberg tables:
- Must explicitly disable deletion vectors
- Must disable Row IDs
- Use `OPTIMIZE FULL` to recluster all records

### Upgrade to Iceberg v3

Use Iceberg v3 features (available in Beta in DBR 17.3+):

```sql
-- Iceberg v3 provides:
-- - Deletion vectors (10x faster updates without rewriting Parquet files)
-- - Row-level lineage
-- - Variant data type support

ALTER TABLE iceberg_catalog.schema.orders
SET TBLPROPERTIES ('format-version' = '3');
```

**Benefits**:
- Deletion vectors enable fast updates and deletes
- Compared to regular MERGE statements, up to 10x speedup
- Deletes stored as separate files, merged during reads

### Use Recommended Iceberg Client Versions

For external engines accessing Unity Catalog:

- **Iceberg clients**: Version 1.9.2 or above recommended
- **Apache Spark**: Compatible with Iceberg REST spec
- **Apache Flink**: Compatible with Iceberg REST spec
- **Trino**: Compatible with Iceberg REST spec

### Enable Predictive Optimization

Predictive optimization is **mandatory** for managed Iceberg tables:

```sql
-- Automatically enabled for managed Iceberg tables in Unity Catalog
-- Handles:
-- - Snapshot expiration
-- - Unreferenced file deletion
-- - Incremental clustering (if Liquid Clustering enabled)
```

**Benefits**:
- Reduces storage costs
- Improves query performance
- Automates maintenance tasks

### Share Iceberg Tables with Delta Sharing

Share Iceberg tables to external Iceberg clients (December 2025 feature):

```sql
-- Create managed Iceberg table
CREATE TABLE iceberg_catalog.schema.sales
USING iceberg
CLUSTER BY (region, sale_date)
AS SELECT * FROM source;

-- Share to external Iceberg clients (Snowflake, Trino, Flink, Spark)
-- Use Delta Sharing for:
-- - Databricks-to-Databricks sharing
-- - Open sharing with external Iceberg engines
-- - Zero-copy access for external clients
```

**Use cases**:
- Share streaming table outputs to external analytics platforms
- Provide governed data access to partner organizations
- Enable cross-platform analytics without data duplication

---

## Performance Optimization

### Clustering Key Selection

Same principles as Delta Lake, with some nuances:

```sql
-- Good: High-cardinality leading keys
CREATE TABLE iceberg_catalog.schema.events
USING iceberg
CLUSTER BY (user_id, event_date, event_type)  -- user_id most selective
AS SELECT * FROM source;

-- Limit to 4 clustering keys for optimal performance
```

**Iceberg-specific consideration**: External engines may have different query patterns. Choose clustering keys based on the most common access patterns across all engines.

### File Format Optimization

Iceberg only supports Parquet:

```sql
-- Parquet format automatically used
CREATE TABLE iceberg_catalog.schema.data
USING iceberg
AS SELECT * FROM source;

-- Ensure optimal Parquet file sizes (128MB-1GB recommended)
```

### Manual Optimization

```sql
-- Run OPTIMIZE FULL after major changes or initial clustering
OPTIMIZE iceberg_catalog.schema.events FULL;

-- Regular maintenance (predictive optimization handles this automatically)
OPTIMIZE iceberg_catalog.schema.events;
```

---

## Migration from Delta Lake to Iceberg

### When to Migrate

Only migrate when you have clear requirements:
- External Iceberg engines need direct table access
- Organization-wide Iceberg standardization
- Iceberg v3 features provide significant value

**Do NOT migrate** if:
- Using streaming tables or AUTO CDC flows
- Need incremental materialized view refresh
- Rely on change data feed
- Require full Databricks platform integration

### Migration Approach

**Option 1: UniForm (Recommended)**

Use Delta Lake Universal Format (UniForm) for Iceberg compatibility:

```sql
-- Enable UniForm on existing Delta table
ALTER TABLE catalog.schema.orders
SET TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- Table is now readable by Iceberg engines while maintaining Delta Lake features
```

**Benefits**:
- Keep Delta Lake features (CDF, streaming, AUTO CDC)
- External Iceberg engines can read the table
- No data duplication
- Backward compatible

**Option 2: Full Migration**

Create new Iceberg table and migrate data:

```sql
-- Create Iceberg table
CREATE TABLE iceberg_catalog.schema.orders_iceberg
USING iceberg
CLUSTER BY (order_date, region)
AS
SELECT * FROM catalog.schema.orders_delta;

-- Test with external engines
-- Update references
-- Validate functionality
-- Drop old Delta table when complete
```

**Caution**: Lose streaming, AUTO CDC, and incremental refresh capabilities.

---

## Common Issues and Solutions

### Issue: Cannot create streaming table from Iceberg source
**Cause**: Iceberg does not support change data feed
**Solution**: Use Pattern 1 (Delta for pipelines, share as Iceberg) or Pattern 2 (batch copy to Delta)

### Issue: Materialized view always does full recomputation
**Cause**: Iceberg tables do not support incremental refresh
**Solution**: Accept full recomputation or migrate to Delta Lake for incremental processing

### Issue: External engine cannot read Iceberg table
**Cause**: Client version incompatible or REST Catalog API not configured
**Solution**: Upgrade to Iceberg client 1.9.2+, verify REST Catalog API configuration

### Issue: Liquid clustering not working on Iceberg table
**Cause**: Deletion vectors or Row IDs not disabled
**Solution**: Explicitly disable deletion vectors and Row IDs, run `OPTIMIZE FULL`

### Issue: Poor query performance on Iceberg table
**Cause**: Not clustered or not optimized
**Solution**: Add `CLUSTER BY` clause, run `OPTIMIZE FULL`, enable predictive optimization

---

## Decision Matrix: Delta vs Iceberg for SDP

| Feature | Delta Lake | Iceberg | Recommendation |
|---------|-----------|---------|----------------|
| **Streaming tables** | ✅ Full support | ❌ Not supported | Use Delta |
| **AUTO CDC flows** | ✅ Full support | ❌ Not supported | Use Delta |
| **Incremental MV refresh** | ✅ Supported | ❌ Full recomputation | Use Delta |
| **Change data feed** | ✅ Supported | ❌ Not supported | Use Delta |
| **Liquid Clustering** | ✅ GA | ✅ Public Preview | Either |
| **Multi-engine access** | ⚠️ Via UniForm | ✅ Native | Iceberg or UniForm |
| **External client writes** | ❌ Limited | ✅ Full support | Iceberg |
| **Deletion vectors** | ✅ Supported | ✅ v3 only | Either |
| **Data profiling** | ✅ Supported | ❌ Not supported | Use Delta |
| **Online tables** | ✅ Supported | ❌ Not supported | Use Delta |
| **Predictive optimization** | ✅ Supported | ✅ Mandatory | Either |
| **Databricks platform integration** | ✅ Full | ⚠️ Partial | Use Delta |

---

## Best Practices Summary

### For SDP Pipelines
- ✅ Use Delta Lake as default for SDP pipelines (streaming, AUTO CDC, incremental MVs)
- ✅ Share Delta tables as Iceberg via Delta Sharing or UniForm for external access
- ✅ Only use Iceberg directly for batch-only workloads with multi-engine requirements

### For Iceberg Tables
- ✅ Use Liquid Clustering for query performance
- ✅ Upgrade to Iceberg v3 for deletion vectors (10x faster updates)
- ✅ Use Iceberg client 1.9.2+ for Unity Catalog access
- ✅ Enable predictive optimization (mandatory for managed tables)
- ✅ Choose clustering keys based on cross-engine query patterns

### For Interoperability
- ✅ Use Delta Lake Universal Format (UniForm) to maintain Delta features while providing Iceberg compatibility
- ✅ Use Delta Sharing to share tables with external Iceberg clients (zero-copy access)
- ✅ Access Iceberg tables via REST Catalog API from Spark, Flink, Trino, Snowflake

### For Migration
- ✅ Use UniForm instead of full migration when possible
- ✅ Only migrate to Iceberg if multi-engine access is critical and streaming features are not needed
- ❌ Do NOT migrate if using streaming tables, AUTO CDC, or incremental materialized views

---

## References

- [Databricks Apache Iceberg Documentation](https://docs.databricks.com/aws/en/iceberg/)
- [Databricks Liquid Clustering for Iceberg](https://docs.databricks.com/aws/en/delta/clustering)
- [Delta Lake Universal Format (UniForm) for Iceberg](https://www.databricks.com/blog/delta-lake-universal-format-uniform-iceberg-compatibility-now-ga)
- [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/)
- [Apache Iceberg v3 on Databricks](https://www.databricks.com/blog/advancing-lakehouse-apache-iceberg-v3-databricks)
- [Iceberg REST Catalog API](https://docs.databricks.com/aws/en/iceberg/)
