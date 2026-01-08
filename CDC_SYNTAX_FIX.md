# CDC Syntax Fixed ✅

## What Was Wrong

The LLM was generating **mixed/incorrect syntax** for CDC flows:

```sql
❌ INCORRECT (what LLM was generating):
CREATE FLOW customers_scd1_flow7 AS
AUTO CDC INTO customers_iceberg7
FROM STREAM(silver_customers_clean7)
KEYS (customer_id)
SEQUENCE BY created_at
COLUMNS * EXCEPT (_rescued_data)
STORED AS SCD TYPE 1;
```

**Problems:**
1. `AUTO CDC INTO` is not valid SQL syntax (it's a doc reference name)
2. `STREAM()` function doesn't exist - should be just the table name
3. Mixed documentation terminology with actual SQL

## Correct Syntax

```sql
✅ CORRECT SCD Type 1:
CREATE FLOW customers_scd1_flow7 AS
APPLY CHANGES INTO customers_iceberg7
FROM silver_customers_clean7
KEYS (customer_id)
SEQUENCE BY created_at
COLUMNS * EXCEPT (_rescued_data)
STORED AS SCD TYPE 1;
```

```sql
✅ CORRECT SCD Type 2:
CREATE FLOW customers_scd2_flow7 AS
APPLY CHANGES INTO customers_history7
FROM silver_customers_clean7
KEYS (customer_id)
SEQUENCE BY created_at
COLUMNS * EXCEPT (_rescued_data)
STORED AS SCD TYPE 2
TRACK HISTORY ON *;
```

## What Was Fixed

Updated all CDC examples in the **embedded skills** at:
```
databricks-ai-functions/databricks_ai_functions/skills/sdp/
├── SKILL.md ✅
├── scd-query-patterns.md ✅
├── streaming-patterns.md ✅
└── dlt-migration-guide.md ✅
```

**All changed from:**
- ❌ `AUTO CDC INTO ... FROM stream(table)`

**To:**
- ✅ `APPLY CHANGES INTO ... FROM table`

## Why It Matters

These skills are now **embedded directly in the Python package**. When you:
1. Build the `databricks-ai-functions` wheel
2. Deploy the model serving endpoint
3. The LLM calls `build_pipeline()`

The LLM will now retrieve **correct CDC syntax** from these embedded skills.

## Next Steps

1. **Re-run the test:**
   ```
   /Workspace/Users/cal.reynolds@databricks.com/ai-dev-kit/databricks-ai-functions/test_embedded_skills
   ```

2. **Expected result:** The LLM should generate `APPLY CHANGES INTO` syntax (not `AUTO CDC INTO` or `STREAM()`)

3. **If successful:** Deploy using the same approach (the wheel now has correct skills)

## References

- **Official Databricks CDC Docs:** https://docs.databricks.com/aws/en/ldp/cdc
- **SQL Syntax:** `CREATE FLOW ... APPLY CHANGES INTO`
- **Key Point:** `FROM table_name` (no `STREAM()` function, no `AUTO CDC INTO`)

