# Databricks notebook source
# MAGIC %md
# MAGIC # AI Dev Kit - Build Pipeline Demo
# MAGIC 
# MAGIC This notebook demonstrates how to use the AI Dev Kit to build Spark Declarative Pipelines (SDP) using natural language.
# MAGIC 
# MAGIC ## Setup
# MAGIC 
# MAGIC 1. Upload the `ai-dev-kit` project to `/Workspace/Shared/ai-dev-kit/`
# MAGIC 2. Run the cells below

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# Install required packages
%pip install databricks-sdk>=0.31.0

# COMMAND ----------

# Restart Python to load new packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Project to Path

# COMMAND ----------

import sys
from pathlib import Path

# Add project directories to Python path
project_paths = [
    '/Workspace/Shared/ai-dev-kit/databricks-tools-core',
    '/Workspace/Shared/ai-dev-kit/databricks-ai-functions',
]

for path in project_paths:
    if path not in sys.path:
        sys.path.insert(0, path)
        print(f"✅ Added: {path}")

# COMMAND ----------

# Verify imports work
from databricks_ai_functions.sdp.build_pipeline import build_pipeline
print("✅ Successfully imported build_pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Simple Bronze-Silver-Gold Pipeline

# COMMAND ----------

result = build_pipeline(
    user_request="""
    Create a simple data processing pipeline:
    
    1. Bronze: Load raw data from samples.tpch.orders
    2. Silver: Clean the data - remove nulls and filter for valid orders
    3. Gold: Create daily aggregations - count orders and sum totals by order date
    
    Keep it simple - just basic transformations.
    """,
    catalog="main",
    schema="default",  # Change to your schema
    model="databricks-gpt-5-2",
    start_run=True,
)

print(result)

# COMMAND ----------

# Display clickable link
displayHTML(f"""
<div style="padding: 20px; background: #f0f9ff; border-left: 4px solid #0066cc;">
  <h2 style="margin-top: 0;">✅ Pipeline Created!</h2>
  <p><strong>Pipeline ID:</strong> {result['pipeline_id']}</p>
  <p><strong>Status:</strong> {result['status']}</p>
  <p><a href="{result['pipeline_url']}" target="_blank" style="
    display: inline-block;
    padding: 10px 20px;
    background: #0066cc;
    color: white;
    text-decoration: none;
    border-radius: 4px;
    margin-top: 10px;
  ">🔗 Open Pipeline in Databricks</a></p>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: SCD Type 2 with Iceberg

# COMMAND ----------

result_scd = build_pipeline(
    user_request="""
    Create a product catalog pipeline with SCD Type 2 history tracking:
    
    1. Bronze: Ingest product updates from samples.tpch.part as a streaming table
    2. Silver: Implement SCD Type 2 using AUTO CDC to track product changes over time
       - Use p_partkey as the primary key
       - Track all column changes in history
       - Sequence by ingestion time
    3. Gold: Create a current products view showing only the latest version of each product
    
    Use Iceberg format with SCD Type 2 for full history tracking.
    """,
    catalog="main",
    schema="default",  # Change to your schema
    model="databricks-gpt-5-2",
    start_run=True,
)

print(result_scd)

# COMMAND ----------

# Display clickable link
displayHTML(f"""
<div style="padding: 20px; background: #f0fff4; border-left: 4px solid #059669;">
  <h2 style="margin-top: 0;">✅ SCD Type 2 Pipeline Created!</h2>
  <p><strong>Pipeline ID:</strong> {result_scd['pipeline_id']}</p>
  <p><strong>Status:</strong> {result_scd['status']}</p>
  <p><a href="{result_scd['pipeline_url']}" target="_blank" style="
    display: inline-block;
    padding: 10px 20px;
    background: #059669;
    color: white;
    text-decoration: none;
    border-radius: 4px;
    margin-top: 10px;
  ">🔗 Open Pipeline in Databricks</a></p>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Custom Pipeline

# COMMAND ----------

# Try your own pipeline!
custom_request = """
[Describe your pipeline requirements here]

For example:
- Load data from your source table
- Apply transformations
- Create aggregations or analytics
"""

# Uncomment to run:
# result_custom = build_pipeline(
#     user_request=custom_request,
#     catalog="main",
#     schema="your_schema",
#     model="databricks-gpt-5-2",
#     start_run=True,
# )
# print(result_custom)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tips
# MAGIC 
# MAGIC 1. **Schema Introspection**: The function automatically fetches source table schemas for accurate column names
# MAGIC 2. **Skills-Driven**: It uses SDP skills documents for correct syntax patterns
# MAGIC 3. **Validation**: Generated SQL is validated before deployment
# MAGIC 4. **Monitoring**: Check pipeline status in the Databricks UI
# MAGIC 
# MAGIC ## Supported Features
# MAGIC 
# MAGIC - ✅ Bronze-Silver-Gold architectures
# MAGIC - ✅ Streaming tables and materialized views
# MAGIC - ✅ SCD Type 2 with AUTO CDC
# MAGIC - ✅ Schema evolution
# MAGIC - ✅ Unity Catalog integration
# MAGIC - ✅ Iceberg table format

