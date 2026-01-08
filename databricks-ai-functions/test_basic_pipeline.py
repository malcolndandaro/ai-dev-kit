#!/usr/bin/env python3
"""
Basic test - Simple bronze-silver-gold pipeline without SCD/CDC complexity.

This test can run on:
- Local machine (with PYTHONPATH set)
- Databricks notebooks (with sys.path modifications)
- Databricks jobs (with proper library configuration)
"""
import sys
import json
from pathlib import Path

# Auto-detect if running in Databricks or locally
try:
    # Try Databricks notebook magic
    dbutils  # type: ignore
    IN_DATABRICKS = True
    # Add workspace paths if they exist
    workspace_paths = [
        '/Workspace/Shared/ai-dev-kit/databricks-tools-core',
        '/Workspace/Shared/ai-dev-kit/databricks-ai-functions',
    ]
    for path in workspace_paths:
        if Path(path).exists():
            sys.path.insert(0, path)
except NameError:
    IN_DATABRICKS = False
    # Local development - use relative paths
    # This file is in: ai-dev-kit/databricks-ai-functions/test_basic_pipeline.py
    # We need to add: ai-dev-kit/databricks-tools-core (to import databricks_tools_core.*)
    #                  ai-dev-kit/databricks-ai-functions (to import databricks_ai_functions.*)
    project_root = Path(__file__).parent.parent  # ai-dev-kit/
    sys.path.insert(0, str(project_root / "databricks-tools-core"))
    sys.path.insert(0, str(project_root / "databricks-ai-functions"))

from databricks_ai_functions.sdp.build_pipeline import build_pipeline


def test_basic_pipeline():
    """Test creating a simple bronze-silver-gold pipeline."""
    print("=" * 80)
    print("Basic Test: Simple Bronze-Silver-Gold Pipeline")
    print("=" * 80)
    print(f"Running in: {'Databricks' if IN_DATABRICKS else 'Local'}")
    
    user_request = """
    Create a simple data processing pipeline:
    
    1. Bronze: Load raw data from samples.tpch.orders
    2. Silver: Clean the data - remove nulls and filter for valid orders
    3. Gold: Create daily aggregations - count orders and sum totals by order date
    
    Target: main.ai_dev_kit
    
    Keep it simple - just basic transformations, no CDC or SCD.
    """
    
    try:
        print("\n📋 Request:")
        print(user_request)
        print("\n🔍 Fetching source table schema...")
        print("\n🚀 Building simple pipeline...")
        
        result = build_pipeline(
            user_request=user_request,
            catalog="main",
            schema="ai_dev_kit",
            model="databricks-gpt-5-2",
            start_run=True,
        )
        
        print("\n" + "=" * 80)
        print("✅ PIPELINE CREATED!")
        print("=" * 80)
        print(json.dumps(result, indent=2))
        print(f"\n🔗 View pipeline at:")
        print(f"   {result.get('pipeline_url')}")
        
        print(f"\n✅ Expected:")
        print(f"   - Bronze: CREATE OR REFRESH STREAMING TABLE ... FROM STREAM samples.tpch.orders")
        print(f"   - Silver: CREATE OR REFRESH STREAMING TABLE ... FROM STREAM bronze_orders WHERE ...")
        print(f"   - Gold: CREATE OR REFRESH MATERIALIZED VIEW ... GROUP BY order_date")
        
        if IN_DATABRICKS:
            # Create clickable link in Databricks
            try:
                displayHTML(f'<h3><a href="{result["pipeline_url"]}" target="_blank">🔗 Open Pipeline</a></h3>')  # type: ignore
            except:
                pass
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_basic_pipeline()
    sys.exit(0 if success else 1)
