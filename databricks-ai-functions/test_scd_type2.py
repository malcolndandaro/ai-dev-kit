#!/usr/bin/env python3
"""
Test SCD Type 2 with Iceberg on SDP - should generate AUTO CDC flow.
"""
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from databricks_ai_functions.sdp.build_pipeline import build_pipeline


def test_scd_type2_pipeline():
    """Test creating an SCD Type 2 pipeline with AUTO CDC."""
    print("=" * 80)
    print("Testing: SCD Type 2 with AUTO CDC on Iceberg")
    print("=" * 80)
    
    user_request = """
    Create a customer data pipeline with SCD Type 2 history tracking:
    
    1. Bronze: Ingest customer updates from samples.tpch.customer as a streaming table
    2. Silver: Apply AUTO CDC to track customer history with SCD Type 2
       - Use c_custkey as the primary key
       - Track history on all columns
       - Use row version or timestamp for sequencing
    3. Gold: Create a current view showing only the latest version of each customer
    
    Target: main.ai_dev_kit
    
    Requirements:
    - Use Iceberg table format (default for Unity Catalog)
    - Implement SCD Type 2 with AUTO CDC FLOW
    - Track __START_AT, __END_AT for history
    """
    
    try:
        print("\n📋 Request:")
        print(user_request)
        print("\n🔍 Fetching source table schema...")
        print("\n🚀 Building SCD Type 2 pipeline...")
        
        result = build_pipeline(
            user_request=user_request,
            catalog="main",
            schema="ai_dev_kit",
            model="databricks-gpt-5-2",
            start_run=True,
        )
        
        print("\n" + "=" * 80)
        print("✅ SCD PIPELINE CREATED!")
        print("=" * 80)
        print(json.dumps(result, indent=2))
        print(f"\n🔗 View pipeline at:")
        print(f"   {result.get('pipeline_url')}")
        
        print("\n✅ Expected SQL patterns:")
        print("   - Bronze: CREATE OR REFRESH STREAMING TABLE")
        print("   - Silver: CREATE OR REFRESH STREAMING TABLE (for history)")
        print("   - Silver: CREATE FLOW ... AUTO CDC INTO ... STORED AS SCD TYPE 2")
        print("   - Gold: CREATE OR REFRESH MATERIALIZED VIEW with __END_AT IS NULL")
        print("\n📝 Verify in workspace files that AUTO CDC syntax is correct")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_scd_type2_pipeline()
    sys.exit(0 if success else 1)

