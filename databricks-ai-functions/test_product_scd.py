#!/usr/bin/env python3
"""
Test SCD Type 2 with Iceberg - Product catalog example.
"""
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from databricks_ai_functions.sdp.build_pipeline import build_pipeline


def test_product_scd_pipeline():
    """Test creating a product catalog SCD Type 2 pipeline."""
    print("=" * 80)
    print("Testing: Product Catalog SCD Type 2 with Iceberg")
    print("=" * 80)
    
    user_request = """
    Create a product catalog pipeline with SCD Type 2 history tracking:
    
    1. Bronze: Ingest product updates from samples.tpch.part as a streaming table
    2. Silver: Implement SCD Type 2 using AUTO CDC to track product changes over time
       - Use p_partkey as the primary key
       - Track all column changes in history
       - Sequence by ingestion time or metadata timestamp
    3. Gold: Create a current products view showing only the latest version of each product
    
    Target: main.ai_dev_kit
    
    This will use Iceberg format (Unity Catalog default) with SCD Type 2 for full history tracking.
    """
    
    try:
        print("\n📋 Request:")
        print(user_request)
        print("\n🔍 Fetching source table schema...")
        print("\n🚀 Building product SCD Type 2 pipeline...")
        
        result = build_pipeline(
            user_request=user_request,
            catalog="main",
            schema="ai_dev_kit",
            model="databricks-gpt-5-2",
            start_run=True,
        )
        
        print("\n" + "=" * 80)
        print("✅ PRODUCT SCD PIPELINE CREATED!")
        print("=" * 80)
        print(json.dumps(result, indent=2))
        print(f"\n🔗 View pipeline at:")
        print(f"   {result.get('pipeline_url')}")
        
        # Get pipeline name to check SQL files
        pipeline_id = result.get('pipeline_id')
        print(f"\n📝 To verify SQL syntax, check workspace files at:")
        print(f"   /Workspace/Shared/ai-dev-kit/pipelines/<pipeline_name>/transformations/")
        print(f"\n✅ Expected SQL from skills:")
        print(f"   - CREATE OR REFRESH STREAMING TABLE (bronze)")
        print(f"   - CREATE FLOW ... AS AUTO CDC INTO ... STORED AS SCD TYPE 2 (silver)")
        print(f"   - CREATE OR REFRESH MATERIALIZED VIEW ... WHERE __END_AT IS NULL (gold)")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_product_scd_pipeline()
    sys.exit(0 if success else 1)

