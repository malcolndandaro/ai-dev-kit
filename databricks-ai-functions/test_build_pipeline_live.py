#!/usr/bin/env python3
"""
Test with corrected prompt - using proper 3-part table names.
"""
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from databricks_ai_functions.sdp.build_pipeline import build_pipeline


def test_corrected_references():
    """Test with proper 3-part table reference."""
    print("=" * 80)
    print("Testing: Corrected prompt with 3-part table names")
    print("=" * 80)
    
    # Use a simpler source table that definitely has 3 parts
    user_request = """
    Create a taxi analytics pipeline:
    1. Bronze: Ingest from samples.nyctaxi.trips
    2. Silver: Clean and validate (remove nulls, valid amounts)
    3. Gold: Daily trip summary with counts and revenue
    
    Target: main.ai_dev_kit
    """
    
    try:
        print("\n📋 Request:")
        print(user_request)
        print("\n🚀 Building pipeline...")
        
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
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_corrected_references()
    sys.exit(0 if success else 1)

