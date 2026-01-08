#!/usr/bin/env python3
"""
Integration test for build_pipeline - requires Databricks authentication.
Run this after authenticating with: databricks auth login
"""
import sys
import json
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from databricks_ai_functions.sdp.build_pipeline import build_pipeline
from databricks.sdk import WorkspaceClient


def check_authentication():
    """Check if Databricks authentication is available."""
    try:
        w = WorkspaceClient()
        print(f"✅ Authenticated to: {w.config.host}")
        return True
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        print("\nTo authenticate, run:")
        print("  databricks auth login --host <your-databricks-host>")
        return False


def test_simple_pipeline():
    """Test creating a simple bronze-silver-gold pipeline."""
    print("\n" + "=" * 80)
    print("Integration Test: Simple bronze-silver-gold pipeline")
    print("=" * 80)
    
    user_request = """
    Create a simple data quality pipeline that:
    1. Creates a bronze layer from the sample NYC taxi data (main.samples.nyctaxi.trips)
    2. Cleans it in a silver layer (remove nulls, filter invalid data)
    3. Creates a gold layer with summary statistics
    
    Target schema: main.ai_dev_kit
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
            start_run=True,  # Start the pipeline automatically
        )
        
        print("\n" + "=" * 80)
        print("✅ PIPELINE CREATED SUCCESSFULLY!")
        print("=" * 80)
        print(json.dumps(result, indent=2))
        print(f"\n🔗 View pipeline at:")
        print(f"   {result.get('pipeline_url')}")
        print(f"\n📊 Details:")
        print(f"   Pipeline ID: {result.get('pipeline_id')}")
        print(f"   Update ID: {result.get('update_id')}")
        print(f"   Created: {result.get('created')}")
        print(f"   Status: {result.get('status')}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_streaming_pipeline():
    """Test creating a streaming pipeline (without starting it)."""
    print("\n" + "=" * 80)
    print("Integration Test: Streaming pipeline (create only)")
    print("=" * 80)
    
    user_request = """
    Create a streaming pipeline for real-time event processing:
    1. Ingest streaming data using STREAMING TABLE
    2. Apply transformations in real-time
    3. Create aggregated metrics
    
    Use the NYC taxi sample data as source: main.samples.nyctaxi.trips
    Target schema: main.ai_dev_kit
    """
    
    try:
        print("\n📋 Request:")
        print(user_request)
        print("\n🚀 Creating pipeline (not starting)...")
        
        result = build_pipeline(
            user_request=user_request,
            catalog="main",
            schema="ai_dev_kit",
            model="databricks-gpt-5-2",
            start_run=False,  # Don't start automatically
        )
        
        print("\n" + "=" * 80)
        print("✅ PIPELINE CREATED (not started)")
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


def main():
    """Run integration tests."""
    print("\n" + "=" * 80)
    print("🧪 BUILD_PIPELINE INTEGRATION TESTS")
    print("=" * 80)
    print("\nThis test requires Databricks authentication.")
    
    if not check_authentication():
        return 1
    
    results = []
    
    # Test 1: Simple pipeline
    results.append(("Simple Pipeline", test_simple_pipeline()))
    
    # Test 2: Streaming pipeline  
    results.append(("Streaming Pipeline", test_streaming_pipeline()))
    
    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{status}: {test_name}")
    
    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)
    
    print(f"\nTotal: {passed_count}/{total_count} tests passed")
    
    if passed_count == total_count:
        print("\n🎉 All integration tests passed!")
        print("\nNext steps:")
        print("  1. Check the Databricks UI for the created pipelines")
        print("  2. Verify the pipeline configurations")
        print("  3. Review the generated SQL transformations")
    
    return 0 if passed_count == total_count else 1


if __name__ == "__main__":
    sys.exit(main())

