#!/usr/bin/env python3
"""
Unit tests for build_pipeline with mocking to avoid Databricks authentication.
Tests the internal logic without requiring live connections.
"""
import sys
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from databricks_ai_functions.sdp.build_pipeline import (
    build_pipeline,
    _generate_pipeline_plan,
    _materialize_local_files,
)


def test_generate_pipeline_plan():
    """Test that _generate_pipeline_plan produces valid output."""
    print("=" * 80)
    print("Testing: _generate_pipeline_plan function")
    print("=" * 80)
    
    # Mock LLM response
    mock_llm_response = json.dumps({
        "name": "customer_pipeline",
        "catalog": "main",
        "schema": "ai_dev_kit",
        "files": [
            {
                "rel_path": "transformations/01_bronze_customers.sql",
                "language": "SQL",
                "content": "CREATE OR REFRESH MATERIALIZED VIEW bronze_customers AS SELECT * FROM main.samples.customers"
            },
            {
                "rel_path": "transformations/02_silver_customers.sql",
                "language": "SQL",
                "content": "CREATE OR REFRESH MATERIALIZED VIEW silver_customers AS SELECT * FROM bronze_customers WHERE customer_id IS NOT NULL"
            }
        ],
        "start_run": True,
        "full_refresh": True
    })
    
    with patch('databricks_ai_functions.sdp.build_pipeline.call_llm', return_value=mock_llm_response):
        plan = _generate_pipeline_plan(
            user_request="Create a pipeline from customers to bronze to silver",
            skills=[{"path": "test.md", "snippet": "Example skill"}],
            catalog="main",
            schema="ai_dev_kit",
            model="test-model"
        )
    
    print(f"\n✅ Generated plan:")
    print(json.dumps(plan, indent=2))
    
    # Validate plan structure
    assert plan["name"] == "customer_pipeline", "Pipeline name mismatch"
    assert plan["catalog"] == "main", "Catalog mismatch"
    assert plan["schema"] == "ai_dev_kit", "Schema mismatch"
    assert len(plan["files"]) == 2, "Expected 2 files"
    
    # Validate rel_path prefix
    for f in plan["files"]:
        assert f["rel_path"].startswith("transformations/"), f"rel_path must start with transformations/: {f['rel_path']}"
    
    print("✅ All validations passed!")
    return True


def test_materialize_local_files():
    """Test that _materialize_local_files creates correct directory structure."""
    print("\n" + "=" * 80)
    print("Testing: _materialize_local_files function")
    print("=" * 80)
    
    plan = {
        "name": "test_pipeline",
        "catalog": "main",
        "schema": "ai_dev_kit",
        "files": [
            {
                "rel_path": "transformations/01_bronze.sql",
                "content": "CREATE OR REFRESH MATERIALIZED VIEW bronze AS SELECT 1"
            },
            {
                "rel_path": "transformations/02_silver.sql",
                "content": "CREATE OR REFRESH MATERIALIZED VIEW silver AS SELECT * FROM bronze"
            }
        ]
    }
    
    try:
        local_root = _materialize_local_files(plan)
        
        print(f"\n✅ Created local directory: {local_root}")
        print(f"Directory structure:")
        
        # Verify structure
        assert local_root.exists(), "Root directory doesn't exist"
        assert local_root.name == "test_pipeline", "Pipeline folder name mismatch"
        
        transformations_dir = local_root / "transformations"
        assert transformations_dir.exists(), "Transformations directory doesn't exist"
        
        for f in plan["files"]:
            file_path = local_root / f["rel_path"]
            assert file_path.exists(), f"File doesn't exist: {file_path}"
            content = file_path.read_text()
            assert content == f["content"], f"Content mismatch for {file_path}"
            print(f"  ✓ {file_path.relative_to(local_root.parent)}")
        
        print("✅ All files created successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_build_pipeline_integration():
    """Test full build_pipeline with all external calls mocked."""
    print("\n" + "=" * 80)
    print("Testing: Full build_pipeline with mocks")
    print("=" * 80)
    
    # Mock plan from LLM
    mock_plan = {
        "name": "integration_test_pipeline",
        "catalog": "main",
        "schema": "ai_dev_kit",
        "files": [
            {
                "rel_path": "transformations/01_bronze.sql",
                "content": "CREATE OR REFRESH MATERIALIZED VIEW bronze_test AS SELECT * FROM main.samples.test_data"
            }
        ],
        "start_run": True,
        "full_refresh": True
    }
    
    mock_llm_response = json.dumps(mock_plan)
    
    # Mock the various dependencies
    mock_run_result = Mock()
    mock_run_result.success = True
    mock_run_result.pipeline_id = "test-pipeline-123"
    mock_run_result.update_id = "update-456"
    mock_run_result.created = True
    mock_run_result.message = "Pipeline created successfully"
    
    mock_workspace_client = MagicMock()
    mock_workspace_client.config.host = "https://test.databricks.com"
    
    try:
        with patch('databricks_ai_functions.sdp.build_pipeline.load_skills', return_value=[{"path": "test.md", "text": "test skill"}]), \
             patch('databricks_ai_functions.sdp.build_pipeline.retrieve_relevant_skills', return_value=[{"path": "test.md", "snippet": "test snippet"}]), \
             patch('databricks_ai_functions.sdp.build_pipeline.call_llm', return_value=mock_llm_response), \
             patch('databricks_ai_functions.sdp.build_pipeline.upload_folder', return_value={"uploaded": 1}), \
             patch('databricks_ai_functions.sdp.build_pipeline.create_or_update_pipeline', return_value=mock_run_result), \
             patch('databricks_ai_functions.sdp.build_pipeline.WorkspaceClient', return_value=mock_workspace_client):
            
            result = build_pipeline(
                user_request="Create a test pipeline from test_data",
                catalog="main",
                schema="ai_dev_kit",
                model="test-model",
                start_run=True
            )
        
        print(f"\n✅ Pipeline build result:")
        print(json.dumps(result, indent=2))
        
        # Validate result structure
        assert result["status"] == "success", "Status should be success"
        assert result["pipeline_id"] == "test-pipeline-123", "Pipeline ID mismatch"
        assert result["update_id"] == "update-456", "Update ID mismatch"
        assert result["created"] == True, "Created flag mismatch"
        assert "pipeline_url" in result, "Missing pipeline_url"
        
        print("✅ All validations passed!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_rel_path_correction():
    """Test that rel_path is corrected if missing transformations/ prefix."""
    print("\n" + "=" * 80)
    print("Testing: rel_path prefix correction")
    print("=" * 80)
    
    # Mock LLM response with incorrect rel_path
    mock_llm_response = json.dumps({
        "name": "test_pipeline",
        "catalog": "main",
        "schema": "ai_dev_kit",
        "files": [
            {
                "rel_path": "01_bronze.sql",  # Missing transformations/ prefix
                "language": "SQL",
                "content": "SELECT 1"
            }
        ],
        "start_run": True,
        "full_refresh": True
    })
    
    with patch('databricks_ai_functions.sdp.build_pipeline.call_llm', return_value=mock_llm_response):
        plan = _generate_pipeline_plan(
            user_request="test",
            skills=[],
            catalog="main",
            schema="ai_dev_kit",
            model="test-model"
        )
    
    # Check that rel_path was corrected
    assert plan["files"][0]["rel_path"] == "transformations/01_bronze.sql", \
        f"Expected transformations/01_bronze.sql, got {plan['files'][0]['rel_path']}"
    
    print(f"✅ rel_path corrected: {plan['files'][0]['rel_path']}")
    return True


def main():
    """Run all unit tests."""
    print("\n🧪 Starting build_pipeline unit tests (with mocking)...\n")
    
    results = []
    
    try:
        # Test 1: Generate pipeline plan
        results.append(("Generate Pipeline Plan", test_generate_pipeline_plan()))
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        results.append(("Generate Pipeline Plan", False))
    
    try:
        # Test 2: Materialize local files
        results.append(("Materialize Local Files", test_materialize_local_files()))
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        results.append(("Materialize Local Files", False))
    
    try:
        # Test 3: Full integration with mocks
        results.append(("Build Pipeline Integration", test_build_pipeline_integration()))
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        results.append(("Build Pipeline Integration", False))
    
    try:
        # Test 4: rel_path correction
        results.append(("rel_path Correction", test_rel_path_correction()))
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        results.append(("rel_path Correction", False))
    
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
    
    return 0 if passed_count == total_count else 1


if __name__ == "__main__":
    sys.exit(main())

