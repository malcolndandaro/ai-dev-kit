"""
Integration tests for cluster functions.

Tests:
- get_best_cluster (with running_only, name_filter options)
"""

import pytest
from databricks_tools_core.compute import get_best_cluster

@pytest.mark.integration
class TestGetBestCluster:
    """Tests for get_best_cluster function."""

    def test_get_best_cluster_returns_dict_or_none(self):
        """If cluster found, return a dictionary with cluster basics (keys = cluster_id, cluster_name, state, data_security_mode). If no cluster matches best cluster logic, return None."""
        result = get_best_cluster()

        # May be None if no clusters available
        assert result is not None
        assert isinstance(result, dict)
        assert "cluster_id" in result
        assert "cluster_name" in result
        assert "state" in result
        assert "data_security_mode" in result
        assert isinstance(result["cluster_id"], str)
        assert len(result["cluster_id"]) > 0
        assert result["cluster_id"] == '0304-162117-qgsi1x04'

    def test_get_best_cluster_with_name_filter(self):
        """Should find cluster matching name_filter (environment specific)."""
        test_name = "Shared AI Dev Kit"
        result = get_best_cluster(name_filter=test_name)
        
        assert result is not None, "No cluster found, be sure matching cluster is running in the workspace."
        assert isinstance(result, dict)
        assert result["cluster_name"] == test_name, f"Expected {test_name} but got {result['cluster_name']}. Be sure matching cluster is running in the workspace."

    def test_get_best_cluster_consistency(self):
        """Multiple calls should return the same result."""
        result_1 = get_best_cluster()
        result_2 = get_best_cluster()

        # Should return same cluster ID (assuming cluster state doesn't change)
        assert result_1 is not None
        assert result_2 is not None
        assert result_1["cluster_id"] == result_2["cluster_id"], (
            "get_best_cluster should return consistent results"
        )


if __name__ == "__main__":
    # Run tests directly for quick debugging

    test_best = TestGetBestCluster()

    tests = [
        # ("test_get_best_cluster_returns_dict_or_none", test_best.test_get_best_cluster_returns_dict_or_none),
        ("test_get_best_cluster_with_name_filter", test_best.test_get_best_cluster_with_name_filter),
        # ("test_get_best_cluster_consistency", test_best.test_get_best_cluster_consistency),
    ]

    for name, test_func in tests:
        print(f"\n{name}...")
        try:
            test_func()
            print("  ✓ PASSED")
        except pytest.skip.Exception as e:
            print(f"  ⊘ SKIPPED: {e}")
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
