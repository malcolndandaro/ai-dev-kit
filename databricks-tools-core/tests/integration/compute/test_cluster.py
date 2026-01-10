"""
Integration tests for cluster functions.

Tests:
- list_clusters (with running_only, shared_only filters)
- get_best_cluster (with running_only, name_filter options)
"""

import pytest
from databricks_tools_core.compute import list_clusters, get_best_cluster


@pytest.mark.integration
class TestListClusters:
    """Tests for list_clusters function."""

    def test_list_clusters_returns_list(self):
        """Should return a list of clusters."""
        clusters = list_clusters(running_only=True, limit=5)

        assert isinstance(clusters, list)
        print(f"Found {len(clusters)} running clusters")

    def test_list_clusters_structure(self):
        """Each cluster should have expected fields."""
        clusters = list_clusters(running_only=True, limit=5)

        for c in clusters:
            assert "id" in c, "Cluster should have 'id'"
            assert "name" in c, "Cluster should have 'name'"
            assert "state" in c, "Cluster should have 'state'"
            assert "single_user_name" in c, "Cluster should have 'single_user_name'"
            assert "data_security_mode" in c, "Cluster should have 'data_security_mode'"
            assert c["id"] is not None
            assert c["name"] is not None

    def test_list_clusters_running_only(self):
        """With running_only=True, should only return RUNNING clusters."""
        clusters = list_clusters(running_only=True, limit=10)

        for c in clusters:
            assert c["state"] == "RUNNING", (
                f"Cluster {c['name']} should be RUNNING, got {c['state']}"
            )

    def test_list_clusters_running_first(self):
        """Running clusters should be listed first."""
        clusters = list_clusters(running_only=False, limit=10)

        if len(clusters) == 0:
            pytest.skip("No clusters available")

        # Find first non-running cluster
        first_non_running_idx = None
        for i, c in enumerate(clusters):
            if c["state"] != "RUNNING":
                first_non_running_idx = i
                break

        # If we found a non-running cluster, all before it should be running
        if first_non_running_idx is not None:
            for i in range(first_non_running_idx):
                assert clusters[i]["state"] == "RUNNING", (
                    f"Cluster at index {i} should be RUNNING"
                )

    def test_list_clusters_with_limit(self):
        """Should respect the limit parameter."""
        clusters_5 = list_clusters(running_only=True, limit=5)
        clusters_2 = list_clusters(running_only=True, limit=2)

        assert len(clusters_2) <= 2
        assert len(clusters_5) <= 5

    def test_list_clusters_shared_only(self):
        """With shared_only=True, should only return shared access mode clusters."""
        clusters = list_clusters(running_only=True, shared_only=True, limit=10)

        for c in clusters:
            # Shared clusters should not have single_user_name set
            assert c["single_user_name"] is None, (
                f"Cluster {c['name']} has single_user_name={c['single_user_name']}, not shared"
            )

    def test_list_clusters_has_expected_fields(self):
        """Clusters should have all expected metadata fields."""
        clusters = list_clusters(running_only=True, limit=5)

        if len(clusters) == 0:
            pytest.skip("No running clusters available")

        expected_fields = [
            "id", "name", "state", "spark_version", "node_type_id",
            "single_user_name", "data_security_mode"
        ]
        for c in clusters:
            for field in expected_fields:
                assert field in c, f"Cluster should have '{field}'"


@pytest.mark.integration
class TestGetBestCluster:
    """Tests for get_best_cluster function."""

    def test_get_best_cluster_returns_string_or_none(self):
        """Should return a cluster ID string or None."""
        cluster_id = get_best_cluster()

        # May be None if no clusters available
        if cluster_id is not None:
            assert isinstance(cluster_id, str)
            assert len(cluster_id) > 0

    def test_get_best_cluster_returns_valid_id(self):
        """Returned ID should be in the cluster list."""
        cluster_id = get_best_cluster()

        if cluster_id is not None:
            clusters = list_clusters(running_only=False, limit=50)
            cluster_ids = [c["id"] for c in clusters]
            assert cluster_id in cluster_ids, (
                f"Cluster ID {cluster_id} not found in list"
            )

    def test_get_best_cluster_prefers_running(self):
        """Should prefer running clusters by default."""
        cluster_id = get_best_cluster(running_only=True)

        if cluster_id is not None:
            clusters = list_clusters(running_only=False, limit=50)
            selected = next((c for c in clusters if c["id"] == cluster_id), None)

            assert selected is not None, "Selected cluster should be in list"
            assert selected["state"] == "RUNNING", (
                "With running_only=True, should select a running cluster"
            )

    def test_get_best_cluster_with_name_filter(self):
        """Should find cluster matching name_filter."""
        # First, get a running cluster name to search for
        clusters = list_clusters(running_only=True, limit=5)

        if len(clusters) == 0:
            pytest.skip("No running clusters available")

        # Use part of the first cluster's name as filter
        test_name = clusters[0]["name"]
        cluster_id = get_best_cluster(name_filter=test_name)

        if cluster_id is not None:
            # Verify it found the right cluster
            selected = next((c for c in clusters if c["id"] == cluster_id), None)
            assert selected is not None
            print(f"Found cluster: {selected['name']} with filter '{test_name}'")

    def test_get_best_cluster_consistency(self):
        """Multiple calls should return the same result."""
        cluster_id_1 = get_best_cluster()
        cluster_id_2 = get_best_cluster()

        # Should return same ID (assuming cluster state doesn't change)
        assert cluster_id_1 == cluster_id_2, (
            "get_best_cluster should return consistent results"
        )


if __name__ == "__main__":
    # Run tests directly for quick debugging
    print("\n" + "=" * 50)
    print("Running cluster integration tests")
    print("=" * 50)

    test_list = TestListClusters()
    test_best = TestGetBestCluster()

    tests = [
        ("test_list_clusters_returns_list", test_list.test_list_clusters_returns_list),
        ("test_list_clusters_structure", test_list.test_list_clusters_structure),
        ("test_list_clusters_running_only", test_list.test_list_clusters_running_only),
        ("test_list_clusters_with_limit", test_list.test_list_clusters_with_limit),
        ("test_list_clusters_shared_only", test_list.test_list_clusters_shared_only),
        ("test_get_best_cluster_returns_string_or_none", test_best.test_get_best_cluster_returns_string_or_none),
        ("test_get_best_cluster_returns_valid_id", test_best.test_get_best_cluster_returns_valid_id),
        ("test_get_best_cluster_prefers_running", test_best.test_get_best_cluster_prefers_running),
        ("test_get_best_cluster_with_name_filter", test_best.test_get_best_cluster_with_name_filter),
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
