"""
Tests for auth configuration functions.

Tests:
- set_databricks_config
- get_default_cluster_id
- get_default_warehouse_id
- get_workspace_client with profile
"""

import pytest
from databricks_tools_core.auth import (
    set_databricks_config,
    get_default_cluster_id,
    get_default_warehouse_id,
    get_workspace_client,
)


class TestDatabricksConfig:
    """Tests for config context variables."""

    def test_set_and_get_cluster_id(self):
        """Should store and retrieve cluster ID."""
        test_id = "test-cluster-123"

        # Set the config
        set_databricks_config(cluster_id=test_id)

        # Retrieve it
        result = get_default_cluster_id()
        assert result == test_id

        # Clean up
        set_databricks_config(cluster_id=None)
        assert get_default_cluster_id() is None

    def test_set_and_get_warehouse_id(self):
        """Should store and retrieve warehouse ID."""
        test_id = "test-warehouse-456"

        # Set the config
        set_databricks_config(warehouse_id=test_id)

        # Retrieve it
        result = get_default_warehouse_id()
        assert result == test_id

        # Clean up
        set_databricks_config(warehouse_id=None)
        assert get_default_warehouse_id() is None

    def test_set_multiple_config_values(self):
        """Should set multiple config values at once."""
        set_databricks_config(
            profile="test-profile",
            cluster_id="cluster-abc",
            warehouse_id="warehouse-xyz"
        )

        assert get_default_cluster_id() == "cluster-abc"
        assert get_default_warehouse_id() == "warehouse-xyz"

        # Clean up
        set_databricks_config(profile=None, cluster_id=None, warehouse_id=None)

    def test_config_defaults_are_none(self):
        """Default config values should be None."""
        # Clear any existing config
        set_databricks_config(profile=None, cluster_id=None, warehouse_id=None)

        assert get_default_cluster_id() is None
        assert get_default_warehouse_id() is None


@pytest.mark.integration
class TestWorkspaceClientWithProfile:
    """Integration tests for get_workspace_client with profile."""

    def test_get_workspace_client_default(self):
        """Should get client with default auth."""
        # Clear profile config
        set_databricks_config(profile=None)

        client = get_workspace_client()
        assert client is not None

        # Verify it works by making a simple API call
        # This uses whatever auth is configured (env vars or config file)
        try:
            user = client.current_user.me()
            assert user is not None
            print(f"Connected as: {user.user_name}")
        except Exception as e:
            pytest.skip(f"No default auth configured: {e}")

    def test_get_workspace_client_with_profile(self):
        """Should get client using specified profile."""
        # This test requires a valid profile in ~/.databrickscfg
        # Skip if E2DEFAULT profile doesn't exist
        try:
            set_databricks_config(profile="E2DEFAULT")
            client = get_workspace_client()
            user = client.current_user.me()
            assert user is not None
            print(f"Connected with E2DEFAULT profile as: {user.user_name}")
        except Exception as e:
            pytest.skip(f"E2DEFAULT profile not available: {e}")
        finally:
            set_databricks_config(profile=None)


if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("Running auth config tests")
    print("=" * 50)

    test_config = TestDatabricksConfig()
    test_client = TestWorkspaceClientWithProfile()

    tests = [
        ("test_set_and_get_cluster_id", test_config.test_set_and_get_cluster_id),
        ("test_set_and_get_warehouse_id", test_config.test_set_and_get_warehouse_id),
        ("test_set_multiple_config_values", test_config.test_set_multiple_config_values),
        ("test_config_defaults_are_none", test_config.test_config_defaults_are_none),
        ("test_get_workspace_client_default", test_client.test_get_workspace_client_default),
        ("test_get_workspace_client_with_profile", test_client.test_get_workspace_client_with_profile),
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
