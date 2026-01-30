"""
Integration tests for Unity Catalog - System Table queries.

Tests:
- query_system_table with various system table types
- Parameter validation
- Date partition filtering

Note: System tables may not be enabled in all workspaces. Tests that
query system tables gracefully handle the case when tables don't exist.
"""

import logging
import pytest

from databricks_tools_core.unity_catalog import query_system_table

logger = logging.getLogger(__name__)


@pytest.mark.integration
class TestQuerySystemTable:
    """Tests for querying system tables."""

    def test_query_audit_logs(self, warehouse_id: str):
        """Should query audit logs (may be empty if not enabled)."""
        try:
            result = query_system_table(
                system_table="audit",
                days_back=1,
                limit=5,
                warehouse_id=warehouse_id,
            )

            assert result["system_table"] == "audit"
            assert result["full_table_name"] == "system.access.audit"
            assert isinstance(result["data"], list)
            assert result["row_count"] == len(result["data"])
            assert "event_date" in result["sql"]
            logger.info(f"Audit logs: {result['row_count']} rows")

        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e).upper() or "not found" in str(e).lower():
                pytest.skip("System table system.access.audit not available")
            raise

    def test_query_query_history(self, warehouse_id: str):
        """Should query recent SQL history."""
        try:
            result = query_system_table(
                system_table="query_history",
                days_back=1,
                limit=10,
                warehouse_id=warehouse_id,
            )

            assert result["system_table"] == "query_history"
            assert result["full_table_name"] == "system.query.history"
            assert isinstance(result["data"], list)
            logger.info(f"Query history: {result['row_count']} rows")

        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e).upper() or "not found" in str(e).lower():
                pytest.skip("System table system.query.history not available")
            raise

    def test_query_billing_usage(self, warehouse_id: str):
        """Should query billing usage."""
        try:
            result = query_system_table(
                system_table="billing_usage",
                days_back=7,
                limit=10,
                warehouse_id=warehouse_id,
            )

            assert result["system_table"] == "billing_usage"
            assert isinstance(result["data"], list)
            logger.info(f"Billing usage: {result['row_count']} rows")

        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e).upper() or "not found" in str(e).lower():
                pytest.skip("System table system.billing.usage not available")
            raise

    def test_query_jobs(self, warehouse_id: str):
        """Should query job definitions."""
        try:
            result = query_system_table(
                system_table="jobs",
                days_back=7,
                limit=10,
                warehouse_id=warehouse_id,
            )

            assert result["system_table"] == "jobs"
            assert result["full_table_name"] == "system.lakeflow.jobs"
            assert isinstance(result["data"], list)
            logger.info(f"Jobs: {result['row_count']} rows")

        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e).upper() or "not found" in str(e).lower():
                pytest.skip("System table system.lakeflow.jobs not available")
            raise

    def test_query_clusters(self, warehouse_id: str):
        """Should query cluster configurations."""
        try:
            result = query_system_table(
                system_table="clusters",
                days_back=7,
                limit=10,
                warehouse_id=warehouse_id,
            )

            assert result["system_table"] == "clusters"
            assert isinstance(result["data"], list)
            logger.info(f"Clusters: {result['row_count']} rows")

        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e).upper() or "not found" in str(e).lower():
                pytest.skip("System table system.compute.clusters not available")
            raise

    def test_query_table_lineage(self, warehouse_id: str):
        """Should query table lineage."""
        try:
            result = query_system_table(
                system_table="table_lineage",
                days_back=7,
                limit=10,
                warehouse_id=warehouse_id,
            )

            assert result["system_table"] == "table_lineage"
            assert isinstance(result["data"], list)
            logger.info(f"Table lineage: {result['row_count']} rows")

        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e).upper() or "not found" in str(e).lower():
                pytest.skip("System table system.access.table_lineage not available")
            raise


@pytest.mark.integration
class TestQuerySystemTableFilters:
    """Tests for system table query filters."""

    def test_custom_columns(self, warehouse_id: str):
        """Should support custom SELECT columns."""
        try:
            result = query_system_table(
                system_table="query_history",
                days_back=1,
                limit=5,
                custom_columns="statement_type, executed_by, start_time",
                warehouse_id=warehouse_id,
            )

            assert "statement_type" in result["sql"]
            assert "executed_by" in result["sql"]
            logger.info(f"Custom columns query: {result['sql'][:100]}...")

        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e).upper() or "not found" in str(e).lower():
                pytest.skip("System table not available")
            raise

    def test_custom_where(self, warehouse_id: str):
        """Should support custom WHERE clause."""
        try:
            result = query_system_table(
                system_table="audit",
                days_back=1,
                limit=5,
                custom_where="action_name IS NOT NULL",
                warehouse_id=warehouse_id,
            )

            assert "action_name IS NOT NULL" in result["sql"]
            logger.info(f"Custom WHERE query: {result['sql'][:100]}...")

        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e).upper() or "not found" in str(e).lower():
                pytest.skip("System table not available")
            raise

    def test_user_email_filter(self, warehouse_id: str):
        """Should filter by user email."""
        try:
            result = query_system_table(
                system_table="query_history",
                days_back=1,
                limit=5,
                user_email="test@example.com",
                warehouse_id=warehouse_id,
            )

            assert "executed_by" in result["sql"]
            assert "test@example.com" in result["sql"]
            logger.info(f"User email filter: {result['sql'][:100]}...")

        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e).upper() or "not found" in str(e).lower():
                pytest.skip("System table not available")
            raise


@pytest.mark.integration
class TestQuerySystemTableValidation:
    """Tests for query validation."""

    def test_invalid_system_table_raises(self):
        """Should raise ValueError for invalid system table name."""
        with pytest.raises(ValueError) as exc_info:
            query_system_table(system_table="nonexistent_table")

        assert "invalid system_table" in str(exc_info.value).lower()
        assert "valid values" in str(exc_info.value).lower()

    def test_date_partition_filter_included(self, warehouse_id: str):
        """Should include date partition filter for performance."""
        try:
            result = query_system_table(
                system_table="audit",
                days_back=3,
                limit=1,
                warehouse_id=warehouse_id,
            )

            # Audit table uses event_date column
            assert "event_date >= CURRENT_DATE() - INTERVAL 3 DAY" in result["sql"]
            logger.info("Date partition filter correctly included")

        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e).upper() or "not found" in str(e).lower():
                pytest.skip("System table not available")
            raise

    def test_billing_prices_no_date_filter(self, warehouse_id: str):
        """Should not include date filter for tables without date columns."""
        try:
            result = query_system_table(
                system_table="billing_prices",
                days_back=7,
                limit=5,
                warehouse_id=warehouse_id,
            )

            # billing_prices has no date column
            assert "INTERVAL" not in result["sql"]
            logger.info("No date filter for billing_prices (correct)")

        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e).upper() or "not found" in str(e).lower():
                pytest.skip("System table not available")
            raise
