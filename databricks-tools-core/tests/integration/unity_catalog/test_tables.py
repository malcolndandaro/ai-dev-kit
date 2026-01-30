"""
Integration tests for Unity Catalog - Table operations.

Tests:
- list_tables
- get_table
- create_table (SDK)
- delete_table
- create_table_sql (SQL with liquid clustering, properties)
- create_table_as_select (CTAS)
- clone_table
- alter_table
"""

import logging
import pytest

from databricks.sdk.service.catalog import ColumnInfo, ColumnTypeName

from databricks_tools_core.unity_catalog import (
    list_tables,
    get_table,
    create_table,
    delete_table,
    create_table_sql,
    create_table_as_select,
    clone_table,
    alter_table,
)

logger = logging.getLogger(__name__)

UC_TEST_PREFIX = "uc_test"


@pytest.mark.integration
class TestListTables:
    """Tests for listing tables."""

    def test_list_tables(self, test_catalog: str, uc_test_schema: str):
        """Should list tables in schema."""
        tables = list_tables(
            catalog_name=test_catalog,
            schema_name=uc_test_schema,
        )

        logger.info(f"Found {len(tables)} tables in {test_catalog}.{uc_test_schema}")
        assert isinstance(tables, list)

    def test_list_tables_contains_test_table(
        self, test_catalog: str, uc_test_schema: str, uc_test_table: str
    ):
        """Should include the test table."""
        tables = list_tables(
            catalog_name=test_catalog,
            schema_name=uc_test_schema,
        )
        table_names = [t.name for t in tables]

        assert f"{UC_TEST_PREFIX}_employees" in table_names


@pytest.mark.integration
class TestGetTable:
    """Tests for getting table details."""

    def test_get_table(self, uc_test_table: str):
        """Should get table details by full name."""
        table = get_table(uc_test_table)

        logger.info(f"Got table: {table.full_name} (type: {table.table_type})")
        assert table.full_name == uc_test_table
        assert table.columns is not None
        assert len(table.columns) == 7  # employee_id, name, email, department, salary, hire_date, is_active

    def test_get_table_not_found(self, test_catalog: str, uc_test_schema: str):
        """Should raise error for non-existent table."""
        with pytest.raises(Exception):
            get_table(f"{test_catalog}.{uc_test_schema}.nonexistent_table_xyz")


@pytest.mark.integration
class TestCreateTableSDK:
    """Tests for SDK-based table creation."""

    def test_create_managed_table(
        self, test_catalog: str, uc_test_schema: str, unique_name: str, warehouse_id: str
    ):
        """Should create a managed table via SDK (uses SQL DDL internally for MANAGED)."""
        table_name = f"{UC_TEST_PREFIX}_sdk_{unique_name}"
        full_name = f"{test_catalog}.{uc_test_schema}.{table_name}"

        try:
            logger.info(f"Creating managed table: {full_name}")
            table = create_table(
                catalog_name=test_catalog,
                schema_name=uc_test_schema,
                table_name=table_name,
                columns=[
                    ColumnInfo(name="id", type_name=ColumnTypeName.LONG, type_text="bigint"),
                    ColumnInfo(name="value", type_name=ColumnTypeName.STRING, type_text="string"),
                ],
                warehouse_id=warehouse_id,
            )

            assert table.name == table_name
            logger.info(f"Table created: {table.full_name}")

            # Verify via get
            fetched = get_table(full_name)
            assert fetched.full_name == full_name
            assert len(fetched.columns) >= 2

        finally:
            try:
                delete_table(full_name)
            except Exception as e:
                logger.warning(f"Failed to cleanup table: {e}")

    def test_delete_table(
        self, test_catalog: str, uc_test_schema: str, unique_name: str, warehouse_id: str
    ):
        """Should delete a table."""
        table_name = f"{UC_TEST_PREFIX}_del_{unique_name}"
        full_name = f"{test_catalog}.{uc_test_schema}.{table_name}"

        # Use SQL-based creation (simpler, avoids SDK ColumnInfo complexity)
        create_table_sql(
            catalog_name=test_catalog,
            schema_name=uc_test_schema,
            table_name=table_name,
            columns=[{"name": "id", "type": "BIGINT"}],
            warehouse_id=warehouse_id,
        )

        logger.info(f"Deleting table: {full_name}")
        delete_table(full_name)

        with pytest.raises(Exception):
            get_table(full_name)
        logger.info(f"Table deleted successfully: {full_name}")


@pytest.mark.integration
class TestCreateTableSQL:
    """Tests for SQL-based table creation (advanced features)."""

    def test_create_table_sql_basic(
        self, test_catalog: str, uc_test_schema: str, unique_name: str, warehouse_id: str
    ):
        """Should create a table via SQL."""
        table_name = f"{UC_TEST_PREFIX}_sql_{unique_name}"
        full_name = f"{test_catalog}.{uc_test_schema}.{table_name}"

        try:
            result = create_table_sql(
                catalog_name=test_catalog,
                schema_name=uc_test_schema,
                table_name=table_name,
                columns=[
                    {"name": "id", "type": "BIGINT"},
                    {"name": "name", "type": "STRING", "comment": "Full name"},
                    {"name": "region", "type": "STRING"},
                ],
                comment="SQL-created test table",
                warehouse_id=warehouse_id,
            )

            assert result["status"] == "created"
            assert result["full_table_name"] == full_name
            logger.info(f"SQL table created: {result['full_table_name']}")

            # Verify
            table = get_table(full_name)
            assert table.full_name == full_name

        finally:
            try:
                delete_table(full_name)
            except Exception:
                pass

    def test_create_table_sql_with_cluster_by(
        self, test_catalog: str, uc_test_schema: str, unique_name: str, warehouse_id: str
    ):
        """Should create a table with liquid clustering."""
        table_name = f"{UC_TEST_PREFIX}_lc_{unique_name}"
        full_name = f"{test_catalog}.{uc_test_schema}.{table_name}"

        try:
            result = create_table_sql(
                catalog_name=test_catalog,
                schema_name=uc_test_schema,
                table_name=table_name,
                columns=[
                    {"name": "id", "type": "BIGINT"},
                    {"name": "region", "type": "STRING"},
                    {"name": "event_date", "type": "DATE"},
                ],
                cluster_by=["region", "event_date"],
                warehouse_id=warehouse_id,
            )

            assert result["status"] == "created"
            assert "CLUSTER BY" in result["sql"]
            logger.info(f"Liquid clustering table created: {result['full_table_name']}")

        finally:
            try:
                delete_table(full_name)
            except Exception:
                pass

    def test_create_table_sql_with_properties(
        self, test_catalog: str, uc_test_schema: str, unique_name: str, warehouse_id: str
    ):
        """Should create a table with table properties."""
        table_name = f"{UC_TEST_PREFIX}_props_{unique_name}"
        full_name = f"{test_catalog}.{uc_test_schema}.{table_name}"

        try:
            result = create_table_sql(
                catalog_name=test_catalog,
                schema_name=uc_test_schema,
                table_name=table_name,
                columns=[
                    {"name": "id", "type": "BIGINT"},
                ],
                table_properties={"quality": "silver", "team": "data-eng"},
                warehouse_id=warehouse_id,
            )

            assert result["status"] == "created"
            assert "TBLPROPERTIES" in result["sql"]
            logger.info(f"Table with properties created: {result['full_table_name']}")

        finally:
            try:
                delete_table(full_name)
            except Exception:
                pass


@pytest.mark.integration
class TestCreateTableAsSelect:
    """Tests for CTAS (Create Table As Select)."""

    def test_ctas(
        self, test_catalog: str, uc_test_schema: str, uc_test_table: str,
        unique_name: str, warehouse_id: str
    ):
        """Should create table from SELECT query."""
        table_name = f"{UC_TEST_PREFIX}_ctas_{unique_name}"
        full_name = f"{test_catalog}.{uc_test_schema}.{table_name}"

        try:
            result = create_table_as_select(
                catalog_name=test_catalog,
                schema_name=uc_test_schema,
                table_name=table_name,
                select_query=f"SELECT employee_id, name, department FROM {uc_test_table} WHERE is_active = true",
                comment="CTAS test table",
                warehouse_id=warehouse_id,
            )

            assert result["status"] == "created"
            logger.info(f"CTAS table created: {result['full_table_name']}")

            # Verify data
            from databricks_tools_core.sql import execute_sql
            rows = execute_sql(
                sql_query=f"SELECT COUNT(*) as cnt FROM {full_name}",
                warehouse_id=warehouse_id,
            )
            count = int(rows[0]["cnt"])
            assert count == 4, f"Expected 4 active employees, got {count}"

        finally:
            try:
                delete_table(full_name)
            except Exception:
                pass


@pytest.mark.integration
class TestCloneTable:
    """Tests for table cloning."""

    def test_deep_clone(
        self, test_catalog: str, uc_test_schema: str,
        uc_test_table_for_clone: str, unique_name: str, warehouse_id: str
    ):
        """Should deep clone a table."""
        target_name = f"{UC_TEST_PREFIX}_clone_{unique_name}"
        target_full = f"{test_catalog}.{uc_test_schema}.{target_name}"

        try:
            result = clone_table(
                source_table=uc_test_table_for_clone,
                target_catalog=test_catalog,
                target_schema=uc_test_schema,
                target_table=target_name,
                clone_type="DEEP",
                warehouse_id=warehouse_id,
            )

            assert result["status"] == "cloned"
            assert result["clone_type"] == "DEEP"
            logger.info(f"Deep clone created: {result['target_table']}")

            # Verify data
            from databricks_tools_core.sql import execute_sql
            rows = execute_sql(
                sql_query=f"SELECT COUNT(*) as cnt FROM {target_full}",
                warehouse_id=warehouse_id,
            )
            assert int(rows[0]["cnt"]) == 3

        finally:
            try:
                delete_table(target_full)
            except Exception:
                pass


@pytest.mark.integration
class TestAlterTable:
    """Tests for altering tables."""

    def test_alter_table_add_columns(
        self, test_catalog: str, uc_test_schema: str,
        unique_name: str, warehouse_id: str
    ):
        """Should add columns to a table."""
        table_name = f"{UC_TEST_PREFIX}_alter_{unique_name}"
        full_name = f"{test_catalog}.{uc_test_schema}.{table_name}"

        try:
            # Create base table
            create_table_sql(
                catalog_name=test_catalog,
                schema_name=uc_test_schema,
                table_name=table_name,
                columns=[{"name": "id", "type": "BIGINT"}],
                warehouse_id=warehouse_id,
            )

            # Add columns
            result = alter_table(
                full_table_name=full_name,
                add_columns=[
                    {"name": "name", "type": "STRING", "comment": "Person name"},
                    {"name": "age", "type": "INT"},
                ],
                warehouse_id=warehouse_id,
            )

            assert result["status"] == "altered"
            assert len(result["executed_sql"]) == 1
            logger.info(f"Columns added: {result['executed_sql']}")

            # Verify columns
            table = get_table(full_name)
            col_names = [c.name for c in table.columns]
            assert "name" in col_names
            assert "age" in col_names

        finally:
            try:
                delete_table(full_name)
            except Exception:
                pass

    def test_alter_table_set_properties(
        self, test_catalog: str, uc_test_schema: str,
        unique_name: str, warehouse_id: str
    ):
        """Should set table properties."""
        table_name = f"{UC_TEST_PREFIX}_altprop_{unique_name}"
        full_name = f"{test_catalog}.{uc_test_schema}.{table_name}"

        try:
            create_table_sql(
                catalog_name=test_catalog,
                schema_name=uc_test_schema,
                table_name=table_name,
                columns=[{"name": "id", "type": "BIGINT"}],
                warehouse_id=warehouse_id,
            )

            result = alter_table(
                full_table_name=full_name,
                set_properties={"quality": "gold", "team": "analytics"},
                warehouse_id=warehouse_id,
            )

            assert result["status"] == "altered"
            assert "TBLPROPERTIES" in result["executed_sql"][0]
            logger.info(f"Properties set: {result['executed_sql']}")

        finally:
            try:
                delete_table(full_name)
            except Exception:
                pass

    def test_alter_table_no_fields_raises(self):
        """Should raise ValueError when no alterations specified."""
        with pytest.raises(ValueError) as exc_info:
            alter_table(full_table_name="cat.sch.tbl")

        assert "at least one" in str(exc_info.value).lower()
