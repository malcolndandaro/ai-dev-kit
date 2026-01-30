"""
Unity Catalog - Table Operations

Functions for managing tables in Unity Catalog.
Includes SDK-based CRUD and SQL-based DDL for advanced features
(liquid clustering, CTAS, cloning).
"""
import logging
import re
from typing import Any, Dict, List, Optional
from databricks.sdk.service.catalog import TableInfo, ColumnInfo, TableType, DataSourceFormat

from ..auth import get_workspace_client

logger = logging.getLogger(__name__)

_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.\-]*$")


def list_tables(catalog_name: str, schema_name: str) -> List[TableInfo]:
    """
    List all tables in a schema.

    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema

    Returns:
        List of TableInfo objects with table metadata

    Raises:
        DatabricksError: If API request fails
    """
    w = get_workspace_client()
    return list(w.tables.list(
        catalog_name=catalog_name,
        schema_name=schema_name
    ))


def get_table(full_table_name: str) -> TableInfo:
    """
    Get detailed information about a specific table.

    Args:
        full_table_name: Full table name (catalog.schema.table format)

    Returns:
        TableInfo object with table metadata including:
        - name, full_name, catalog_name, schema_name
        - table_type, owner, comment
        - created_at, updated_at
        - storage_location, columns

    Raises:
        DatabricksError: If API request fails
    """
    w = get_workspace_client()
    return w.tables.get(full_name=full_table_name)


def create_table(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    columns: List[ColumnInfo],
    table_type: TableType = TableType.MANAGED,
    comment: Optional[str] = None,
    storage_location: Optional[str] = None,
    warehouse_id: Optional[str] = None,
) -> TableInfo:
    """
    Create a new table in Unity Catalog.

    For MANAGED tables, uses SQL DDL (the REST API only supports EXTERNAL tables).
    For EXTERNAL tables, uses the SDK REST API.

    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        table_name: Name of the table to create
        columns: List of ColumnInfo objects defining table columns
                 Example: [ColumnInfo(name="id", type_name=ColumnTypeName.INT),
                          ColumnInfo(name="value", type_name=ColumnTypeName.STRING)]
        table_type: TableType.MANAGED or TableType.EXTERNAL (default: TableType.MANAGED)
        comment: Optional description of the table
        storage_location: Storage location for EXTERNAL tables
        warehouse_id: Optional SQL warehouse ID (used for MANAGED table creation via SQL)

    Returns:
        TableInfo object with created table metadata

    Raises:
        DatabricksError: If API request fails
    """
    w = get_workspace_client()
    full_name = f"{catalog_name}.{schema_name}.{table_name}"

    if table_type == TableType.EXTERNAL:
        # SDK REST API supports EXTERNAL Delta table creation
        if not storage_location:
            raise ValueError("storage_location is required for EXTERNAL tables")

        table = w.tables.create(
            name=table_name,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_type=table_type,
            data_source_format=DataSourceFormat.DELTA,
            storage_location=storage_location,
            columns=columns,
        )

        if comment:
            try:
                w.tables.update(full_name=full_name, comment=comment)
            except Exception:
                pass
        return table
    else:
        # REST API does not support MANAGED table creation;
        # use SQL DDL instead and return the TableInfo via get.
        col_dicts = []
        for col in columns:
            col_dict: Dict[str, str] = {
                "name": col.name,
                "type": col.type_text or (col.type_name.value if col.type_name else "STRING"),
            }
            if col.comment:
                col_dict["comment"] = col.comment
            col_dicts.append(col_dict)

        create_table_sql(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            columns=col_dicts,
            table_type="MANAGED",
            comment=comment,
            warehouse_id=warehouse_id,
        )
        return w.tables.get(full_name=full_name)


def delete_table(full_table_name: str) -> None:
    """
    Delete a table from Unity Catalog.

    Args:
        full_table_name: Full table name (catalog.schema.table format)

    Raises:
        DatabricksError: If API request fails
    """
    w = get_workspace_client()
    w.tables.delete(full_name=full_table_name)


def _validate_identifier(name: str) -> str:
    """Validate and backtick-quote a SQL identifier."""
    if not _IDENTIFIER_PATTERN.match(name):
        raise ValueError(f"Invalid SQL identifier: '{name}'")
    return name


def _execute_uc_sql(sql_query: str, warehouse_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Execute SQL using the existing execute_sql infrastructure."""
    from ..sql.sql import execute_sql
    return execute_sql(sql_query=sql_query, warehouse_id=warehouse_id)


def create_table_sql(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    columns: List[Dict[str, str]],
    table_type: str = "MANAGED",
    comment: Optional[str] = None,
    storage_location: Optional[str] = None,
    cluster_by: Optional[List[str]] = None,
    table_properties: Optional[Dict[str, str]] = None,
    warehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a table using SQL, supporting advanced features like liquid clustering.

    Use this instead of create_table() when you need cluster_by or table_properties.

    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        table_name: Name of the table to create
        columns: List of column dicts [{"name": "id", "type": "BIGINT", "comment": "..."}]
        table_type: "MANAGED" or "EXTERNAL" (default: "MANAGED")
        comment: Optional table description
        storage_location: Required for EXTERNAL tables
        cluster_by: Columns for liquid clustering (e.g., ["region", "date"])
        table_properties: Key-value table properties
        warehouse_id: Optional SQL warehouse ID

    Returns:
        Dict with status and full table name

    Raises:
        ValueError: If invalid parameters
    """
    full_name = f"{_validate_identifier(catalog_name)}.{_validate_identifier(schema_name)}.{_validate_identifier(table_name)}"

    col_defs = []
    for col in columns:
        col_def = f"`{_validate_identifier(col['name'])}` {col['type']}"
        if col.get("comment"):
            col_def += f" COMMENT '{col['comment']}'"
        col_defs.append(col_def)

    sql_parts = [f"CREATE TABLE {full_name}"]
    sql_parts.append(f"({', '.join(col_defs)})")

    if table_type.upper() == "EXTERNAL":
        if not storage_location:
            raise ValueError("storage_location required for EXTERNAL tables")
        sql_parts.append(f"LOCATION '{storage_location}'")

    if cluster_by:
        validated_cols = [_validate_identifier(c) for c in cluster_by]
        sql_parts.append(f"CLUSTER BY ({', '.join(validated_cols)})")

    if table_properties:
        props = ", ".join(f"'{k}' = '{v}'" for k, v in table_properties.items())
        sql_parts.append(f"TBLPROPERTIES ({props})")

    if comment:
        sql_parts.append(f"COMMENT '{comment}'")

    sql = "\n".join(sql_parts)
    logger.info(f"Creating table with SQL: {sql}")
    _execute_uc_sql(sql, warehouse_id=warehouse_id)

    return {"status": "created", "full_table_name": full_name, "sql": sql}


def create_table_as_select(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    select_query: str,
    comment: Optional[str] = None,
    cluster_by: Optional[List[str]] = None,
    warehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a table from a SELECT query (CTAS).

    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        table_name: Name of the table to create
        select_query: SELECT SQL query defining the table contents
        comment: Optional table description
        cluster_by: Columns for liquid clustering
        warehouse_id: Optional SQL warehouse ID

    Returns:
        Dict with status and full table name
    """
    full_name = f"{_validate_identifier(catalog_name)}.{_validate_identifier(schema_name)}.{_validate_identifier(table_name)}"

    sql_parts = [f"CREATE TABLE {full_name}"]

    if cluster_by:
        validated_cols = [_validate_identifier(c) for c in cluster_by]
        sql_parts.append(f"CLUSTER BY ({', '.join(validated_cols)})")

    if comment:
        sql_parts.append(f"COMMENT '{comment}'")

    sql_parts.append(f"AS {select_query}")

    sql = "\n".join(sql_parts)
    logger.info(f"Creating table as select: {sql}")
    _execute_uc_sql(sql, warehouse_id=warehouse_id)

    return {"status": "created", "full_table_name": full_name, "sql": sql}


def clone_table(
    source_table: str,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    clone_type: str = "DEEP",
    version: Optional[int] = None,
    timestamp: Optional[str] = None,
    warehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Clone a table (deep or shallow).

    Args:
        source_table: Full source table name (catalog.schema.table)
        target_catalog: Target catalog name
        target_schema: Target schema name
        target_table: Target table name
        clone_type: "DEEP" or "SHALLOW" (default: "DEEP")
        version: Clone at specific version number
        timestamp: Clone at specific timestamp (e.g., "2024-01-01")
        warehouse_id: Optional SQL warehouse ID

    Returns:
        Dict with status and table names
    """
    _validate_identifier(source_table)
    target_full = f"{_validate_identifier(target_catalog)}.{_validate_identifier(target_schema)}.{_validate_identifier(target_table)}"

    sql = f"CREATE TABLE {target_full} {clone_type.upper()} CLONE {source_table}"
    if version is not None:
        sql += f" VERSION AS OF {version}"
    elif timestamp is not None:
        sql += f" TIMESTAMP AS OF '{timestamp}'"

    logger.info(f"Cloning table: {sql}")
    _execute_uc_sql(sql, warehouse_id=warehouse_id)

    return {
        "status": "cloned",
        "source_table": source_table,
        "target_table": target_full,
        "clone_type": clone_type.upper(),
        "sql": sql,
    }


def alter_table(
    full_table_name: str,
    new_name: Optional[str] = None,
    owner: Optional[str] = None,
    set_properties: Optional[Dict[str, str]] = None,
    add_columns: Optional[List[Dict[str, str]]] = None,
    warehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Alter a table's properties, name, owner, or columns.

    Args:
        full_table_name: Full table name (catalog.schema.table)
        new_name: New table name (not full path, just the name)
        owner: New owner
        set_properties: Properties to set
        add_columns: Columns to add [{"name": "col", "type": "STRING", "comment": "..."}]
        warehouse_id: Optional SQL warehouse ID

    Returns:
        Dict with status and executed SQL statements
    """
    _validate_identifier(full_table_name)
    executed = []

    if new_name:
        sql = f"ALTER TABLE {full_table_name} RENAME TO {_validate_identifier(new_name)}"
        _execute_uc_sql(sql, warehouse_id=warehouse_id)
        executed.append(sql)

    if owner:
        sql = f"ALTER TABLE {full_table_name} SET OWNER TO `{_validate_identifier(owner)}`"
        _execute_uc_sql(sql, warehouse_id=warehouse_id)
        executed.append(sql)

    if set_properties:
        props = ", ".join(f"'{k}' = '{v}'" for k, v in set_properties.items())
        sql = f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ({props})"
        _execute_uc_sql(sql, warehouse_id=warehouse_id)
        executed.append(sql)

    if add_columns:
        col_defs = []
        for col in add_columns:
            col_def = f"`{_validate_identifier(col['name'])}` {col['type']}"
            if col.get("comment"):
                col_def += f" COMMENT '{col['comment']}'"
            col_defs.append(col_def)
        sql = f"ALTER TABLE {full_table_name} ADD COLUMNS ({', '.join(col_defs)})"
        _execute_uc_sql(sql, warehouse_id=warehouse_id)
        executed.append(sql)

    if not executed:
        raise ValueError("At least one alteration must be specified")

    return {"status": "altered", "full_table_name": full_table_name, "executed_sql": executed}
