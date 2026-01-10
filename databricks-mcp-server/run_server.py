#!/usr/bin/env python
"""Run the Databricks MCP Server."""
import argparse
from databricks_mcp_server.server import mcp
from databricks_tools_core.auth import set_databricks_config

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Databricks MCP Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use a specific profile
  python -m databricks_mcp_server.server --profile my-profile

  # Set default cluster and warehouse
  python -m databricks_mcp_server.server --profile my-profile --cluster-id abc123 --warehouse-id xyz789
        """
    )
    parser.add_argument(
        "--profile",
        help="Databricks config profile name from ~/.databrickscfg",
    )
    parser.add_argument(
        "--cluster-id",
        help="Default cluster ID for compute operations",
    )
    parser.add_argument(
        "--warehouse-id",
        help="Default warehouse ID for SQL operations",
    )

    args = parser.parse_args()

    # Configure authentication and defaults
    set_databricks_config(
        profile=args.profile,
        cluster_id=args.cluster_id,
        warehouse_id=args.warehouse_id,
    )
    
    mcp.run(transport="stdio")
