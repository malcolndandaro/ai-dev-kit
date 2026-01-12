"""
Cluster Operations

Functions for listing and selecting Databricks clusters.

Optimized for large workspaces by:
- Filtering RUNNING clusters first (faster iteration)
- Supporting pagination with early termination
- Preferring shared access mode clusters over single-user
"""

import logging
from typing import Any, Dict, List, Optional

from databricks.sdk.service.compute import ListClustersFilterBy, ClusterSource, State

from ..auth import get_workspace_client

logger = logging.getLogger(__name__)


def _is_shared_access_mode(cluster) -> bool:
    """
    Check if cluster has shared/standard access mode (not single-user).

    A cluster is considered shared if:
    - single_user_name is not set (not assigned to specific user)
    - data_security_mode is not SINGLE_USER

    Args:
        cluster: Cluster object from SDK

    Returns:
        True if cluster is shared access mode
    """
    # If single_user_name is set, it's a single-user cluster
    if getattr(cluster, 'single_user_name', None):
        return False

    # Check data_security_mode if available
    data_security_mode = getattr(cluster, 'data_security_mode', None)
    if data_security_mode:
        mode_str = str(data_security_mode).upper()
        if 'SINGLE_USER' in mode_str:
            return False

    return True


def _cluster_to_dict(cluster) -> Dict[str, Any]:
    """Convert cluster object to dictionary."""
    return {
        "id": cluster.cluster_id,
        "name": cluster.cluster_name,
        "state": cluster.state.value if cluster.state else None,
        "spark_version": cluster.spark_version,
        "node_type_id": cluster.node_type_id,
        "autotermination_minutes": cluster.autotermination_minutes,
        "creator_user_name": cluster.creator_user_name,
        "single_user_name": getattr(cluster, 'single_user_name', None),
        "data_security_mode": str(getattr(cluster, 'data_security_mode', None)) if getattr(cluster, 'data_security_mode', None) else None,
    }


def get_best_cluster(
    running_only: bool = True,
    name_filter: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Select the best available cluster based on priority rules.

    Optimized to find running shared clusters first, avoiding full list iteration
    in large workspaces when possible.

    Priority:
    1. Running shared-access cluster matching name_filter (default to "shared")
    2. Any running shared-access cluster with name_filter in name
    3. Any running shared-access cluster
    4. (If running_only=False) Stopped shared-access cluster
    
    Note: single-user clusters are not considered.

    Args:
        running_only: If True (default), only consider RUNNING clusters
        name_filter: Custom name to search for in cluster names (case-insensitive).
                    If None, defaults to searching for 'shared'.

    Returns:
        Dictionary with cluster information, or None if no clusters available.
        Dictionary contains:
        - cluster_id: Cluster ID string
        - cluster_name: Cluster name
        - state: Cluster state (RUNNING, TERMINATED, etc.)
        - data_security_mode: Data security mode

    Raises:
        Exception: If API request fails
    """
    client = get_workspace_client()

    # Use custom filter or default to 'shared'
    search_term = name_filter.lower() if name_filter else "shared"

    # Categories for running clusters
    exact_match_running = []      # Exact name match + shared access
    name_match_running = []       # search_term in name + shared access mode
    shared_access_running = []    # Shared access mode (no name match)
    single_user_running = []      # Single-user (fallback)

    # Categories for stopped clusters (only used if running_only=False)
    shared_stopped = []
    other_stopped = []

    # client.clusters.list()
    filtered_clusters = client.clusters.list(filter_by=ListClustersFilterBy(cluster_states=[State.RUNNING, State.PENDING], cluster_sources=[ClusterSource.UI, ClusterSource.API]), page_size=50)
    try:
        print(filtered_clusters)
        for cluster in filtered_clusters:
            c = _cluster_to_dict(cluster)
            print(c.get("name"), c.get("state"), c.get("id"))
            is_running = cluster.state == State.RUNNING
            is_shared_access = _is_shared_access_mode(cluster)
            name_lower = cluster.cluster_name.lower() if cluster.cluster_name else ""
            has_name_match = search_term in name_lower

            logger.debug(f"name_lower:\n running: 1. (running): {is_running}\n 2. (shared): {is_shared_access}\n 3. (name): {name_lower}\n 4. (name match): {has_name_match}")

            if is_running:
                # Check for exact name match (custom filter or default names)
                if name_filter and is_shared_access and name_lower == search_term:
                    exact_match_running.append(cluster)
                    # Found exact match, stop early
                    break
                elif is_shared_access and has_name_match:
                    name_match_running.append(cluster)
                elif is_shared_access:
                    shared_access_running.append(cluster)
                # else:
                #     single_user_running.append(cluster)

                # Early termination: found a good running shared cluster
                if exact_match_running or (name_match_running and len(name_match_running) >= 3):
                    break

            elif not running_only:
                if is_shared_access:
                    shared_stopped.append(cluster)
                else:
                    other_stopped.append(cluster)

    except Exception as e:
        raise Exception(
            f"Failed to list clusters: {str(e)}. "
            f"Check that you have permission to view clusters."
        )

    # Select based on priority
    selected = None

    if exact_match_running:
        selected = exact_match_running[0]
    elif name_match_running:
        selected = name_match_running[0]
    elif shared_access_running:
        selected = shared_access_running[0]
    elif single_user_running:
        selected = single_user_running[0]
        logger.warning(f"No shared-access running clusters found, using single-user: {selected.cluster_name}")
    elif not running_only:
        if shared_stopped:
            selected = shared_stopped[0]
        elif other_stopped:
            selected = other_stopped[0]

    if selected is None:
        logger.warning("No suitable clusters found in workspace")
        return None

    logger.debug(
        f"Selected cluster: {selected.cluster_name} "
        f"(state: {selected.state}, shared_access: {_is_shared_access_mode(selected)})"
    )
    return {"cluster_id":selected.cluster_id, "cluster_name":selected.cluster_name, "state":selected.state, "data_security_mode":selected.data_security_mode}
