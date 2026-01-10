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

from databricks.sdk.service.compute import State

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


def list_clusters(
    limit: int = 20,
    running_only: bool = False,
    shared_only: bool = False,
    cluster_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    List clusters with optional filtering, or fetch a specific cluster by ID.

    Optimized to iterate through clusters and stop early when limit is reached.
    RUNNING clusters are always listed first.

    Args:
        limit: Maximum number of clusters to return (default: 20)
        running_only: If True, only return RUNNING clusters
        shared_only: If True, only return shared access mode clusters (not single-user)
        cluster_id: If provided, fetch only this specific cluster by ID

    Returns:
        List of cluster dictionaries with keys:
        - id: Cluster ID
        - name: Cluster name
        - state: Current state (RUNNING, TERMINATED, PENDING, etc.)
        - spark_version: Spark version
        - node_type_id: Node type
        - autotermination_minutes: Auto-termination timeout
        - creator_user_name: Who created the cluster
        - single_user_name: Assigned user (None for shared clusters)
        - data_security_mode: Security mode

    Raises:
        Exception: If API request fails
    """
    client = get_workspace_client()

    # If cluster_id is provided, fetch only that cluster
    if cluster_id:
        try:
            cluster = client.clusters.get(cluster_id=cluster_id)
            logger.debug(f"Fetched cluster {cluster_id}: {cluster.cluster_name} (state: {cluster.state})")
            return [_cluster_to_dict(cluster)]
        except Exception as e:
            raise Exception(
                f"Failed to get cluster {cluster_id}: {str(e)}. "
                f"Check that the cluster ID is correct and you have permission to view it."
            )

    running_clusters = []
    other_clusters = []

    try:
        # Iterate through clusters with early termination
        for cluster in client.clusters.list():
            # Apply filters
            if shared_only and not _is_shared_access_mode(cluster):
                continue

            is_running = cluster.state == State.RUNNING

            if is_running:
                running_clusters.append(cluster)
                # Early termination: if we only want running and have enough
                if running_only and len(running_clusters) >= limit:
                    break
            elif not running_only:
                other_clusters.append(cluster)

            # Early termination: if we have enough total
            if len(running_clusters) + len(other_clusters) >= limit * 2:
                # Keep some buffer for sorting
                break

    except Exception as e:
        raise Exception(
            f"Failed to list clusters: {str(e)}. "
            f"Check that you have permission to view clusters."
        )

    # Sort each group by name
    running_clusters.sort(key=lambda c: c.cluster_name.lower() if c.cluster_name else "")
    other_clusters.sort(key=lambda c: c.cluster_name.lower() if c.cluster_name else "")

    # Combine: running first, then others
    all_clusters = running_clusters + other_clusters

    # Convert to dicts and limit
    result = [_cluster_to_dict(c) for c in all_clusters[:limit]]

    logger.debug(f"Listed {len(result)} clusters (running: {len(running_clusters)})")
    return result


def get_best_cluster(
    running_only: bool = True,
    name_filter: Optional[str] = None,
) -> Optional[str]:
    """
    Select the best available cluster based on priority rules.

    Optimized to find running shared clusters first, avoiding full list iteration
    in large workspaces when possible.

    Priority:
    1. Running shared-access cluster matching name_filter (or "Shared endpoint"/"dbdemos-shared-endpoint")
    2. Any running shared-access cluster with name_filter (or 'shared') in name
    3. Any running shared-access cluster
    4. Any running single-user cluster (fallback)
    5. (If running_only=False) Stopped shared-access cluster
    6. (If running_only=False) Any stopped cluster

    Args:
        running_only: If True (default), only consider RUNNING clusters
        name_filter: Custom name to search for in cluster names (case-insensitive).
                    If None, defaults to searching for 'shared'.

    Returns:
        Cluster ID string, or None if no clusters available

    Raises:
        Exception: If API request fails
    """
    client = get_workspace_client()

    # Use custom filter or default to 'shared'
    search_term = name_filter.lower() if name_filter else "shared"

    # Default exact match names (only used when no custom filter)
    default_exact_names = ("Shared endpoint", "dbdemos-shared-endpoint") if not name_filter else ()

    # Categories for running clusters
    exact_match_running = []      # Exact name match + shared access
    name_match_running = []       # search_term in name + shared access mode
    shared_access_running = []    # Shared access mode (no name match)
    single_user_running = []      # Single-user (fallback)

    # Categories for stopped clusters (only used if running_only=False)
    shared_stopped = []
    other_stopped = []

    try:
        for cluster in client.clusters.list():
            is_running = cluster.state == State.RUNNING
            is_shared_access = _is_shared_access_mode(cluster)
            name_lower = cluster.cluster_name.lower() if cluster.cluster_name else ""
            has_name_match = search_term in name_lower

            if is_running:
                # Check for exact name match (custom filter or default names)
                if name_filter and is_shared_access and name_lower == search_term:
                    exact_match_running.append(cluster)
                    # Found exact match, stop early
                    break
                elif not name_filter and is_shared_access and cluster.cluster_name in default_exact_names:
                    exact_match_running.append(cluster)
                    # Found ideal cluster, can stop early
                    break
                elif is_shared_access and has_name_match:
                    name_match_running.append(cluster)
                elif is_shared_access:
                    shared_access_running.append(cluster)
                else:
                    single_user_running.append(cluster)

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
    return selected.cluster_id
