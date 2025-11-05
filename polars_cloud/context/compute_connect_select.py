from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from polars_cloud.exceptions import ComputeClusterMisspecified

if TYPE_CHECKING:
    from polars_cloud.polars_cloud import ComputeSchema
    from polars_cloud.workspace import Workspace


def select_compute_cluster(
    clusters: list[tuple[Workspace, ComputeSchema]],
) -> int | None:
    if not clusters:
        msg = "`no available compute contexts found.`"
        raise ComputeClusterMisspecified(msg)

    print(f"\nFound {len(clusters)} available clusters:")
    print("-" * 125)
    print(
        f"{'#':<3} {'Workspace':<15} {'Type':<12} {'vCPUs':8} {'Memory':<10} {'Storage':<10} {'Size':<10} {'Runtime':<10} {'ID':<38}"
    )
    print("-" * 125)

    for i, (workspace, cluster) in enumerate(clusters, 1):
        runtime = _format_duration(cluster.request_time)
        instance_type = cluster.instance_type or "Unknown"
        memory = _format_gib(cluster.ram_mib / 1024) if cluster.ram_mib else "Unknown"
        storage = _format_gib(cluster.req_storage) if cluster.req_storage else "Unknown"
        print(
            f"{i:<3} {workspace.name[:15]:<15} {instance_type[:12]:<12} {cluster.vcpus:<8} {memory:<10} "
            f"{storage:<10} {cluster.cluster_size:<10} {runtime:<10} {cluster.id}"
        )
    while True:
        try:
            choice = input(f"\nSelect cluster (1-{len(clusters)}) or 'q': ").strip()
            if choice.lower() == "q":
                return None
            idx = int(choice) - 1
            if 0 <= idx < len(clusters):
                return idx
            print(f"Enter 1-{len(clusters)}")
        except (ValueError, KeyboardInterrupt) as e:
            msg = "`invalid compute context context selected.`"
            raise ComputeClusterMisspecified(msg) from e


def _format_duration(request_time: datetime) -> str:
    duration = datetime.now(tz=timezone.utc) - request_time
    seconds = duration.total_seconds()
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60:.0f}m"
    h, m = seconds // 3600, (seconds % 3600) // 60
    return f"{h:.0f}h{m:.0f}m" if m else f"{h}h"


def _format_gib(memory: int | float) -> str:
    memory_str = f"{memory:.1f}".rstrip("0").rstrip(".")
    return f"{memory_str} GiB"
