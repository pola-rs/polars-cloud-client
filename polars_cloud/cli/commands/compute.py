from __future__ import annotations

from typing import TYPE_CHECKING

from polars_cloud import ComputeContext, Workspace
from polars_cloud.cli.commands._utils import handle_errors

if TYPE_CHECKING:
    from uuid import UUID

    from polars_cloud.context.compute_status import ComputeContextStatus


def start_compute(
    *,
    workspace: str | UUID | None = None,
    cpus: int | None = None,
    memory: int | None = None,
    instance_type: str | None = None,
    storage: int | None = None,
    cluster_size: int = 1,
    wait: bool = False,
) -> None:
    """Start a compute cluster."""
    with handle_errors():
        ctx = ComputeContext(
            workspace=workspace,
            cpus=cpus,
            memory=memory,
            storage=storage,
            cluster_size=cluster_size,
        )
        ctx.start(wait=wait)
        print(ctx)


def stop_compute(workspace_name: str, id: UUID, *, wait: bool = False) -> None:
    """Stop a compute cluster."""
    with handle_errors():
        w = Workspace(workspace_name)
        ctx = ComputeContext.connect(w.id, id)
        ctx.stop(wait=wait)


def get_compute_details(workspace_name: str, id: UUID) -> None:
    """Print the details of a compute cluster."""
    with handle_errors():
        w = Workspace(workspace_name)
        ctx = ComputeContext.connect(w.id, id)

    _print_compute_details(ctx)


def _print_compute_details(details: ComputeContext) -> None:
    """Pretty print the details of a workspace to the console."""
    members = vars(details)
    max_key_len = max(len(key) for key in members)
    col_width = max_key_len + 5
    print(f"{'PROPERTY':<{col_width}} VALUE")
    for key, value in members.items():
        print(f"{key:<{col_width}} {value}")


def list_compute() -> None:
    """List all accessible workspaces."""
    with handle_errors():
        workspaces = Workspace.list()
        compute_clusters = [c for w in workspaces for c in ComputeContext.list(w.id)]

    _print_compute_list(compute_clusters)


def _print_compute_list(
    compute_clusters: list[tuple[ComputeContext, ComputeContextStatus]],
) -> None:
    """Pretty print the list of compute contexts to the console."""
    if not compute_clusters:
        print("No compute clusters found.")
        return

    print(f"{'ID':<38} {'INSTANCE TYPE':<15} {'WORKSPACE':<15} {'STATUS':<10}")
    for cluster, status in compute_clusters:
        instance_type = cluster.instance_type
        workspace_name = cluster.workspace.name
        cluster_id = cluster._compute_id

        print(
            f"{cluster_id!s:<38} {instance_type!s:<15} {workspace_name!s:<15} {status!r:<10}"
        )
