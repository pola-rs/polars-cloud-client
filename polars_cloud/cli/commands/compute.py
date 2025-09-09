from __future__ import annotations

from typing import TYPE_CHECKING

from polars_cloud import ComputeContext, ComputeContextStatus, Workspace, constants
from polars_cloud.cli.commands._utils import handle_errors

if TYPE_CHECKING:
    from uuid import UUID


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
            instance_type=instance_type,
            storage=storage,
            cluster_size=cluster_size,
        )
        ctx.start(wait=wait)
        print(ctx)


def stop_compute(workspace_name: str, id: UUID, *, wait: bool = False) -> None:
    """Stop a compute cluster."""
    with handle_errors():
        w = Workspace(workspace_name)
        ctx = ComputeContext.connect(workspace=w, compute_id=id)
        ctx.stop(wait=wait)


def get_compute_details(workspace_name: str, id: UUID) -> None:
    """Print the details of a compute cluster."""
    with handle_errors():
        w = Workspace(workspace_name)
        ctx = ComputeContext.connect(id, w.id)

    _print_compute_details(ctx)


def _print_compute_details(details: ComputeContext) -> None:
    """Pretty print the details of a cluster to the console."""
    members = vars(details)
    max_key_len = max(len(key) for key in members)
    col_width = max_key_len + 5
    print(f"{'PROPERTY':<{col_width}} VALUE")
    for key, value in members.items():
        print(f"{key:<{col_width}} {value}")


def list_compute() -> None:
    """List all accessible workspaces."""
    with handle_errors():
        lines = []
        for w in constants.API_CLIENT.list_workspaces(None):
            for c in constants.API_CLIENT.get_compute_clusters(w.id):
                status = ComputeContextStatus._from_api_schema(c.status)
                lines.append(
                    f"{c.id!s:<38} {c.instance_type!s:<15} {w.name!s:<15} {status.name:<10}"
                )

        if len(lines) == 0:
            print("No compute clusters found.")
        else:
            print(f"{'ID':<38} {'INSTANCE TYPE':<15} {'WORKSPACE':<15} {'STATUS':<10}")
            print("\n".join(lines))
