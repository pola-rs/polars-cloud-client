from __future__ import annotations

from polars_cloud import Organization, Workspace, WorkspaceStatus, constants
from polars_cloud.cli.commands._utils import handle_errors
from polars_cloud.polars_cloud import WorkspaceStateSchema


def _select_or_set_up_organization(
    organization_name: str | None,
) -> Organization | None:
    organizations = constants.API_CLIENT.get_organizations(name=organization_name)

    if organization_name:
        for org in organizations:
            if org.name == organization_name:
                return Organization._from_api_schema(org)

        return Organization.setup(organization_name)

    if not organizations:
        organization_name = input("New organization name: ")
        return Organization.setup(organization_name)

    print(f"\nFound {len(organizations)} available organizations:")
    print("-" * 45)
    print(f"{'#':<3} {'Name':<25}")
    print("-" * 45)

    for i, org in enumerate(organizations, 1):
        print(f"{i:<3} {org.name[:25]:<25}")
    print(f"{len(organizations) + 1:<3} {'<Create new>':<25}")

    while True:
        try:
            choice = (
                input(f"\nSelect organization (1-{len(organizations) + 1} or q): ")
                .strip()
                .lower()
            )
        except KeyboardInterrupt:
            return None

        if choice == "q":
            return None
        elif choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(organizations):
                return Organization._from_api_schema(organizations[idx])
            elif idx == len(organizations):
                organization_name = input("New organization name: ")
                return Organization.setup(organization_name)
        print(f"Enter 1-{len(organizations) + 1} or q")


def _select_or_set_up_workspace(
    org: Organization, workspace_name: str | None
) -> Workspace | None:
    workspaces = constants.API_CLIENT.list_workspaces(
        organization_id=org.id, name=workspace_name
    )

    if workspace_name:
        for ws in workspaces:
            if ws.name == workspace_name:
                workspace = Workspace._from_api_schema(ws)
                workspace.deploy()
                return workspace

        return Workspace.setup(workspace_name, org.name)

    if not workspaces:
        workspace_name = input("New workspace name: ")
        return Workspace.setup(workspace_name, org.name)

    workspaces = [
        ws
        for ws in workspaces
        if ws.status
        in (WorkspaceStateSchema.Uninitialized, WorkspaceStateSchema.Failed)
    ]

    print(
        f"\nFound {len(workspaces)} (re)deployable (Uninitialized/Failed) workspaces:"
    )
    print("-" * 45)
    print(f"{'#':<3} {'Name':<25} {'Status':<12}")
    print("-" * 45)

    for i, ws in enumerate(workspaces, 1):
        status = WorkspaceStatus._from_api_schema(ws.status)
        print(f"{i:<3} {ws.name[:25]:<25} {status.name:<12}")
    print(f"{len(workspaces) + 1:<3} {'<Create new>':<25}")

    while True:
        try:
            choice = (
                input(f"\nSelect workspace (1-{len(workspaces) + 1} or q): ")
                .strip()
                .lower()
            )
        except KeyboardInterrupt:
            return None
        if choice == "q":
            return None
        elif choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(workspaces):
                workspace = Workspace._from_api_schema(workspaces[idx])
                workspace.deploy()
                return workspace
            elif idx == len(workspaces):
                workspace_name = input("New workspace name: ")
                return Workspace.setup(workspace_name, org.name)
        print(f"Enter 1-{len(workspaces) + 1} or q")


def setup(organization_name: str | None, workspace_name: str | None) -> None:
    """Set up an organization and workspace to quickly run your first query.

    Parameters
    ----------
    organization_name
        The desired name of the organization.
    workspace_name
        The desired name of the workspace.
    """
    with handle_errors():
        org = _select_or_set_up_organization(organization_name)
        if org is None:
            return
        _ = _select_or_set_up_workspace(org, workspace_name)
