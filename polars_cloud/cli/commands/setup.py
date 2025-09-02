from __future__ import annotations

from polars_cloud import Organization, Workspace
from polars_cloud.cli.commands._utils import handle_errors


def setup(
    organization_name: str | None,
    workspace_name: str | None,
) -> None:
    """Set up an organization and workspace to quickly run your first query.

    Parameters
    ----------
    organization_name
        The desired name of the organization.
    workspace_name
        The desired name of the workspace.
    """
    with handle_errors():
        if organization_name is None:
            organization_name = input("Organization name: ")

        Organization.setup(organization_name)

        if workspace_name is None:
            workspace_name = input("Workspace name: ")

        Workspace.setup(workspace_name, organization_name)
