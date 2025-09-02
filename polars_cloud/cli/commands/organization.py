from __future__ import annotations

from polars_cloud import Organization
from polars_cloud.cli.commands._utils import handle_errors


def list_organizations() -> None:
    """List all accessible organizations."""
    with handle_errors():
        organizations = Organization.list()

    _print_organization_list(organizations)


def _print_organization_list(organizations: list[Organization]) -> None:
    """Pretty print the list of workspaces to the console."""
    if not organizations:
        print("No organizations found.")
        return

    print(f"{'NAME':<15}\t{'ID':<38}")
    for organization in organizations:
        name = organization.name
        name = (name[:14] + "…") if len(name) > 15 else name
        print(f"{name:<15}\t{organization.id!s:<38}")


def set_up_organization(
    organization_name: str | None,
) -> None:
    """Set up an organization.

    Parameters
    ----------
    organization_name
        The name of the organization.
    """
    with handle_errors():
        if organization_name is None:
            organization_name = input("Organization name: ")
        Organization.setup(organization_name)


def delete_organization(name: str) -> None:
    """Delete a workspace."""
    with handle_errors():
        Organization(name=name).delete()


def get_organization_details(name: str) -> None:
    """Get the details of a workspace."""
    with handle_errors():
        organization = Organization(name=name)
        organization.load()
    print(organization)
