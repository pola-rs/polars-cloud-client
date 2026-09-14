"""Query monitoring: export query profiles to Polars Cloud.

Polars (``pl.Config.enable_monitoring``) looks up ``polars_cloud.QueryCloudObserver``
by name and calls it with optional ``workspace`` and ``organization`` strings, so
the callable exported here must keep that name and signature.
"""

from __future__ import annotations

from uuid import UUID

from polars_cloud.polars_cloud import QueryCloudObserver as _QueryCloudObserver
from polars_cloud.workspace import Workspace

__all__ = ["QueryCloudObserver"]


def _parse_name_or_id(value: str | None) -> UUID | str | None:
    """Read a workspace or organization given as one string: a UUID id, else a name."""
    if value is None:
        return None
    try:
        return UUID(value)
    except ValueError:
        return value


def QueryCloudObserver(
    workspace: str | None = None,
    organization: str | None = None,
) -> _QueryCloudObserver:
    """Create the observer that exports query profiles to Polars Cloud.

    Parameters
    ----------
    workspace
        Name or id of the workspace query profiles are exported to. Defaults to the
        default workspace of the account.
    organization
        Name or id of the organization the workspace belongs to. Only needed to
        disambiguate when the account has access to several workspaces with the
        same name.

    Raises
    ------
    WorkspaceResolveError
        If the workspace does not exist, is ambiguous, does not belong to the
        organization, or no default is set.
    OrganizationResolveError
        If the organization does not exist or is ambiguous.
    """
    parsed_workspace = _parse_name_or_id(workspace)
    workspace_id = Workspace(
        name=parsed_workspace if isinstance(parsed_workspace, str) else None,
        id=parsed_workspace if isinstance(parsed_workspace, UUID) else None,
        organization=_parse_name_or_id(organization),
    ).id
    return _QueryCloudObserver(workspace_id)
