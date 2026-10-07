"""Query monitoring: export query profiles to Polars Cloud.

Polars (``pl.Config.enable_monitoring``) looks up ``polars_cloud.QueryCloudObserver``
by name and calls it with optional ``workspace`` and ``organization`` strings, so
the callable exported here must keep that name and signature.
"""

from __future__ import annotations

from uuid import UUID

from polars_cloud import constants
from polars_cloud.exceptions import WorkspaceResolveError
from polars_cloud.polars_cloud import QueryCloudObserver as _QueryCloudObserver

__all__ = ["QueryCloudObserver"]


def _parse_name_or_id(value: str | UUID | None) -> tuple[str | None, UUID | None]:
    """Read a workspace or organization given as one value into a name or an id."""
    if value is None:
        return None, None
    if isinstance(value, UUID):
        return None, value
    try:
        return None, UUID(value)
    except ValueError:
        return value, None


def QueryCloudObserver(
    workspace: str | UUID | None = None,
    organization: str | UUID | None = None,
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
        If no workspace was given and no default is set anywhere.
    ValueError
        If the workspace or organization does not exist or is ambiguous.
    """
    workspace_name, workspace_id = _parse_name_or_id(workspace)
    organization_name, organization_id = _parse_name_or_id(organization)

    resolved_workspace_id = constants.API_CLIENT.resolve_default_workspace_id(
        workspace_name=workspace_name,
        workspace_id=workspace_id,
        organization_name=organization_name,
        organization_id=organization_id,
    )
    if resolved_workspace_id is None:
        msg = (
            "No (default) workspace specified."
            "\n\nHint: Either directly specify the workspace, set one with"
            " `pc.Workspace('name').set_default()`, set the"
            " `POLARS_CLOUD_DEFAULT_WORKSPACE_ID` environment variable, or set your"
            " default workspace in the dashboard."
        )
        raise WorkspaceResolveError(msg)

    return _QueryCloudObserver(resolved_workspace_id)
