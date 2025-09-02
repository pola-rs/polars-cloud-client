from __future__ import annotations

import logging
import time
import webbrowser
from functools import cached_property
from typing import TYPE_CHECKING
from uuid import UUID

import polars_cloud.polars_cloud as pcr
from polars_cloud import constants
from polars_cloud.exceptions import (
    OrganizationResolveError,
    VerificationTimeoutError,
    WorkspaceResolveError,
)
from polars_cloud.organization import Organization
from polars_cloud.workspace.workspace_compute_default import (
    WorkspaceDefaultComputeSpecs,
)
from polars_cloud.workspace.workspace_status import WorkspaceStatus

if TYPE_CHECKING:
    import sys

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self

logger = logging.getLogger(__name__)

POLLING_INTERVAL_SECONDS_DEFAULT = 2
POLLING_TIMEOUT_SECONDS_DEFAULT = 300


class Workspace:
    """Polars Workspace.

    Parameters
    ----------
    name
        Name of the workspace.
    id
        Workspace identifier.
    """

    def __init__(
        self,
        name: str | None = None,
        *,
        id: UUID | None = None,
        organization: str | UUID | Organization | None = None,
    ):
        """Creates a workspace object for an existing workspace.

        Parameters
        ----------
        name
            The workspace name.
        id
            The workspace id.
        organization
            The organization to load the workspace from. This is useful for when a
            user is in multiple organizations and both of them contain a workspace
            with the same name

        Examples
        --------
        >>> pc.Workspace()
        Workspace(id=UUID('xxxxxxxx-xxxx-7fd0-899b-5aaeefa553d1'),
            name='workspace-name', status=Active, defaults=None)
        """
        self._name = name
        self._id = id
        self._status: None | WorkspaceStatus = None

        if organization is None:
            self._organization = None
        else:
            self._organization = Organization._parse(organization)

        self.load()

        if name is not None and name != self._name:
            msg = f"The provided workspace name {name!r} and id {id!r} do not match. The ID is of an workspace named {self._name!r}."
            raise WorkspaceResolveError(msg)

        # After a load self._organization always only contains the UUID
        if isinstance(organization, UUID) and organization != self.organization._id:
            msg = f"The provided organization id {organization!r} is not the same as the organization id {self.organization._id!r} of the provided workspace"
            raise WorkspaceResolveError(msg)

        if (
            isinstance(organization, Organization)
            and organization._id is not None
            and organization._id != self.organization._id
        ):
            msg = f"The provided organization id {organization._id!r} is not the same as the organization id {self.organization._id!r} of the provided workspace"
            raise WorkspaceResolveError(msg)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"id={self._id!r}, "
            f"name={self._name!r}, "
            f"status={self._status!r})"
        )

    @classmethod
    def _from_api_schema(cls, workspace_schema: pcr.WorkspaceSchema) -> Self:
        """Parse API result into a Python object."""
        self = object.__new__(cls)
        self._update_from_api_schema(workspace_schema)
        return self

    def _update_from_api_schema(self, workspace_schema: pcr.WorkspaceSchema) -> None:
        """Update the object from an API result."""
        self._id = workspace_schema.id
        self._name = workspace_schema.name
        self._status = WorkspaceStatus._from_api_schema(workspace_schema.status)
        self._organization = Organization._from_id_unchecked(
            workspace_schema.organization_id
        )

    @property
    def id(self) -> UUID:
        """Workspace id."""
        if self._id is None:
            self.load()
        assert self._id is not None
        return self._id

    @property
    def name(self) -> str:
        """Workspace name."""
        if self._name is None:
            self.load()
        assert self._name is not None
        return self._name

    @property
    def status(self) -> WorkspaceStatus:
        """Workspace status."""
        if self._status is None:
            self.load()
        assert self._status is not None
        return self._status

    @property
    def organization(self) -> Organization:
        """Workspace status."""
        if self._organization is None:
            self.load()
        assert self._organization is not None
        return self._organization

    @cached_property
    def defaults(self) -> WorkspaceDefaultComputeSpecs | None:
        """Default Cluster Specification."""
        api_defaults = constants.API_CLIENT.get_workspace_default_compute_specs(self.id)
        if not api_defaults:
            return None

        defaults = WorkspaceDefaultComputeSpecs._from_api_schema(api_defaults)

        return defaults

    @classmethod
    def _parse(
        cls,
        workspace: str | Workspace | UUID | None,
    ) -> Self:
        """Create a Workspace based on generic user input."""
        if isinstance(workspace, Workspace):
            return workspace  # type: ignore[return-value]
        elif isinstance(workspace, str):
            return cls(name=workspace)
        elif isinstance(workspace, UUID):
            return cls(id=workspace)
        elif workspace is None:
            return cls()
        else:
            msg = f"Unknown type {type(workspace)}, expected str | Workspace | UUID | None"
            raise RuntimeError(msg)

    def load(self) -> None:
        """Load the workspace details (e.g. name, status, id) from the control plane.

        .. note::

         Depending on the input `load` will load the `Workspace` object by id / name
         or if neither is given it will attempt to get the users default workspace.
        """
        if self._id is not None:
            self._load_by_id()
        elif self._name is not None:
            self._load_by_name()
        else:
            self._load_by_default()

    def _load_by_name(self) -> None:
        """Load the workspace by name."""
        workspaces = constants.API_CLIENT.list_workspaces(self._name)

        # The API endpoint is a substring search, but we only want the exact name
        matches = [ws for ws in workspaces if ws.name == self._name]

        if len(matches) == 0:
            msg = f"Workspace {self._name!r} does not exist"
            raise WorkspaceResolveError(msg)
        elif len(matches) == 1:
            self._update_from_api_schema(matches[0])
        else:
            if self._organization is not None:
                matches = [
                    ws
                    for ws in matches
                    if (
                        ws.organization_id == str(self._organization.id)
                        and ws.name == self._name
                    )
                ]
                if len(matches) == 0:
                    msg = f"The workspace {self._name!r} is not part of the {self._organization.name} organization"
                    raise WorkspaceResolveError(msg)

                self._update_from_api_schema(matches[0])
                return

            msg = (
                f"Multiple workspaces with the same name {self._name!r}.\n\n"
                "Hint: Specify an organization or refer to the workspace by ID\n"
                '`workspace = WorkSpace("workspace", organization_name="organization")`'
            )
            raise WorkspaceResolveError(msg)

    def _load_by_id(self) -> None:
        """Load the workspace by id."""
        assert self._id is not None
        workspace_details = constants.API_CLIENT.get_workspace(self._id)
        self._update_from_api_schema(workspace_details)

    def _load_by_default(self) -> None:
        """Load the workspace by the default of the user."""
        user: pcr.UserSchema = constants.API_CLIENT.get_user()
        if user.default_workspace_id is None:
            msg = (
                "No (default) workspace specified."
                "\n\nHint: Either directly specify the workspace or set your default workspace in the dashboard."
            )
            raise WorkspaceResolveError(msg)
        self._id = user.default_workspace_id

        try:
            self._load_by_id()
        except pcr.NotFoundError as exc:
            msg = (
                "The workspace you had set as default either does not exist anymore or you do not have access anymore."
                "\n\nHint: Set a new default workspace in the dashboard."
            )
            raise WorkspaceResolveError(msg) from exc

    def is_active(self) -> bool:
        """Whether the Workspace is active.

        Examples
        --------
        >>> pc.Workspace("workspace-name").is_active()
        True
        """
        return self.status == WorkspaceStatus.Active

    def wait_until_active(
        self,
        *,
        interval: int = POLLING_INTERVAL_SECONDS_DEFAULT,
        timeout: int = POLLING_TIMEOUT_SECONDS_DEFAULT,
    ) -> bool:
        """Wait until the workspace becomes active.

        Parameters
        ----------
        interval
            The number of seconds between each verification call.
        timeout
            The number of seconds before verification fails.

        Examples
        --------
        >>> pc.Workspace("workspace-name").wait_until_active(timeout=5)
        True
        """
        max_polls = int(timeout / interval) + 1
        prev_status = WorkspaceStatus.Uninitialized
        logger.debug("polling workspace details endpoint")
        for _ in range(max_polls):
            self.load()
            logger.debug("current workspace status: %s", self.status)

            if self.status != prev_status:
                # Log a message when status changes from UNINITIALIZED to PENDING
                if self.status == WorkspaceStatus.Pending:
                    logger.info("workspace stack is being deployed")
                prev_status = self.status

            if self.status in [
                WorkspaceStatus.Uninitialized,
                WorkspaceStatus.Pending,
            ]:
                time.sleep(interval)
                continue
            elif self.status == WorkspaceStatus.Active:
                logger.info("workspace successfully verified")
                return True
            elif self.status in [WorkspaceStatus.Failed, WorkspaceStatus.Deleted]:
                logger.info("workspace verification failed: status is %s", self.status)
                return False

        else:
            msg = (
                "workspace verification has timed out."
                " Either check the status in your AWS CloudFormation dashboard"
                " or (re-)run workspace verification."
            )
            logger.debug(msg)
            raise VerificationTimeoutError(msg)

    def delete(self) -> None:
        """Delete a workspace.

        Examples
        --------
        >>> pc.Workspace("workspace-name").delete()
        Are you sure you want to delete the workspace? (y/n)
        """
        check = input("Are you sure you want to delete the workspace? (y/n)")
        if check not in ["y", "Y"]:
            return
        logger.debug("Calling workspace delete endpoint")
        workspace_info = constants.API_CLIENT.delete_workspace(self.id)

        if workspace_info is not None:
            logger.debug("opening CloudFormation console")
            _open_cloudformation_console(workspace_info.stack_name, workspace_info.url)
        else:
            print("Successfully deleted workspace")

    @classmethod
    def setup(
        cls, workspace_name: str, organization_name: str, *, verify: bool = True
    ) -> Self:
        """Create a new workspace.

        Parameters
        ----------
        workspace_name
            Desired name of the workspace
        organization_name
            Name of the organization
        verify
            Wait for workspace to become active

        Examples
        --------
        >>> pc.Workspace.setup(
        ...     "new-workspace-name", organization_name="organization-name"
        ... )
        Please complete the workspace setup process in your browser.
        Workspace creation may take up to 5 minutes to complete after clicking
        'Create stack'. If your browser did not open automatically,
        please go to the following URL:
        [URL]
        """
        logger.debug("creating workspace")

        organizations = constants.API_CLIENT.get_organizations(organization_name)

        # The API endpoint is a substring search, but we only want the exact name
        matches = [org for org in organizations if org.name == organization_name]

        if len(matches) == 0:
            msg = f"No organization with the name {organization_name!r} was found."
            raise OrganizationResolveError(msg)

        organization_id = matches[0].id
        workspace_schema = constants.API_CLIENT.create_workspace(
            workspace_name, organization_id
        )

        logger.debug("opening web browser")
        _open_browser(workspace_schema.full_url)

        workspace = cls._from_api_schema(workspace_schema.workspace)
        if verify:
            logger.info("verifying workspace creation")
            workspace.wait_until_active()

        logger.info("workspace setup successful")
        return workspace

    @classmethod
    def list(cls, name: str | None = None) -> list[Workspace]:
        """List all workspaces the user has access to.

        Parameters
        ----------
        name
            Filter workspaces by name prefix.

        Examples
        --------
        >>> pc.Workspace.list()
        [Workspace(id=UUID('xxxxxxxx-xxxx-7810-ad2d-0a642bccf80e'),
            name='new-workspace', status=Uninitialized, defaults=None),
            Workspace(id=UUID('xxxxxxxx-xxxx-7e02-9a2c-5ab4a8ed8937'),
            name='workspace-name',status=Active, defaults=None),]

        >>> pc.Workspace.list(name="new")
        [Workspace(id=UUID('xxxxxxxx-xxxx-7810-ad2d-0a642bccf80e'),
            name='new-workspace', status=Uninitialized, defaults=None)]
        """
        return [
            cls._from_api_schema(s) for s in constants.API_CLIENT.list_workspaces(name)
        ]


def _open_browser(url: str) -> None:
    """Open a web browser for the user at the specified URL."""
    webbrowser.open(url)
    print(
        "Please complete the workspace setup process in your browser.\n"
        "Workspace creation may take up to 5 minutes to complete after clicking 'Create stack'.\n"
        "If your browser did not open automatically, please go to the following URL:\n"
        f"{url}"
    )


def _open_cloudformation_console(stack_name: str, url: str) -> None:
    print(
        f"To delete your workspace, remove the {stack_name} CloudFormation stack in AWS, \n"
        "which will automatically notify Polars Cloud and delete the workspace.\n"
        "This action will delete all resources associated with your workspace.\n"
        "You will be redirected to the AWS CloudFormation console in 5 seconds to complete the process."
    )
    time.sleep(5)
    webbrowser.open(url)
