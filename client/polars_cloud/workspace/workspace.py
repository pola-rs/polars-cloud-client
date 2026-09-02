from __future__ import annotations

import logging
import sys
import time
from functools import cached_property
from typing import TYPE_CHECKING
from uuid import UUID

import polars_cloud.polars_cloud as pcr
from polars_cloud import constants
from polars_cloud._tracing import traced
from polars_cloud.exceptions import (
    VerificationTimeoutError,
    WorkspaceDeploymentError,
    WorkspaceResolveError,
)
from polars_cloud.organization import Organization
from polars_cloud.workspace._polling import (
    POLLING_INTERVAL_SECONDS_DEFAULT,
    POLLING_TIMEOUT_SECONDS_DEFAULT,
)
from polars_cloud.workspace.provider_type import ProviderType
from polars_cloud.workspace.workspace_compute_default import (
    WorkspaceDefaultComputeSpecs,
)
from polars_cloud.workspace.workspace_provider_aws import WorkspaceProviderAWS
from polars_cloud.workspace.workspace_status import WorkspaceStatus

if sys.version_info >= (3, 13):
    from warnings import deprecated
else:
    from typing_extensions import deprecated

if TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self

logger = logging.getLogger(__name__)

_DEPRECATED_STATUS_HINT = (
    "Workspace status and deployment have been deprecated and will be removed in"
    " future versions to support multiple infrastructure providers. Use"
    " `.aws.is_connected()` to check whether AWS is connected."
)


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
            name='workspace-name', defaults=None)
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
        return f"{self.__class__.__name__}(id={self._id!r}, name={self._name!r})"

    @classmethod
    def _from_api_model(cls, workspace_model: pcr.WorkspaceModel) -> Self:
        """Parse API result into a Python object."""
        self = object.__new__(cls)
        self._update_from_api_model(workspace_model)
        return self

    def _get_console_url(self) -> str | None:
        if self._id is None or self._deployment != pcr.WorkspaceDeploymentModel.Aws:
            return None

        try:
            stack = constants.API_CLIENT.get_workspace_stack(self._id)
        except Exception:
            logger.debug("could not resolve the workspace stack", exc_info=True)
            return None

        return stack.console_url

    def _update_from_api_model(self, workspace_model: pcr.WorkspaceModel) -> None:
        """Update the object from an API result."""
        self._id = workspace_model.id
        self._name = workspace_model.name
        self._deployment = workspace_model.deployment
        self._status = WorkspaceStatus._from_api_model(workspace_model.status)
        self._organization = Organization._from_id_unchecked(
            workspace_model.organization_id
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
    @deprecated(f"`Workspace.status`: {_DEPRECATED_STATUS_HINT}")
    def status(self) -> WorkspaceStatus:
        """Workspace status.

        .. deprecated:: 0.11.0
            Workspace status and deployment have been deprecated and will be removed in
            future versions to support multiple infrastructure providers. Use
            `.aws.is_connected()` to check whether AWS is connected.
        """
        return self._get_status()

    def _get_status(self) -> WorkspaceStatus:
        """Workspace status, without emitting the deprecation warning."""
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

        defaults = WorkspaceDefaultComputeSpecs._from_api_model(api_defaults)

        return defaults

    @cached_property
    def aws(self) -> WorkspaceProviderAWS:
        """The AWS connection of the workspace.

        Examples
        --------
        >>> pc.Workspace("workspace-name").aws.is_connected()
        True
        """
        return WorkspaceProviderAWS(self.id)

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

    @traced
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
        workspaces = constants.API_CLIENT.get_workspaces(self._name)

        # The API endpoint is a substring search, but we only want the exact name
        matches = [ws for ws in workspaces if ws.name == self._name]

        if len(matches) == 0:
            msg = f"Workspace {self._name!r} does not exist"
            raise WorkspaceResolveError(msg)
        elif len(matches) == 1:
            self._update_from_api_model(matches[0])
        else:
            if self._organization is not None:
                matches = [
                    ws
                    for ws in matches
                    if (
                        ws.organization_id == self._organization.id
                        and ws.name == self._name
                    )
                ]
                if len(matches) == 0:
                    msg = f"The workspace {self._name!r} is not part of the {self._organization.name} organization"
                    raise WorkspaceResolveError(msg)

                self._update_from_api_model(matches[0])
                return

            msg = (
                f"Multiple workspaces with the same name {self._name!r}.\n\n"
                "Hint: Specify an organization or refer to the workspace by ID\n"
                '`workspace = WorkSpace("workspace", organization="organization")`'
            )
            raise WorkspaceResolveError(msg)

    def _load_by_id(self) -> None:
        """Load the workspace by id."""
        assert self._id is not None
        workspace_details = constants.API_CLIENT.get_workspace(self._id)
        self._update_from_api_model(workspace_details)

    def _load_by_default(self) -> None:
        """Load the workspace by the default of the user."""
        user: pcr.UserModel = constants.API_CLIENT.get_user()
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

    @deprecated(f"`Workspace.is_active`: {_DEPRECATED_STATUS_HINT}")
    def is_active(self) -> bool:
        """Whether the Workspace is active.

        .. deprecated:: 0.11.0
            Workspace status and deployment have been deprecated and will be removed in
            future versions to support multiple infrastructure providers. Use
            `.aws.is_connected()` to check whether AWS is connected.

        Examples
        --------
        >>> pc.Workspace("workspace-name").is_active()
        True
        """
        return self._get_status() == WorkspaceStatus.Active

    @deprecated(f"`Workspace.wait_until_active`: {_DEPRECATED_STATUS_HINT}")
    @traced
    def wait_until_active(
        self,
        *,
        interval: int = POLLING_INTERVAL_SECONDS_DEFAULT,
        timeout: int = POLLING_TIMEOUT_SECONDS_DEFAULT,
    ) -> bool:
        """Wait until the workspace becomes active.

        .. deprecated:: 0.11.0
            Workspace status and deployment have been deprecated and will be removed in
            future versions to support multiple infrastructure providers. Use
            `.aws.is_connected()` to check whether AWS is connected.

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
        return self._wait_until_active(interval=interval, timeout=timeout)

    @traced
    def _wait_until_active(
        self,
        *,
        interval: int = POLLING_INTERVAL_SECONDS_DEFAULT,
        timeout: int = POLLING_TIMEOUT_SECONDS_DEFAULT,
    ) -> bool:
        """Wait until the workspace becomes active, without the warning."""
        max_polls = int(timeout / interval) + 1
        logger.debug("polling workspace details endpoint")
        for _ in range(max_polls):
            prev_status = self._get_status()
            self.load()
            status = self._get_status()
            logger.debug("current workspace status: %s", status)

            # End states we can immediately act on
            if status == WorkspaceStatus.Active:
                logger.info("workspace successfully verified")
                return True
            elif status == WorkspaceStatus.Deleted:
                logger.info("workspace verification failed: status is %s", status)
                return False

            if status != prev_status:
                # States we act on only if we changed to them
                # Our workspace might be Failed from an earlier deployment
                if status == WorkspaceStatus.Pending:
                    logger.info("workspace stack is being deployed")
                elif status == WorkspaceStatus.Failed:
                    msg = (
                        "Deploying the workspace failed."
                        " Check the status of the deployment in your AWS CloudFormation dashboard"
                    )
                    console_url = self._get_console_url()
                    if console_url:
                        msg += f" or by following this link: {console_url}"
                    logger.debug(msg)
                    raise WorkspaceDeploymentError(msg)

            time.sleep(interval)
            continue

        if self._get_status() == WorkspaceStatus.Failed:
            msg = (
                "Workspace verification has timed out or we failed to detect a status change."
                " Check the status of the deployment in your AWS CloudFormation dashboard"
            )
        else:
            msg = (
                "Workspace verification has timed out."
                " Check the status of the deployment in your AWS CloudFormation dashboard"
            )

        console_url = self._get_console_url()
        if console_url:
            msg += f" or by following this link: {console_url}"

        logger.debug(msg)
        raise VerificationTimeoutError(msg)

    @traced
    def delete(self) -> None:
        """Delete a workspace.

        The workspace must not have any infrastructure attached to it. Disconnect its
        infrastructure provider first, for example with `.aws.disconnect`.

        Examples
        --------
        >>> pc.Workspace("workspace-name").delete()
        Are you sure you want to delete the workspace? (y/n)
        """
        check = input("Are you sure you want to delete the workspace? (y/n)")
        if check not in ["y", "Y"]:
            return
        logger.debug("Calling workspace delete endpoint")
        constants.API_CLIENT.delete_workspace(self.id)
        print("Successfully deleted workspace")

    @classmethod
    @traced
    def create(cls, workspace_name: str, organization_name: str) -> Self:
        """Create a new workspace.

        The workspace is created without any infrastructure attached to it. Use
        :meth:`deploy` to connect it to your own cloud environment.

        Parameters
        ----------
        workspace_name
            Desired name of the workspace.
        organization_name
            Name of the organization to create the workspace in. The organization must
            already exist.

        Examples
        --------
        >>> pc.Workspace.create("new-workspace-name", "organization-name")
        Workspace(id=UUID('xxxxxxxx-xxxx-7fd0-899b-5aaeefa553d1'),
            name='new-workspace-name'
        """
        organization = Organization(name=organization_name)

        logger.debug("creating workspace")
        workspace_model = constants.API_CLIENT.create_workspace(
            workspace_name, organization.id
        )
        return cls._from_api_model(workspace_model)

    @classmethod
    @deprecated(
        "`setup` has been deprecated, use `create` and `connect_provider` instead."
    )
    @traced
    def setup(
        cls, workspace_name: str, organization_name: str, *, verify: bool = True
    ) -> Self:
        """Create a new workspace and connect it to AWS.

        .. deprecated:: 0.11.0
            Workspace setup has been deprecated and will be removed in future
            versions. Use `Workspace.create` to create the workspace, followed by
            `.connect_provider()` to connect it to your infrastructure.

        See Also
        --------
        create, connect_provider
        """
        workspace = cls.create(workspace_name, organization_name)
        workspace.connect_provider(verify=verify)
        return workspace

    @traced
    def connect_provider(
        self, provider: ProviderType = ProviderType.AWS, *, verify: bool = True
    ) -> None:
        """Connect an infrastructure provider to the workspace.

        The workspace runs its compute on the connected provider. For AWS this opens a
        CloudFormation setup flow in a browser.

        Does nothing if the provider is already connected.

        Parameters
        ----------
        provider
            The infrastructure to run the workspace compute on.
        verify
            Wait for the provider to be connected.

        Examples
        --------
        >>> pc.Workspace("workspace-name").connect_provider()
        Please complete the workspace setup process in your browser.
        Connecting your infrastructure may take up to 5 minutes to complete
        after clicking 'Create stack'. If your browser did not open automatically,
        please go to the following URL:
        [URL]
        """
        if provider == ProviderType.KUBERNETES:
            msg = (
                "Kubernetes deployments cannot be deployed from the client."
                " Install the Polars Cloud Helm chart in your cluster instead."
            )
            raise NotImplementedError(msg)

        self.aws.connect(verify=verify)

    @traced
    @deprecated("`deploy` has been deprecated, use `connect_provider` instead.")
    def deploy(
        self,
        *,
        provider_type: ProviderType = ProviderType.AWS,
        verify: bool = True,
    ) -> None:
        """Connect an existing workspace to your own cloud environment.

        .. deprecated:: 0.11.0
            Workspace deploy has been deprecated and will be removed in future versions.
            Use `Workspace.connect_provider` to connect your provider

        Parameters
        ----------
        provider_type
            The infrastructure to run the workspace compute on.
        verify
            Wait for the deployment to complete

        Examples
        --------
        >>> pc.Workspace("workspace-name").deploy()
        Please complete the workspace setup process in your browser.
        Connecting your infrastructure may take up to 5 minutes to complete
        after clicking 'Create stack'. If your browser did not open automatically,
        please go to the following URL:
        [URL]
        """
        self.connect_provider(provider_type, verify=verify)

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
            name='new-workspace', defaults=None),
            Workspace(id=UUID('xxxxxxxx-xxxx-7e02-9a2c-5ab4a8ed8937'),
            name='workspace-name', defaults=None),]

        >>> pc.Workspace.list(name="new")
        [Workspace(id=UUID('xxxxxxxx-xxxx-7810-ad2d-0a642bccf80e'),
            name='new-workspace', defaults=None)]
        """
        return [
            cls._from_api_model(s) for s in constants.API_CLIENT.get_workspaces(name)
        ]
