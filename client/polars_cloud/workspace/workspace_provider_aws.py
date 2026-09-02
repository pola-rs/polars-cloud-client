from __future__ import annotations

import logging
import time
import webbrowser
from typing import TYPE_CHECKING

from polars_cloud import constants
from polars_cloud._tracing import traced
from polars_cloud.exceptions import VerificationTimeoutError, WorkspaceDeploymentError
from polars_cloud.polars_cloud import AwsConnectionStatusModel
from polars_cloud.workspace._polling import (
    POLLING_INTERVAL_SECONDS_DEFAULT,
    POLLING_TIMEOUT_SECONDS_DEFAULT,
)

if TYPE_CHECKING:
    from uuid import UUID

    from polars_cloud.polars_cloud import WorkspaceAwsConnectionModel

logger = logging.getLogger(__name__)


class WorkspaceProviderAWS:
    """The AWS connection of a workspace.

    You can attach your own AWS account to a workspace to support BYOC on AWS and run
    clusters directly on your own AWS account.

    Parameters
    ----------
    workspace_id
        The id of the workspace to manage the AWS connection of.

    Examples
    --------
    >>> provider = pc.WorkspaceProviderAWS(workspace.id)
    >>> provider.is_connected()
    True
    """

    def __init__(self, workspace_id: UUID) -> None:
        self._workspace_id = workspace_id

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(workspace_id={self._workspace_id!r})"

    def is_connected(self) -> bool:
        """Whether the workspace has an AWS account connected.

        Examples
        --------
        >>> pc.WorkspaceProviderAWS(workspace.id).is_connected()
        True
        """
        connection = self._connection()
        return (
            connection is not None
            and connection.status == AwsConnectionStatusModel.Completed
        )

    def _connection(self) -> WorkspaceAwsConnectionModel | None:
        return constants.API_CLIENT.get_workspace_aws_connection(self._workspace_id)

    @traced
    def connect(self, *, verify: bool = True) -> None:
        """Connect an AWS account to the workspace.

        This method will allow you to setup a connection with your AWS account
        and Polars Cloud. It opens a CloudFormation setup flow in a browser.

        Does nothing if the workspace already has an AWS account connected.

        Parameters
        ----------
        verify
            Wait for the AWS connection to be established.

        Examples
        --------
        >>> pc.WorkspaceProviderAWS(workspace.id).connect()
        Please complete the workspace setup process in your browser.
        Workspace creation may take up to 5 minutes to complete after clicking
        'Create stack'. If your browser did not open automatically,
        please go to the following URL:
        [URL]
        """
        if self.is_connected():
            logger.debug("workspace is already connected to AWS")
            print(
                "The workspace is already connected to AWS.\n"
                "Disconnect it first if you want to connect a different account."
            )
            return

        setup_urls = constants.API_CLIENT.get_workspace_setup_url(self._workspace_id)

        logger.debug("opening web browser")
        _open_browser(setup_urls.full_setup_url)

        if verify:
            logger.info("verifying AWS connection")
            self.wait_until_connected()

        logger.info("AWS connection successful")

    @traced
    def disconnect(self) -> None:
        """Disconnect the AWS account from the workspace.

        Does nothing if the workspace has no AWS account connected.

        Examples
        --------
        >>> pc.WorkspaceProviderAWS(workspace.id).disconnect()
        To finish removing the AWS connection, delete the [STACK] CloudFormation
        stack in AWS.
        """
        if not self.is_connected():
            logger.debug("workspace has no AWS connection to disconnect")
            print(
                "The workspace has no AWS account connected, "
                "so there is nothing to disconnect."
            )
            return

        logger.debug("calling workspace AWS disconnect endpoint")
        stack = constants.API_CLIENT.delete_workspace_aws_connection(self._workspace_id)

        _open_cloudformation_console(stack.stack_name, stack.url)

    @traced
    def wait_until_connected(
        self,
        *,
        interval: int = POLLING_INTERVAL_SECONDS_DEFAULT,
        timeout: int = POLLING_TIMEOUT_SECONDS_DEFAULT,
    ) -> None:
        """Block until the workspace has an AWS account connected.

        Parameters
        ----------
        interval
            Number of seconds between checks.
        timeout
            Number of seconds to wait before giving up.

        Raises
        ------
        WorkspaceDeploymentError
            If the CloudFormation stack rolled back, or the connection was removed
            while we were waiting for it.
        VerificationTimeoutError
            If the connection was not established within the timeout.

        Examples
        --------
        >>> pc.WorkspaceProviderAWS(workspace.id).wait_until_connected()
        """
        max_polls = int(timeout / interval) + 1
        logger.debug("polling workspace AWS connection endpoint")

        prev_status = None
        connection = None
        for poll in range(max_polls):
            if poll > 0:
                time.sleep(interval)

            connection = self._connection()
            status = connection.status if connection is not None else None
            logger.debug("current AWS connection status: %s", status)

            if status == AwsConnectionStatusModel.Completed:
                logger.info("AWS connection successfully verified")
                return

            # Connecting after a disconnect or a rolled back stack keeps reporting the
            # previous attempt's status until CloudFormation calls back, so only a
            # change into an end state tells us anything about this attempt.
            if poll > 0 and status != prev_status:
                if status == AwsConnectionStatusModel.Failed:
                    msg = "Connecting AWS failed." + _cloudformation_hint(connection)
                    logger.debug(msg)
                    raise WorkspaceDeploymentError(msg)
                elif status == AwsConnectionStatusModel.Deleted:
                    msg = "The AWS connection was removed while we were connecting it."
                    logger.debug(msg)
                    raise WorkspaceDeploymentError(msg)

            prev_status = status

        msg = "Verifying the AWS connection has timed out." + _cloudformation_hint(
            connection
        )
        logger.debug(msg)
        raise VerificationTimeoutError(msg)


def _cloudformation_hint(connection: WorkspaceAwsConnectionModel | None) -> str:
    """Point the user at the CloudFormation stack, by link when we know where it is."""
    hint = " Check the status of the deployment in your AWS CloudFormation dashboard"
    console_url = connection.console_url if connection is not None else None
    if console_url:
        hint += f" or by following this link: {console_url}"
    return hint


def _open_browser(url: str) -> None:
    """Open a web browser for the user at the specified URL."""
    webbrowser.open(url)
    print(
        "Please complete the aws connection setup process in your browser.\n"
        "It may take up to 5 minutes to complete after clicking 'Create stack'.\n"
        "If your browser did not open automatically, please go to the following URL:\n"
        f"{url}"
    )


def _open_cloudformation_console(stack_name: str, url: str) -> None:
    print(
        f"To finish removing the AWS connection, delete the {stack_name} CloudFormation stack in AWS.\n"
        "The workspace is kept, along with any clusters it runs outside of AWS.\n"
        "You will be redirected to the AWS CloudFormation console in 5 seconds to complete the process."
    )
    time.sleep(5)
    webbrowser.open(url)
