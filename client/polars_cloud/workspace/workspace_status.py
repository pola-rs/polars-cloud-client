import sys
from enum import Enum
from typing import final

import polars_cloud.polars_cloud as pcr

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@final
class WorkspaceStatus(Enum):
    """State of the workspace.

    .. deprecated:: 0.11.0
        Workspace status and deployment have been deprecated and will be removed in
        future versions to support multiple infrastructure providers. Use
        `.aws.is_connected()` to check whether AWS is connected.
    """

    Uninitialized = 0
    """Workspace is not yet deployed in cloud environment."""

    Pending = 1
    """Workspace is being deployed."""

    Active = 2
    """Workspace is active."""

    Failed = 3
    """Workspace deployment failed."""

    Deleted = 4
    """Workspace is deleted."""

    @classmethod
    def _from_api_model(cls, model: pcr.WorkspaceStateModel) -> Self:
        """Parse API result into a Python object."""
        if model == pcr.WorkspaceStateModel.Uninitialized:
            return cls.Uninitialized
        elif model == pcr.WorkspaceStateModel.Pending:
            return cls.Pending
        elif model == pcr.WorkspaceStateModel.Active:
            return cls.Active
        elif model == pcr.WorkspaceStateModel.Failed:
            return cls.Failed
        elif model == pcr.WorkspaceStateModel.Deleted:
            return cls.Deleted
        else:
            msg = f"Unknown type found for workspace status {model}"
            raise RuntimeError(msg)

    def __repr__(self) -> str:
        return self.name
