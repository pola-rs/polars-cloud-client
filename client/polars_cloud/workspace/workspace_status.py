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
    """State of the workspace."""

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
    def _from_api_schema(cls, schema: pcr.WorkspaceStateSchema) -> Self:
        """Parse API result into a Python object."""
        if schema == pcr.WorkspaceStateSchema.Uninitialized:
            return cls.Uninitialized
        elif schema == pcr.WorkspaceStateSchema.Pending:
            return cls.Pending
        elif schema == pcr.WorkspaceStateSchema.Active:
            return cls.Active
        elif schema == pcr.WorkspaceStateSchema.Failed:
            return cls.Failed
        elif schema == pcr.WorkspaceStateSchema.Deleted:
            return cls.Deleted
        else:
            msg = f"Unknown type found for workspace status {schema}"
            raise RuntimeError(msg)

    def __repr__(self) -> str:
        return self.name
