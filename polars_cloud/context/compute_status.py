import sys
from enum import Enum
from typing import final

import polars_cloud.polars_cloud as pcr

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class UnknownComputeContextStatusException(Exception):
    pass


@final
class ComputeContextStatus(Enum):
    """The status of the compute cluster associated with a `ComputeContext`."""

    UNINITIALIZED = 0
    """Compute Context is not yet initialized with the control plane."""

    STARTING = 1
    """Compute Context is starting."""

    IDLE = 2
    """Compute Context is idle."""

    RUNNING = 3
    """Compute Context is running."""

    STOPPING = 4
    """Compute Context is stopping."""

    STOPPED = 5
    """Compute Context stopped."""

    FAILED = 6
    """Compute Context failed."""

    def is_uninitialized(self) -> bool:
        return self == ComputeContextStatus.UNINITIALIZED

    def is_available(self) -> bool:
        return self in [
            ComputeContextStatus.STARTING,
            ComputeContextStatus.IDLE,
            ComputeContextStatus.RUNNING,
        ]

    def is_stopped(self) -> bool:
        return self in [ComputeContextStatus.STOPPED, ComputeContextStatus.STOPPING]

    def is_failed(self) -> bool:
        return self == ComputeContextStatus.FAILED

    @classmethod
    def _from_api_schema(cls, status: pcr.ComputeStatusSchema) -> Self:
        if status == pcr.ComputeStatusSchema.Starting:
            return cls.STARTING
        elif status == pcr.ComputeStatusSchema.Idle:
            return cls.IDLE
        elif status == pcr.ComputeStatusSchema.Running:
            return cls.RUNNING
        elif status == pcr.ComputeStatusSchema.Stopping:
            return cls.STOPPING
        elif status == pcr.ComputeStatusSchema.Stopped:
            return cls.STOPPED
        elif status == pcr.ComputeStatusSchema.Failed:
            return cls.FAILED
        else:
            msg = f"Unknown ComputeStatus found {status}"
            raise UnknownComputeContextStatusException(msg)

    def __repr__(self) -> str:
        return self.name
