# mypy: disable-error-code="comparison-overlap"
import sys
from enum import Enum
from typing import final

from polars_cloud.polars_cloud import QueryStatusCodeSchema

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@final
class QueryStatus(Enum):
    QUEUED = 0
    """Query is queued."""

    SCHEDULED = 1
    """Query is scheduled on the compute plane."""

    INPROGRESS = 2
    """Query is in progress."""

    SUCCESS = 3
    """Query successfully completed."""

    FAILED = 4
    """Query failed."""

    CANCELED = 5
    """Query was cancelled by user."""

    def is_done(self) -> bool:
        """Whether the query is done. Can be successfully or unsuccessfully."""
        return self in [
            QueryStatus.SUCCESS,
            QueryStatus.FAILED,
            QueryStatus.CANCELED,
        ]

    @classmethod
    def _from_api_schema(cls, schema: QueryStatusCodeSchema) -> Self:
        if schema == QueryStatusCodeSchema.Queued:
            return cls.QUEUED
        elif schema == QueryStatusCodeSchema.Scheduled:
            return cls.SCHEDULED
        elif schema == QueryStatusCodeSchema.InProgress:
            return cls.INPROGRESS
        elif schema == QueryStatusCodeSchema.Success:
            return cls.SUCCESS
        elif schema == QueryStatusCodeSchema.Failed:
            return cls.FAILED
        elif schema == QueryStatusCodeSchema.Canceled:
            return cls.CANCELED
        else:
            msg = f"Unknown query status {schema}"
            raise RuntimeError(msg)
