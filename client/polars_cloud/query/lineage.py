from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uuid import UUID


@dataclass
class LineageContext:
    """Lineage metadata for a remote query.

    Provided by the user via ``LazyFrameRemote.with_lineage()``.
    Passed through to the OpenLineage emitter on the compute plane.
    """

    job_namespace: str
    job_name: str
    parent_run_id: UUID | None = None
    parent_job_namespace: str | None = None
    parent_job_name: str | None = None
