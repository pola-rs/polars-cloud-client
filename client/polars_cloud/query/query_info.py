from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from uuid import UUID

    import polars_cloud.polars_cloud as pcr
    from polars_cloud._typing import FileType


@dataclass
class QueryInfo:
    id: UUID
    inner: pcr.QueryInfoPy

    @property
    def head(self) -> pl.DataFrame | None:
        """Get the first n rows the of the result."""
        head = self.inner.head
        return pl.read_ipc(head) if head else None

    @property
    def total_stages(self) -> int:
        """Get total planned stages of physical plan."""
        return self.inner.total_stages

    @property
    def failed_stages(self) -> int:
        """Get failed stages of physical plan."""
        return self.inner.failed_stages

    @property
    def finished_stages(self) -> int:
        """Get finished stages of physical plan."""
        return self.inner.finished_stages

    @property
    def n_rows_result(self) -> int | None:
        """Get total number of rows of the result."""
        return self.inner.n_rows_result

    @property
    def errors(self) -> list[str] | None:
        """Get errors that optionally occurred.

        These still need to be post-processed.
        """
        return self.inner.errors

    @property
    def sink_dst(self) -> list[str] | None:
        """Get the sink destination uri's."""
        return self.inner.sink_dst

    @property
    def sink_type(self) -> FileType:
        """Get the sink destination file type."""
        return self.inner.file_type_sink

    @property
    def ir_plan_explain(self) -> str | None:
        """Get the IR plan."""
        return self.inner.ir_plan_explain

    @property
    def ir_plan_dot(self) -> str | None:
        """Get the IR plan as dot graph."""
        return self.inner.ir_plan_dot

    @property
    def phys_plan_explain(self) -> str | None:
        """Get the physical plan."""
        return self.inner.phys_plan_explain

    @property
    def phys_plan_dot(self) -> str | None:
        """Get the physical plan as dot graph."""
        return self.inner.phys_plan_dot
