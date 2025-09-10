from __future__ import annotations

import datetime
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING

import polars as pl
from polars.exceptions import (
    NoDataError,
)

if TYPE_CHECKING:
    from pathlib import Path
    from uuid import UUID

    import polars_cloud.polars_cloud as pcr


@dataclass
class QueryProgress:
    id: UUID
    inner: pcr.QueryProgressPy

    @property
    def total_stages(self) -> int | None:
        """Get total planned stages of physical plan."""
        return self.inner.total_stages

    def graph(
        self,
        *,
        show: bool = True,
        output_path: str | Path | None = None,
        raw_output: bool = False,
        figsize: tuple[float, float] = (16.0, 12.0),
    ) -> str | None:
        """Return the query plan as dot diagram.

        Parameters
        ----------
        show
            Show the figure.
        output_path
            Write the figure to disk.
        raw_output
            Return dot syntax. This cannot be combined with `show` and/or `output_path`.
        figsize
            Passed to matplotlib if `show == True`.
        """
        if self.inner.phys_plan_dot is None:
            msg = "no dot plan available for this query yet"
            raise NoDataError(msg)

        dot = self.inner.phys_plan_dot

        # matplotlib <= 3.9 produces a warning, which makes it impossible to show graph
        # when debugging a test, because tests fail on warnings.
        #
        # See https://github.com/matplotlib/matplotlib/issues/30249/
        #
        # The workaround is to suppress the warning.
        #
        # This is needed for matplotlib 3.9, which is the highest available version for
        # Python 3.9. It is fixed in matplotlib 3.10. Once we require Python 3.10,
        # we can also require matplotlib 3.10, and remove this workaround.
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return pl._utils.various.display_dot_graph(
                dot=dot,
                show=show,
                output_path=output_path,
                raw_output=raw_output,
                figsize=figsize,
            )

    def plan(
        self,
    ) -> str:
        """Return the executed physical plan in string format."""
        return (
            self.inner.phys_plan_explain
            if self.inner.phys_plan_explain is not None
            else ""
        )

    @cached_property
    def data(self) -> pl.DataFrame | None:
        """Get the raw progress data."""
        data = self.inner.data
        return pl.read_ipc(data) if data else None

    @property
    def summary(self) -> pl.DataFrame | None:
        """Get a summary of the progress of the query."""
        data = self.data
        if data is None:
            return None
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        return data.group_by(
            pl.col("stage_number"),
            pl.col("span_name"),
            pl.col("end_time").is_not_null().alias("completed"),
            maintain_order=True,
        ).agg(
            worker_ids=pl.col("worker_id").unique(),
            duration=(
                pl.col("end_time").fill_null(now).max() - pl.col("start_time").min()
            ),
            output_rows=pl.col("output_rows").sum(),
        )
