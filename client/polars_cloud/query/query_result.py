from __future__ import annotations

import builtins
import json
import keyword
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, overload

import polars as pl

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from polars_cloud._typing import (
        PlanType,
    )

# needed for eval
from polars.exceptions import (  # noqa: F401
    ColumnNotFoundError,
    ComputeError,
    DuplicateError,
    InvalidOperationError,
    NoDataError,
    OutOfBoundsError,
    PolarsError,
    SchemaError,
    SchemaFieldNotFoundError,
    ShapeError,
    SQLInterfaceError,
    SQLSyntaxError,
    StringCacheMismatchError,
    StructFieldNotFoundError,
)

from polars_cloud.query.query_status import QueryStatus

if TYPE_CHECKING:
    from typing import Any

    from polars import DataFrame, LazyFrame

    import polars_cloud.polars_cloud as pcr
    from polars_cloud._typing import FileType
    from polars_cloud.query import DirectQuery
    from polars_cloud.query.query_info import QueryInfo


def format_result(
    finished_task_info: QueryInfo,
    repr_f: Callable[[pl.DataFrame], str],
) -> str:
    assert finished_task_info.head is not None
    return f"""
total_stages: {finished_task_info.total_stages}
finished_stages: {finished_task_info.finished_stages}
total_rows: {finished_task_info.n_rows_result}
location: {finished_task_info.sink_dst}
head:
{repr_f(finished_task_info.head)}
"""


@dataclass
class StageStatistics:
    """Information about a query stage."""

    inner: pcr.StageStatsPy

    @property
    def num_workers_used(self) -> int:
        """The number of workers used by this stage.

        Examples
        --------
        >>> # run a query
        >>> in_progress = (
        ...     pl.scan_parquet("datasets/").sort("a").remote().distributed().execute()
        ... )  # doctest: +SKIP
        >>> # await the result
        >>> result = in_progress.await_result()  # doctest: +SKIP
        >>> # inspect how many workers used in stage 0
        >>> assert result.stage_statistics(0).num_workers_used > 1  # doctest: +SKIP
        """
        return self.inner.num_workers_used

    def __repr__(self) -> str:
        return f"""StageStatistics{{
    num_workers_used: {self.num_workers_used}
}}"""


class QueryResult:
    """The result of a Polars Cloud query.

    .. note::
     This object should not be instantiated directly by the user.

    """

    def __init__(
        self, result: QueryInfo, status: QueryStatus, query: DirectQuery | None = None
    ):
        self.finished_task_info = result
        self.status = status
        self._query = query

    @property
    def head(self) -> DataFrame | None:
        """The first n rows of the result."""
        return self.finished_task_info.head

    @property
    def n_rows_total(self) -> int | None:
        """Total rows that are outputted by the result."""
        return self.finished_task_info.n_rows_result

    @property
    def total_stages(self) -> int:
        """Get total planned stages of physical plan."""
        return self.finished_task_info.total_stages

    @property
    def failed_stages(self) -> int:
        """Total failed stages."""
        return self.finished_task_info.failed_stages

    @property
    def finished_stages(self) -> int:
        """Total executed stages.

        This can be more than total stages, as failed stages can be rescheduled.
        """
        return self.finished_task_info.finished_stages

    @property
    def location(self) -> list[str] | None:
        """Location where the result is written."""
        return self.finished_task_info.sink_dst

    @property
    def file_type(self) -> FileType:
        """The file type where the result is written to."""
        return self.finished_task_info.sink_type

    @overload
    def stage_statistics(self, stage_number: None = None) -> list[StageStatistics]: ...

    @overload
    def stage_statistics(self, stage_number: int) -> StageStatistics | None: ...

    def stage_statistics(
        self, stage_number: int | None = None
    ) -> StageStatistics | None | list[StageStatistics]:
        """Return the statistics of a specific stage.

        Parameters
        ----------
        stage_number
            if None this will give a list of stages.

        """
        stages_stats = self.finished_task_info.inner.stages_stats
        if stage_number is None:
            out: list[StageStatistics] = []
            i = 0
            if stages_stats is None:
                return out

            while True:
                item = stages_stats.get(i)
                if item is None:
                    return out
                out.append(StageStatistics(inner=item))
                i += 1

        if stages_stats is None:
            return None
        return StageStatistics(inner=stages_stats.get(stage_number))

    def lazy(self) -> LazyFrame:
        """Convert the `QueryResult` into a `LazyFrame`."""
        if self.status != QueryStatus.SUCCESS:
            msg = "a query must be successful to convert it to a 'LazyFrame'"
            raise InvalidOperationError(msg)

        file_type = self.file_type
        location = self.location

        if location is None or len(location) == 0:
            msg = "cannot create a 'LazyFrame', the result location is unknown"
            raise InvalidOperationError(msg)

        if file_type == "parquet":
            return pl.scan_parquet(location)
        elif file_type == "csv":
            return pl.scan_csv(location)
        elif file_type == "ipc":
            return pl.scan_ipc(location)
        elif file_type == "ndjson":
            return pl.scan_ndjson(location)
        elif file_type == "json":
            msg = "JSON not yet supported"
            raise InvalidOperationError(msg)
        else:
            msg = f"{file_type} not yet supported"
            raise InvalidOperationError(msg)

    def __repr__(self) -> str:
        if self.finished_task_info.head is not None:
            # If the query is successful, just show the (truncated) result
            with pl.Config(tbl_hide_dataframe_shape=True):
                return format_result(self.finished_task_info, repr)
        else:
            return f"""
            status: {self.status}
            {self.finished_task_info.__repr__()}"""

    def _repr_html_(self) -> str:
        """Format output data in HTML for display in Jupyter Notebooks."""

        def _format_table(rows: list[tuple[str, Any]]) -> str:
            """Generate HTML table rows from a list of (label, value) tuples."""
            table_rows = "".join(
                f"""
                    <tr>
                        <td style="padding: 4px 12px 4px 0; color: #666;">{label}:</td>
                        <td style="padding: 4px 0; font-weight: 500;">{value}</td>
                    </tr>"""
                for label, value in rows
            )
            return f"""
                <table style="border-collapse: collapse; font-family: monospace; font-size: 12px;">
                    {table_rows}
                </table>"""

        def _format_section(title: str, content: str) -> str:
            """Generate an HTML section with a title and content."""
            return f"""
        <div style="margin-bottom: 16px;">
            <h3 style="margin: 0 0 8px 0; color: #333; font-family: sans-serif; font-size: 14px; font-weight: 600;">{title}</h3>
            <div style="font-family: monospace; font-size: 12px;">
                {content}
            </div>
        </div>
        """

        info = self.finished_task_info
        sections = []

        # Status section (show for all queries)
        status_color = "#4CAF50" if self.status == QueryStatus.SUCCESS else "#f44336"
        status_content = f'<span style="color: {status_color}; font-weight: 600;">{self.status}</span>'
        sections.append(_format_section("Query Status", status_content))

        if self.status == QueryStatus.SUCCESS:
            # Query metrics section
            metrics_rows = [
                ("Total Stages", info.total_stages),
                ("Finished Stages", info.finished_stages),
                (
                    "Total Rows",
                    info.n_rows_result if info.n_rows_result is not None else "N/A",
                ),
            ]
            metrics_table = _format_table(metrics_rows)
            sections.append(_format_section("Query Metrics", metrics_table))

            # Location section
            if info.sink_dst:
                locations_html = "<br>".join(
                    f'<code style="background: #f5f5f5; padding: 2px 6px; border-radius: 3px; font-size: 11px;">{loc}</code>'
                    for loc in info.sink_dst
                )
                sections.append(_format_section("Output Location", locations_html))

            # Head section (only if head is available)
            if info.head is not None:
                with pl.Config(tbl_hide_dataframe_shape=True):
                    head_html = info.head._repr_html_()

                head_section = f"""
        <div style="margin-bottom: 16px;">
            <h3 style="margin: 0 0 8px 0; color: #333; font-family: sans-serif; font-size: 14px; font-weight: 600;">Result Preview</h3>
            <div>{head_html}</div>
        </div>
        """
                sections.append(head_section)
        else:
            # For failed/non-successful queries, show additional info
            error_content = (
                f'<div style="white-space: pre-wrap;">{info.__repr__()}</div>'
            )
            sections.append(_format_section("Details", error_content))

        # Combine all sections
        return f"""
        <div style="padding: 12px; border: 1px solid #ddd; border-radius: 4px; background: white;">
            {"".join(sections)}
        </div>
        """

    def raise_err(self) -> None:
        assert self.finished_task_info.errors is not None
        raise_query_errors(self.finished_task_info.errors)

    def graph(
        self,
        plan_type: PlanType = "physical",
        *,
        show: bool = True,
        output_path: str | Path | None = None,
        raw_output: bool = False,
        figsize: tuple[float, float] = (16.0, 12.0),
    ) -> str | None:
        """Return the query plan as dot diagram.

        .. note::
            This can only be called in 'direct' mode.

        Parameters
        ----------
        plan_type: {'physical', 'ir'}
            Plan visualization to return.

            * physical: The executed physical plan/stages.
            * ir: The optimized query plan before execution.
        show
            Show the figure.
        output_path
            Write the figure to disk.
        raw_output
            Return dot syntax. This cannot be combined with `show` and/or `output_path`.
        figsize
            Passed to matplotlib if `show == True`.
        """
        if self._query is None:
            msg = "cannot call 'QueryResult.graph' in proxy mode"
            raise ComputeError(msg)

        return self._query.graph(
            plan_type=plan_type,
            show=show,
            output_path=output_path,
            raw_output=raw_output,
            figsize=figsize,
        )

    def plan(
        self,
        plan_type: PlanType = "physical",
    ) -> str:
        """Return the executed plan in string format.

        .. note::
            This can only be called in 'direct' mode.

        Parameters
        ----------
        plan_type: {'physical', 'ir'}
            Plan visualization to return.

            * physical: The executed physical plan/stages.
            * ir: The optimized query plan before execution.

        """
        if self._query is None:
            msg = "cannot call 'QueryResult.plan' in proxy mode"
            raise ComputeError(msg)

        return self._query.plan(plan_type=plan_type)


def decode_error(encoded: str) -> BaseException:
    # This doesn't get passed user input, but only error types,
    # just to be sure guard the input
    def guarded_eval(msg: str) -> Any:
        if not msg.isidentifier() or keyword.iskeyword(msg):
            err = f"{msg!r} is not a valid identifier"
            raise ValueError(err)
        obj = getattr(builtins, msg, None) or globals().get(msg)
        if obj is None or not (
            isinstance(obj, type) and issubclass(obj, BaseException)
        ):
            err = f"{msg!r} is not a known exception"
            raise ValueError(err)
        return obj

    split = encoded.split(":", 1)
    # Evaluate the string to get the class of the error type
    try:
        error_type = guarded_eval(split[0])
        msg = split[1].lstrip()

        # Recreate the instance e.g. `ValueError(msg)`
        error_value = error_type(msg)
    # Fallback if we cannot restore the message
    except (SyntaxError, ValueError):
        error_value = pl.exceptions.ComputeError(f"spawn failed with: {encoded}")
    assert isinstance(error_value, BaseException)
    return error_value


def raise_query_errors(errors: list[str]) -> None:
    """Raise a Python exception from query error strings.

    The errors are JSON-encoded strings from the compute plane, each containing
    a mapping from worker/scheduler to error message.
    """
    parsed_errors = {}  # type: ignore[var-annotated]

    # Deduplicate the errors, by converting to set.
    for err in set(errors):
        # We expect {"worker/scheduler", "error: msg"}
        payload = json.loads(err)
        for key, value in payload.items():
            # Example data
            # key: "worker_id" or "scheduler"
            # value: ValueError: msg

            error_value = decode_error(value)

            # Store the errors per worker/scheduler
            if key not in parsed_errors:
                parsed_errors[key] = []
            parsed_errors[key].append(error_value)

    # Remove the scheduler
    scheduler_err = parsed_errors.pop("scheduler")[0]
    worker_errors = {}

    for worker, err_list in parsed_errors.items():
        worker_errors[worker] = err_list

    try:
        exc = next(iter(worker_errors.values()))[0]
        if sys.version_info >= (3, 11):
            exc.__cause__ = QueryError(
                f"scheduler failed with:\n{scheduler_err}",
                [error for errs in worker_errors.values() for error in errs],
            )
        else:
            msg = f"workers failed with: {parsed_errors}"
            exc.__cause__ = PolarsError(msg)
        raise exc

    except StopIteration:
        raise scheduler_err from None


if sys.version_info >= (3, 11):

    def is_ipython() -> bool:
        try:
            get_ipython()  # type: ignore[name-defined]
        except NameError:
            return False
        else:
            return True

    class QueryError(BaseExceptionGroup[PolarsError]):  # noqa: F821
        def __str__(self) -> str:
            if is_ipython() and not getattr(self, "_is_recursive", False):
                import traceback

                self._is_recursive = True
                return "\n".join(traceback.format_exception(self))
            return super().__str__()
