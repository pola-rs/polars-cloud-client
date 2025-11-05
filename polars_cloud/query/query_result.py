from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, overload

import polars as pl

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

    def __init__(self, result: QueryInfo, status: QueryStatus):
        self.finished_task_info = result
        self.status = status

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
        if self.finished_task_info.head is not None:
            # If the query is successful, just show the (truncated) result
            with pl.Config(tbl_hide_dataframe_shape=True):
                return format_result(
                    self.finished_task_info, lambda df: df._repr_html_()
                )
        else:
            return repr(self)

    def raise_err(self) -> None:
        errors = {}  # type: ignore[var-annotated]
        assert self.finished_task_info.errors is not None

        # This doesn't get passed user input, but only error types,
        # just to be sure guard the input
        def guarded_eval(msg: str) -> Any:
            if msg.isalnum():
                return eval(msg)
            return None

        # Deduplicate the errors, by converting to set.
        for err in set(self.finished_task_info.errors):
            # We expect {"worker/scheduler", "error: msg"}
            payload = json.loads(err)
            for key, value in payload.items():
                # Example data
                # key: "worker_id" or "scheduler"
                # value: ValueError: msg

                error_value = decode_error(value)

                # Store the errors per worker/scheduler
                if key not in errors:
                    errors[key] = []
                errors[key].append(error_value)

        # Remove the scheduler
        scheduler_err = errors.pop("scheduler")[0]
        worker_errors = {}

        for worker, err_list in errors.items():
            worker_errors[worker] = err_list

        try:
            exc = next(iter(worker_errors.values()))[0]
            if sys.version_info >= (3, 11):
                exc.__cause__ = QueryError(
                    f"scheduler failed with:\n{scheduler_err}",
                    [error for errors in worker_errors.values() for error in errors],
                )
            else:
                msg = f"workers failed with: {errors}"
                exc.__cause__ = PolarsError(msg)
            raise exc

        except StopIteration:
            raise scheduler_err from None


def decode_error(encoded: str) -> BaseException:
    # This doesn't get passed user input, but only error types,
    # just to be sure guard the input
    def guarded_eval(msg: str) -> Any:
        if msg.isalnum():
            return eval(msg)
        return None

    split = encoded.split(":", 1)
    # Evaluate the string to get the class of the error type
    try:
        error_type = guarded_eval(split[0])
        msg = split[1]

        # Recreate the instance e.g. `ValueError(msg)`
        error_value = error_type(msg)
    # Fallback if we cannot restore the message
    except SyntaxError:
        error_value = pl.exceptions.ComputeError(f"spawn failed with: {encoded}")
    assert isinstance(error_value, BaseException)
    return error_value


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
