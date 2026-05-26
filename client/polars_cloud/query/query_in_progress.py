from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import polars as pl

try:
    from IPython.display import HTML, clear_output, display

    _IPYTHON_AVAILABLE = True
except ImportError:
    _IPYTHON_AVAILABLE = False

# needed for eval
from polars.exceptions import (  # noqa: F401
    ColumnNotFoundError,
    ComputeError,
    DuplicateError,
    InvalidOperationError,
    NoDataError,
    SchemaError,
    SchemaFieldNotFoundError,
    ShapeError,
    SQLSyntaxError,
    StringCacheMismatchError,
    StructFieldNotFoundError,
)

from polars_cloud import constants
from polars_cloud.polars_cloud import PlanFormatPy
from polars_cloud.query._utils import get_token
from polars_cloud.query.query_detail import QueryDetail
from polars_cloud.query.query_info import QueryInfo
from polars_cloud.query.query_result import QueryResult
from polars_cloud.query.query_status import QueryStatus

if TYPE_CHECKING:
    import threading
    from pathlib import Path
    from uuid import UUID

    import polars_cloud.polars_cloud as pcr
    from polars_cloud._typing import (
        PlanType,
    )
    from polars_cloud.context import ClientContext, ComputeContext


def get_timeout() -> int:
    import os

    # Defaults to 1 year.
    return int(os.environ.get("POLARS_TIMEOUT_MS", 31536000000))


def check_timeout(t0: float, duration: int) -> None:
    elapsed = int((time.time() - t0) * 1000)
    if duration - elapsed < 0:
        msg = f"POLARS_TIMEOUT_MS has elapsed: time in ms: {duration}"
        raise TimeoutError(msg)


# Not to be mistaken with `polars.InProgressQuery` which is local
class InProgressQueryRemote(ABC):
    """Abstract base class for an in progress remote query."""

    @abstractmethod
    def get_status(self) -> QueryStatus:
        """Get the current status of the query."""

    @abstractmethod
    async def await_result_async(self, *, raise_on_failure: bool = True) -> QueryResult:
        """Await the result of the query asynchronously and return the result.

        Parameters
        ----------
        raise_on_failure
            Raise an error if the query failed.
        """

    @abstractmethod
    def await_result(
        self, *, raise_on_failure: bool = True, silent: bool | None = None
    ) -> QueryResult:
        """Block the current thread until the query is processed and get a result.

        Parameters
        ----------
        raise_on_failure
            Raise an error if the query failed.
        silent
            Don't print to stdout during blocking execution.
        """

    @abstractmethod
    def cancel(self) -> None:
        """Cancel the execution of the query."""

    async def _poll_status_until_done_async(self) -> QueryStatus:
        """Poll the status of the query until it is either completed or failed."""
        i = 0
        ms = get_timeout()
        t0 = time.time()
        while not (status := self.get_status()).is_done():
            i += 1
            await asyncio.sleep(min(1, 0.05 * 1.5 ** min(30, i)))
            check_timeout(t0, ms)

        return status

    def _poll_status_until_done(self) -> QueryStatus:
        """Poll the status of the query until it is either completed or failed."""
        i = 0
        ms = get_timeout()
        t0 = time.time()
        while not (status := self.get_status()).is_done():
            i += 1
            time.sleep(min(1, 0.05 * 1.5 ** min(30, i)))
            check_timeout(t0, ms)

        return status


class ProxyQuery(InProgressQueryRemote):
    """A Polars Cloud proxy mode query.

    .. note::
     This object is returned when spawning a new query on a compute cluster running
     in proxy mode. It should not be instantiated directly by the user.

    Examples
    --------
    >>> ctx = pc.ComputeContext(connection_mode="proxy")
    >>> query = lf.remote(ctx).sink_parquet(...)
    >>> type(query)
    <class 'polars_cloud.query.query.ProxyQuery'>
    """

    def __init__(self, query_id: UUID, workspace_id: UUID):
        self._query_id = query_id
        self._workspace_id = workspace_id

    def get_status(self) -> QueryStatus:
        query = constants.API_CLIENT.get_query(self._workspace_id, self._query_id)
        query_status = QueryStatus._from_api_model(query.state_timing.latest_status)
        return query_status

    def _get_result(
        self, status: QueryStatus, *, raise_on_failure: bool = True
    ) -> QueryResult:
        result_raw = constants.API_CLIENT.get_query_result(self._query_id)
        result = QueryResult(
            result=QueryInfo(id=self._query_id, inner=result_raw),
            status=status,
        )

        if raise_on_failure and status == QueryStatus.FAILED:
            result.raise_err()

        return result

    async def await_result_async(
        self, *, raise_on_failure: bool = True, silent: bool | None = None
    ) -> QueryResult:
        with SpinnerRepr(
            query_id=self._query_id, token=None, client=None, silent=silent
        ):
            status = self._poll_status_until_done()
        return self._get_result(status, raise_on_failure=raise_on_failure)

        status = await self._poll_status_until_done_async()
        return self._get_result(status, raise_on_failure=raise_on_failure)

    def await_result(
        self, *, raise_on_failure: bool = True, silent: bool | None = None
    ) -> QueryResult:
        with SpinnerRepr(
            query_id=self._query_id, token=None, client=None, silent=silent
        ):
            status = self._poll_status_until_done()

        return self._get_result(status, raise_on_failure=raise_on_failure)

    def cancel(self) -> None:
        constants.API_CLIENT.cancel_proxy_query(
            workspace_id=self._workspace_id, query_id=self._query_id
        )

    def __repr__(self) -> str:
        status = self.get_status()
        return f"ProxyQuery(id={self._query_id}, status={status})"

    def _repr_html_(self) -> str:
        """Format output data in HTML for display in Jupyter Notebooks."""
        status = self.get_status()
        status_color = (
            "#4CAF50"
            if status.is_done() and status == QueryStatus.SUCCESS
            else "#FFC107"
            if not status.is_done()
            else "#f44336"
        )

        return f"""
        <div style="padding: 12px; border: 1px solid #ddd; border-radius: 4px; background: white; max-width: 600px;">
            <h3 style="margin: 0 0 12px 0; color: #333; font-family: sans-serif; font-size: 16px; font-weight: 600;">Direct InProgressQuery</h3>
            <table style="border-collapse: collapse; font-family: monospace; font-size: 12px; width: 100%;">
                <tr>
                    <td style="padding: 4px 12px 4px 0; color: #666;">Query ID:</td>
                    <td style="padding: 4px 0; font-weight: 500;"><code style="background: #f5f5f5; padding: 2px 6px; border-radius: 3px; font-size: 11px;">{self._query_id}</code></td>
                </tr>
                <tr>
                    <td style="padding: 4px 12px 4px 0; color: #666;">Status:</td>
                    <td style="padding: 4px 0;"><span style="color: {status_color}; font-weight: 600;">{status}</span></td>
                </tr>
            </table>
            <div style="margin-top: 12px; padding-top: 12px; border-top: 1px solid #eee; font-family: sans-serif; font-size: 11px; color: #666;">
                💡 Use <code style="background: #f5f5f5; padding: 2px 4px; border-radius: 3px;">await_result()</code> to get the query result
            </div>
        </div>
        """


class DirectQuery(InProgressQueryRemote):
    """A Polars Cloud direct connect query.

    .. note::
     This object is returned when spawning a new query on a compute cluster running
     in direct connect mode. It should not be instantiated directly by the user.

    Examples
    --------
    >>> ctx = pc.ComputeContext(connection_mode="direct")
    >>> query = lf.remote(ctx).sink_parquet(...)
    >>> type(query)
    <class 'polars_cloud.query.query.DirectQuery'>
    """

    def __init__(
        self,
        query_id: UUID,
        client: pcr.SchedulerClient,
        cluster: ComputeContext | ClientContext,
    ):
        self._query_id = query_id
        self._client = client
        self._cluster = cluster
        self._tag: bytes | None = None

        assert cluster._compute_id is not None

    def get_status(self) -> QueryStatus:
        status_code = self._client.get_direct_query_status(
            self._query_id, token=self._cluster._get_token()
        )
        return QueryStatus._from_api_model(status_code)

    def _get_result(
        self, status: QueryStatus, *, raise_on_failure: bool = True
    ) -> QueryResult:
        query_info_py = self._client.get_direct_query_result(
            self._query_id, token=self._cluster._get_token()
        )
        query_info = QueryInfo(self._query_id, query_info_py)
        result = QueryResult(result=query_info, status=status, query=self)

        if raise_on_failure and status == QueryStatus.FAILED:
            result.raise_err()

        return result

    async def await_result_async(self, *, raise_on_failure: bool = True) -> QueryResult:
        status = await self._poll_status_until_done_async()
        return self._get_result(status, raise_on_failure=raise_on_failure)

    def await_result(
        self, *, raise_on_failure: bool = True, silent: bool | None = None
    ) -> QueryResult:
        token = get_token(self._cluster)

        with SpinnerRepr(
            query_id=self._query_id, token=token, client=self._client, silent=silent
        ):
            status = self._poll_status_until_done()
        return self._get_result(status, raise_on_failure=raise_on_failure)

    def cancel(self) -> None:
        self._client.cancel_direct_query(
            self._query_id, token=self._cluster._get_token()
        )

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
        if plan_type == "ir":
            plans = self._client.get_direct_query_plan(
                query_id=self._query_id, token=self._cluster._get_token(), ir=True
            )
            if plans.format != PlanFormatPy.Dot or plans.ir_plan is None:
                msg = "no dot diagram created for this query.\n\nConsider setting 'plan_type' to 'dot'"
                raise NoDataError(msg)
            dot = plans.ir_plan
        elif plan_type == "physical":
            plans = self._client.get_direct_query_plan(
                query_id=self._query_id, token=self._cluster._get_token(), phys=True
            )
            if plans.format != PlanFormatPy.Dot or plans.phys_plan is None:
                msg = "no dot diagram created for this query.\n\nConsider setting 'plan_type' to 'dot'. If the query wasn't distributed, no physical plan was created."
                raise NoDataError(msg)
            dot = plans.phys_plan
        else:
            msg = f"plan_type should be one of: {{'physical', 'ir'}}, got {plan_type}"
            raise ValueError(msg)

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
        plan_type: PlanType = "physical",
    ) -> str:
        """Return the executed plan in string format.

        Parameters
        ----------
        plan_type: {'physical', 'ir'}
            Plan visualization to return.

            * physical: The executed physical plan/stages.
            * ir: The optimized query plan before execution.

        """
        if plan_type == "physical":
            plans = self._client.get_direct_query_plan(
                query_id=self._query_id, token=self._cluster._get_token(), phys=True
            )
            if plans.format != PlanFormatPy.Explain:
                return ""
            return plans.phys_plan or ""
        elif plan_type == "ir":
            plans = self._client.get_direct_query_plan(
                query_id=self._query_id, token=self._cluster._get_token(), ir=True
            )
            if plans.format != PlanFormatPy.Explain:
                return ""
            return plans.ir_plan or ""
        else:
            msg = "expected either one of {'physical', 'ir'}"
            raise ValueError(msg)

    def __repr__(self) -> str:
        status = self.get_status()
        return f"DirectQuery(id={self._query_id}, status={status})"

    def _repr_html_(self) -> str:
        """Format output data in HTML for display in Jupyter Notebooks."""
        status = self.get_status()
        status_color = (
            "#4CAF50"
            if status.is_done() and status == QueryStatus.SUCCESS
            else "#FFC107"
            if not status.is_done()
            else "#f44336"
        )

        return f"""
        <div style="padding: 12px; border: 1px solid #ddd; border-radius: 4px; background: white; max-width: 600px;">
            <h3 style="margin: 0 0 12px 0; color: #333; font-family: sans-serif; font-size: 16px; font-weight: 600;">Direct InProgressQuery</h3>
            <table style="border-collapse: collapse; font-family: monospace; font-size: 12px; width: 100%;">
                <tr>
                    <td style="padding: 4px 12px 4px 0; color: #666;">Query ID:</td>
                    <td style="padding: 4px 0; font-weight: 500;"><code style="background: #f5f5f5; padding: 2px 6px; border-radius: 3px; font-size: 11px;">{self._query_id}</code></td>
                </tr>
                <tr>
                    <td style="padding: 4px 12px 4px 0; color: #666;">Status:</td>
                    <td style="padding: 4px 0;"><span style="color: {status_color}; font-weight: 600;">{status}</span></td>
                </tr>
            </table>
            <div style="margin-top: 12px; padding-top: 12px; border-top: 1px solid #eee; font-family: sans-serif; font-size: 11px; color: #666;">
                Use <code style="background: #f5f5f5; padding: 2px 4px; border-radius: 3px;">await_result()</code> to get the query result<br>
            </div>
        </div>
        """


class SpinnerRepr:
    def __init__(
        self,
        query_id: UUID,
        token: str | None,
        client: pcr.SchedulerClient | None,
        *,
        silent: bool | None,
    ) -> None:
        self.label = f"computing query: {query_id}"
        self._done = False
        self._thread: None | threading.Thread = None
        self._notebook = _IPYTHON_AVAILABLE and pl._utils.various._in_notebook()
        self._query_id = query_id
        self._client = client
        self._token = token
        self._silent = silent

    def start(self) -> SpinnerRepr:
        self._done = False

        # TODO: Enable this later for top level calls in notebooks
        return self

    def stop(self) -> None:
        self._done = True

        if self._thread is not None:
            self._thread.join()
            if self._notebook:
                clear_output(wait=True)  # type: ignore[no-untyped-call, unused-ignore]
            else:
                print("\r", end=f"query: {self._query_id} done")

    def pretty_html(self) -> None:
        assert self._client is not None
        try:
            while not self._done:
                detail = QueryDetail._from_inner(
                    self._client.get_query_details(self._query_id, self._token)
                )
                clear_output(wait=True)  # type: ignore[no-untyped-call, unused-ignore]
                display(HTML(detail._repr_html_()))  # type: ignore[no-untyped-call, unused-ignore]
                time.sleep(0.5)
        except RuntimeError:
            # cannot access observatory
            self.spin()

    def pretty(self) -> None:
        assert self._client is not None
        frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        n = len(frames)
        i = 0
        label = ""
        try:
            while not self._done:
                print(f"\r{frames[i % n]} {label:<75}", end="", flush=True)

                if i % 5 == 0:
                    detail = QueryDetail._from_inner(
                        self._client.get_query_details(self._query_id, self._token)
                    )
                    metrics = [
                        f"{len(detail.finished_stages)}/{detail.total_num_stages} stages"
                    ]
                    if detail.total_rows_read is not None:
                        metrics.append(f"{detail.total_rows_read} rows read")
                    if detail.total_bytes_shuffled is not None:
                        metrics.append(f"{detail.total_bytes_shuffled} bytes shuffled")
                    label = " | ".join(metrics)

                i += 1

                time.sleep(0.1)
        except RuntimeError:
            # cannot access observatory
            self.spin()

    def spin(self) -> None:
        frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        n = len(frames)
        i = 0
        while not self._done:
            print(f"\r{frames[i % n]} {self.label}", end="", flush=True)
            i += 1
            time.sleep(0.1)

    def __enter__(self) -> SpinnerRepr:
        return self.start()

    def __exit__(self, *args) -> None:  # type: ignore[no-untyped-def]
        self.stop()
