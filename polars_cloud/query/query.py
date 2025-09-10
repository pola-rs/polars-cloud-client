from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from time import sleep, time
from typing import TYPE_CHECKING

import polars as pl

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
from polars.lazyframe.opt_flags import DEFAULT_QUERY_OPT_FLAGS

import polars_cloud.polars_cloud as pcr
from polars_cloud import constants
from polars_cloud._utils import run_coroutine
from polars_cloud.context import (
    ClusterContext,
    ComputeContext,
)
from polars_cloud.context import cache as compute_cache
from polars_cloud.polars_cloud import PlanFormatPy
from polars_cloud.query._utils import prepare_query
from polars_cloud.query.query_info import QueryInfo
from polars_cloud.query.query_progress import QueryProgress
from polars_cloud.query.query_result import QueryResult, decode_error
from polars_cloud.query.query_status import QueryStatus

if TYPE_CHECKING:
    from pathlib import Path
    from uuid import UUID

    from polars import LazyFrame

    from polars_cloud._typing import (
        Engine,
        PlanType,
        PlanTypePreference,
        ShuffleCompression,
        ShuffleFormat,
    )
    from polars_cloud.context import ClientContext
    from polars_cloud.query.dst import Dst


def get_timeout() -> int:
    import os

    # Defaults to 1 year.
    return int(os.environ.get("POLARS_TIMEOUT_MS", 31536000000))


def check_timeout(t0: float, duration: int) -> None:
    elapsed = int((time() - t0) * 1000)
    if duration - elapsed < 0:
        msg = f"POLARS_TIMEOUT_MS has elapsed: time in ms: {duration}"
        raise TimeoutError(msg)


# Not to be mistaken with `polars.InprogressQuery` which is local
class InProgressQueryRemote(ABC):
    """Abstract base class for an in progress remote query."""

    @abstractmethod
    def get_status(self) -> QueryStatus:
        """Get the current status of the query."""

    @abstractmethod
    async def await_result_async(self, *, raise_on_failure: bool = True) -> QueryResult:
        """Await the result of the query asynchronously and return the result."""

    @abstractmethod
    def await_result(self, *, raise_on_failure: bool = True) -> QueryResult:
        """Block the current thread until the query is processed and get a result."""

    @abstractmethod
    def cancel(self) -> None:
        """Cancel the execution of the query."""

    async def _poll_status_until_done_async(self) -> QueryStatus:
        """Poll the status of the query until it is either completed or failed."""
        i = 0
        ms = get_timeout()
        t0 = time()
        while not (status := self.get_status()).is_done():
            i += 1
            await asyncio.sleep(min(1, 0.05 * 1.5 ** min(30, i)))
            check_timeout(t0, ms)

        return status

    def _poll_status_until_done(self) -> QueryStatus:
        """Poll the status of the query until it is either completed or failed."""
        i = 0
        ms = get_timeout()
        t0 = time()
        while not (status := self.get_status()).is_done():
            i += 1
            sleep(min(1, 0.05 * 1.5 ** min(30, i)))
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
        schema = constants.API_CLIENT.get_query(self._workspace_id, self._query_id)
        query_status = QueryStatus._from_api_schema(
            schema.state_timing.last_known_state
        )
        return query_status

    def _get_result(
        self, status: QueryStatus, *, raise_on_failure: bool = True
    ) -> QueryResult:
        result_raw = constants.API_CLIENT.get_query_result(self._query_id)
        result = QueryResult(
            result=QueryInfo(id=self._query_id, inner=result_raw), status=status
        )

        if raise_on_failure and status == QueryStatus.FAILED:
            result.raise_err()

        return result

    async def await_result_async(self, *, raise_on_failure: bool = True) -> QueryResult:
        status = await self._poll_status_until_done_async()
        return self._get_result(status, raise_on_failure=raise_on_failure)

    def await_result(self, *, raise_on_failure: bool = True) -> QueryResult:
        status = self._poll_status_until_done()
        return self._get_result(status, raise_on_failure=raise_on_failure)

    def cancel(self) -> None:
        constants.API_CLIENT.cancel_proxy_query(
            workspace_id=self._workspace_id, query_id=self._query_id
        )


@dataclass
class DistributionSettings:
    sort_partitioned: bool = True
    pre_aggregation: bool = True


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

    def __init__(self, query_id: UUID, client: pcr.SchedulerClient):
        self._query_id = query_id
        self._client = client
        self._tag: bytes | None = None

    def get_status(self) -> QueryStatus:
        status_code = self._client.get_direct_query_status(
            self._query_id,
        )
        return QueryStatus._from_api_schema(status_code)

    def get_progress(self) -> QueryProgress | None:
        """Get the current progress of the query if available."""
        self._tag = None
        return self._get_progress()

    def _get_progress(self) -> QueryProgress | None:
        progress_py = self._client.get_direct_query_progress(
            self._query_id,
            self._tag,
        )

        if progress_py is None:
            return None

        self._tag = progress_py.tag

        progress = QueryProgress(self._query_id, progress_py)
        return progress

    def _get_result(
        self, status: QueryStatus, *, raise_on_failure: bool = True
    ) -> QueryResult:
        query_info_py = self._client.get_direct_query_result(self._query_id)
        query_info = QueryInfo(self._query_id, query_info_py)
        result = QueryResult(result=query_info, status=status)

        if raise_on_failure and status == QueryStatus.FAILED:
            result.raise_err()

        return result

    async def await_result_async(self, *, raise_on_failure: bool = True) -> QueryResult:
        status = await self._poll_status_until_done_async()
        return self._get_result(status, raise_on_failure=raise_on_failure)

    def await_result(self, *, raise_on_failure: bool = True) -> QueryResult:
        status = self._poll_status_until_done()
        return self._get_result(status, raise_on_failure=raise_on_failure)

    async def await_progress_async(self) -> QueryProgress:
        """Await progress for the ongoing query asynchronously."""
        return await self._poll_progress_until_update_async()

    def await_progress(self) -> QueryProgress:
        """Block the thread and wait until progress is made on the query."""
        return self._poll_progress_until_update()

    def cancel(self) -> None:
        self._client.cancel_direct_query(self._query_id)

    async def _poll_progress_until_update_async(self) -> QueryProgress:
        """Poll the progress of the query until there is an update."""
        i = 0
        ms = get_timeout()
        t0 = time()
        while (progress := self._get_progress()) is None:
            i += 1
            await asyncio.sleep(min(1, 0.05 * 1.5 ** min(30, i)))
            check_timeout(t0, ms)

        return progress

    def _poll_progress_until_update(self) -> QueryProgress:
        """Poll the progress of the query until there is an update."""
        i = 0
        ms = get_timeout()
        t0 = time()
        while (progress := self._get_progress()) is None:
            i += 1
            sleep(min(1, 0.05 * 1.5 ** min(30, i)))
            check_timeout(t0, ms)

        return progress

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
            plans = self._client.get_direct_query_plan(query_id=self._query_id, ir=True)
            if plans.format != PlanFormatPy.Dot or plans.ir_plan is None:
                msg = "no dot diagram created for this query.\n\nConsider setting 'plan_type' to 'dot'"
                raise NoDataError(msg)
            dot = plans.ir_plan
        elif plan_type == "physical":
            plans = self._client.get_direct_query_plan(
                query_id=self._query_id, phys=True
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
                query_id=self._query_id, phys=True
            )
            if plans.format != PlanFormatPy.Explain:
                return ""
            return plans.phys_plan or ""
        elif plan_type == "ir":
            plans = self._client.get_direct_query_plan(query_id=self._query_id, ir=True)
            if plans.format != PlanFormatPy.Explain:
                return ""
            return plans.ir_plan or ""
        else:
            msg = "expected either one of {'physical', 'ir'}"
            raise ValueError(msg)


def spawn_many(
    lf: list[LazyFrame],
    *,
    dst: Path | str | Dst,
    context: ComputeContext | None = None,
    engine: Engine = "auto",
    plan_type: PlanTypePreference = "dot",
    labels: None | list[str] = None,
    shuffle_compression: ShuffleCompression = "auto",
    shuffle_format: ShuffleFormat = "auto",
    distributed: DistributionSettings | None | bool = None,
    n_retries: int = 0,
    **optimizations: bool,
) -> list[ProxyQuery] | list[DirectQuery]:
    """Spawn multiple remote queries and await them asynchronously.

    Parameters
    ----------
    lf
        A list of Polars LazyFrame's which should be executed on the compute cluster.
    dst
        Destination to which the output should be written.
        If an URI is passed, it must be an accessible object store location.
        If set to `"local"`, the query is executed locally.
    context
        The context describing the compute cluster that should execute the query.
        If set to `None` (default), attempts to load a valid compute context from the
        following locations in order:

        1. The compute context cache. This contains the last `Compute` context created.
        2. The default compute context stored in the user profile.
    engine : {'auto', 'streaming', 'in-memory', 'gpu'}
        Execute the engine that will execute the query.
        GPU mode is not yet supported, consider opening an issue.
        Setting the engine to GPU requires the compute cluster to have access to GPUs.
        If it does not, the query will fail.
    plan_type: {"dot", "plain"}
        Which output format is preferred.
    labels
        Labels to add to the query (will be implicitly created)
    shuffle_compression : {'auto', 'lz4', 'zstd', 'uncompressed'}
        Compress files before shuffling them. Compression reduces disk and network IO,
        but disables memory mapping.
        Choose "zstd" for good compression performance.
        Choose "lz4" for fast compression/decompression.
        Choose "uncompressed" for memory mapped access at the expense of file size.
    shuffle_format : {'auto', 'ipc', 'parquet'}
        File format to use for shuffles.
    distributed
        Run as as distributed query with these settings. This may run partially
        distributed, depending on the operation, optimizer statistics
        and available machines.
    n_retries
        How often failed tasks should be retried.
    **optimizations
        Optimizations to enable or disable in the query optimizer, e.g.
        `projection_pushdown=False`.

    See Also
    --------
    spawn: Spawn a remote query and await it asynchronously.

    Raises
    ------
    grpc.RpcError
        If the LazyFrame size is too large. See note below.
    """
    return [  # type: ignore[return-value]
        spawn(
            lf_,
            dst=dst,
            context=context,
            engine=engine,
            plan_type=plan_type,
            labels=labels,
            shuffle_compression=shuffle_compression,
            shuffle_format=shuffle_format,
            n_retries=n_retries,
            distributed=distributed,
            **optimizations,  # type: ignore[arg-type]
        )
        for lf_ in lf
    ]


def spawn_many_blocking(
    lf: list[LazyFrame],
    *,
    dst: Path | str | Dst,
    context: ComputeContext | None = None,
    engine: Engine = "auto",
    plan_type: PlanTypePreference = "dot",
    labels: None | list[str] = None,
    shuffle_compression: ShuffleCompression = "auto",
    shuffle_format: ShuffleFormat = "auto",
    distributed: DistributionSettings | None | bool = None,
    n_retries: int = 0,
    **optimizations: bool,
) -> list[QueryResult]:
    """Spawn multiple remote queries and await them while blocking the thread.

    Parameters
    ----------
    lf
        A list of Polars LazyFrame's which should be executed on the compute cluster.
    dst
        Destination to which the output should be written.
        If an URI is passed, it must be an accessible object store location.
        If set to `"local"`, the query is executed locally.
    context
        The context describing the compute cluster that should execute the query.
        If set to `None` (default), attempts to load a valid compute context from the
        following locations in order:

        1. The compute context cache. This contains the last `Compute` context created.
        2. The default compute context stored in the user profile.
    engine : {'auto', 'streaming', 'in-memory', 'gpu'}
        Execute the engine that will execute the query.
        GPU mode is not yet supported, consider opening an issue.
        Setting the engine to GPU requires the compute cluster to have access to GPUs.
        If it does not, the query will fail.
    plan_type: {"dot", "plain"}
        Which output format is preferred.
    labels
        Labels to add to the query (will be implicitly created)
    shuffle_compression : {'auto', 'lz4', 'zstd', 'uncompressed'}
        Compress files before shuffling them. Compression reduces disk and network IO,
        but disables memory mapping.
        Choose "zstd" for good compression performance.
        Choose "lz4" for fast compression/decompression.
        Choose "uncompressed" for memory mapped access at the expense of file size.
    shuffle_format : {'auto', 'ipc', 'parquet'}
        File format to use for shuffles.
    distributed
        Run as as distributed query with these settings. This may run partially
        distributed, depending on the operation, optimizer statistics
        and available machines.
    n_retries
        How often failed tasks should be retried.
    **optimizations
        Optimizations to enable or disable in the query optimizer, e.g.
        `projection_pushdown=False`.

    See Also
    --------
    spawn_blocking: Spawn a remote query and block the thread until the result is ready.

    Raises
    ------
    grpc.RpcError
        If the LazyFrame size is too large. See note below.
    """

    async def run() -> list[QueryResult]:
        in_process = spawn_many(
            lf,
            dst=dst,
            context=context,
            engine=engine,
            plan_type=plan_type,
            labels=labels,
            shuffle_compression=shuffle_compression,
            shuffle_format=shuffle_format,
            n_retries=n_retries,
            distributed=distributed,
            **optimizations,
        )
        tasks = [asyncio.create_task(t.await_result_async()) for t in in_process]
        return await asyncio.gather(*tasks)

    return run_coroutine(run())


def spawn(
    lf: LazyFrame,
    *,
    dst: Path | str | Dst,
    context: ClientContext | None = None,
    partitioned_by: None | str | list[str] = None,
    engine: Engine = "auto",
    plan_type: PlanTypePreference = "dot",
    labels: None | list[str] = None,
    shuffle_compression: ShuffleCompression = "auto",
    shuffle_format: ShuffleFormat = "auto",
    distributed: DistributionSettings | None | bool = None,
    n_retries: int = 0,
    sink_to_single_file: bool | None = None,
    optimizations: pl.QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS,
) -> ProxyQuery | DirectQuery:
    """Spawn a remote query and await it asynchronously.

    Parameters
    ----------
    lf
        The Polars LazyFrame which should be executed on the compute cluster.
    dst
        Destination to which the output should be written.
        If an URI is passed, it must be an accessible object store location.
        If set to `"local"`, the query is executed locally.
    context
        The context describing the compute cluster that should execute the query.
        If set to `None` (default), attempts to load a valid compute context from the
        following locations in order:

        1. The compute context cache. This contains the last `Compute` context created.
        2. The default compute context stored in the user profile.
    partitioned_by
        Partition query by a key
    engine : {'auto', 'streaming', 'in-memory', 'gpu'}
        Execute the engine that will execute the query.
        GPU mode is not yet supported, consider opening an issue.
        Setting the engine to GPU requires the compute cluster to have access to GPUs.
        If it does not, the query will fail.
    plan_type: {"dot", "plain"}
        Which output format is preferred.
    labels
        Labels to add to the query (will be implicitly created)
    shuffle_compression : {'auto', 'lz4', 'zstd', 'uncompressed'}
        Compress files before shuffling them. Compression reduces disk and network IO,
        but disables memory mapping.
        Choose "zstd" for good compression performance.
        Choose "lz4" for fast compression/decompression.
        Choose "uncompressed" for memory mapped access at the expense of file size.
    shuffle_format : {'auto', 'ipc', 'parquet'}
        File format to use for shuffles.
    distributed
        Run as as distributed query with these settings. This may run partially
        distributed, depending on the operation, optimizer statistics
        and available machines.
    n_retries
        How often failed tasks should be retried.
    sink_to_single_file
        Perform the sink into a single file.

        Setting this to `True` can reduce the amount of work that can be done in a
        distributed manner and therefore be more memory intensive and
        slower.
    optimizations
        The optimization passes done during query optimization.

        .. warning::
            This functionality is considered **unstable**. It may be changed
            at any point without it being considered a breaking change.

    Examples
    --------
    >>> ctx = pc.ComputeContext(...)
    >>> lf = pl.scan_parquet(...).group_by(...).agg(...)
    >>> dst = pc.ParquetDst(location="s3://...")
    >>> query = pc.spawn(lf, dst=dst, context=ctx)

    See Also
    --------
    spawn_blocking: Spawn a remote query and block the thread until the result is ready.

    Raises
    ------
    grpc.RpcError
        If the LazyFrame size is too large. See note below.
    """
    if isinstance(distributed, bool):
        if distributed:
            distributed = DistributionSettings()
        else:
            distributed = None
    if not isinstance(lf, pl.LazyFrame):
        msg = f"expected a 'LazyFrame' for 'lf', got {type(lf)}"
        raise TypeError(msg)

    # Set compute context if not given
    if context is None:
        if compute_cache.cached_context is not None:
            context = compute_cache.cached_context
        else:
            context = ComputeContext()

    # Do not check status to avoid network call
    if context._compute_id is None and isinstance(context, ComputeContext):
        context.start()

    plan, settings = prepare_query(
        lf=lf,
        dst=dst,
        partition_by=partitioned_by,
        engine=engine,
        plan_type=plan_type,
        shuffle_compression=shuffle_compression,
        shuffle_format=shuffle_format,
        n_retries=n_retries,
        distributed_settings=distributed,
        sink_to_single_file=sink_to_single_file,
        optimizations=optimizations,
    )

    if isinstance(context, ClusterContext) or (
        isinstance(context, ComputeContext) and context.connection_mode == "direct"
    ):
        client: pcr.SchedulerClient = context._get_direct_client()  # type: ignore[assignment]
        try:
            q_id = client.do_query(plan=plan, settings=settings)
        except pcr.EncodedPolarsError as e:
            raise decode_error(str(e)) from None

        return DirectQuery(q_id, client)
    # Check if we are using the cloud compute context
    elif isinstance(context, ComputeContext):
        assert context._compute_id is not None
        q_id = constants.API_CLIENT.submit_query(
            context._compute_id, plan, settings, labels
        )
        return ProxyQuery(q_id, workspace_id=context.workspace.id)
    else:
        msg = f"Invalid client type: expected ComputeContext/ClusterContext, got: {type(context).__name__}"
        raise ValueError(msg)


def spawn_blocking(
    lf: LazyFrame,
    *,
    dst: Path | str | Dst,
    context: ClientContext | None = None,
    partitioned_by: None | str | list[str] = None,
    engine: Engine = "auto",
    plan_type: PlanTypePreference = "dot",
    labels: None | list[str] = None,
    shuffle_compression: ShuffleCompression = "auto",
    distributed: DistributionSettings | None | bool = None,
    n_retries: int = 0,
    sink_to_single_file: bool | None = None,
    optimizations: pl.QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS,
) -> QueryResult:
    """Spawn a remote query and block the thread until the result is ready.

    Parameters
    ----------
    lf
        The Polars LazyFrame which should be executed on the compute cluster.
    dst
        Destination to which the output should be written.
        If an URI is passed, it must be an accessible object store location.
        If set to `"local"`, the query is executed locally.
    context
        The context describing the compute cluster that should execute the query.
        If set to `None` (default), attempts to load a valid compute context from the
        following locations in order:

        1. The compute context cache. This contains the last `Compute` context created.
        2. The default compute context stored in the user profile.
    partitioned_by
        Partition query by a key
    engine : {'auto', 'streaming', 'in-memory', 'gpu'}
        Execute the engine that will execute the query.
        GPU mode is not yet supported, consider opening an issue.
        Setting the engine to GPU requires the compute cluster to have access to GPUs.
        If it does not, the query will fail.
    plan_type: {"dot", "plain"}
        Which output format is preferred.
    labels
        Labels to add to the query (will be implicitly created)
    shuffle_compression : {'auto', 'lz4', 'zstd', 'uncompressed'}
        Compress files before shuffling them. Compression reduces disk and network IO,
        but disables memory mapping.
        Choose "zstd" for good compression performance.
        Choose "lz4" for fast compression/decompression.
        Choose "uncompressed" for memory mapped access at the expense of file size.
    distributed
        Run as as distributed query with these settings. This may run partially
        distributed, depending on the operation, optimizer statistics
        and available machines.
    n_retries
        How often failed tasks should be retried.
    sink_to_single_file
        Perform the sink into a single file.

        Setting this to `True` can reduce the amount of work that can be done in a
        distributed manner and therefore be more memory intensive and
        slower.
    optimizations
        The optimization passes done during query optimization.

        .. warning::
            This functionality is considered **unstable**. It may be changed
            at any point without it being considered a breaking change.

    See Also
    --------
    spawn: Spawn a remote query and await it asynchronously.

    Raises
    ------
    grpc.RpcError
        If the LazyFrame size is too large. See note below.
    """
    in_process = spawn(
        lf,
        dst=dst,
        context=context,
        partitioned_by=partitioned_by,
        engine=engine,
        plan_type=plan_type,
        labels=labels,
        shuffle_compression=shuffle_compression,
        n_retries=n_retries,
        sink_to_single_file=sink_to_single_file,
        optimizations=optimizations,
    )
    return in_process.await_result()
