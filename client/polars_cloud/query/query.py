from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

try:
    from IPython.core.getipython import get_ipython as _get_ipython
except ImportError:
    _get_ipython = None  # type: ignore[assignment]

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
from polars_cloud import config as pc_cfg
from polars_cloud import constants
from polars_cloud._utils import run_coroutine
from polars_cloud.context import (
    ClusterContext,
    ComputeContext,
)
from polars_cloud.context import cache as compute_cache
from polars_cloud.query._utils import get_token, prepare_query
from polars_cloud.query.query_in_progress import DirectQuery, ProxyQuery
from polars_cloud.query.query_result import decode_error

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pathlib import Path

    from polars import LazyFrame

    from polars_cloud._typing import (
        Engine,
        PlanTypePreference,
        ShuffleCompression,
        ShuffleFormat,
        SingleWorkerOps,
    )
    from polars_cloud.context import ClientContext
    from polars_cloud.query.dst import Dst
    from polars_cloud.query.lineage import LineageContext
    from polars_cloud.query.query_result import QueryResult


class DistributionSettings:
    def __init__(
        self,
        *,
        equi_join_broadcast_limit: int = 256 * 1024**2,
        partitions_per_worker: int | None = None,
        single_worker_ops: SingleWorkerOps = "auto",
        expression_lowering: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Settings that control the distributed planner or execution.

        Parameters
        ----------
        equi_join_broadcast_limit
            Whether equi joins are allowed to be converted from partitioned to
            broadcasted. The passed value is the maximum size in bytes to broadcasted.
            Set to 0 to disable broadcasting.
        partitions_per_worker
            Into how many parts to split the data when distributing work over workers.
            A higher number means less peak memory usage, but might mean slightly
            less performant execution.
        single_worker_ops
            Whether to allow memory-intensive operations to execute on a single worker.
            This can lead to a faster execution, but it also increases a risk of running
            out of RAM.
        expression_lowering
            Whether individual expressions can be lowered into distributed operations.
        kwargs
            Extra unstable args not useful for general usage.

        """
        self.equi_join_broadcast_limit = equi_join_broadcast_limit
        self.partitions_per_worker = partitions_per_worker
        self.single_worker_ops = single_worker_ops
        # Whether sort operations can be executed on multiple workers.
        self.sort_partitioned = kwargs.get("sort_partitioned", True)
        # Whether group-by and selected aggregations are pre-aggregated on
        # worker nodes if possible.
        self.pre_aggregation = kwargs.get("pre_aggregation", True)
        # Whether individual expressions can be lowered into distributed operations.
        self.expression_lowering = expression_lowering


def spawn_many(
    lf: list[LazyFrame],
    *,
    dst: Path | str | Dst,
    context: ClientContext | None = None,
    engine: Engine = "auto",
    plan_type: PlanTypePreference = "dot",
    labels: None | list[str] = None,
    shuffle_compression: ShuffleCompression = "auto",
    shuffle_format: ShuffleFormat = "auto",
    distributed: DistributionSettings | None | bool = None,
    n_retries: int = 0,
    min_workers: int | None = None,
    max_workers: int | None = None,
    lineage: LineageContext | None = None,
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
    min_workers : int | None
        The minimum number of workers that have to be available to start
        query execution. The cluster will wait until this many workers are
        available.
        When `min_workers=None`, it defaults to the number of workers the
        cluster is configured to have, or the current number of workers for
        dynamically sized clusters.
    max_workers : int | None
        The maximum number of workers to use for query execution.
        When `max_workers=None`, the query will use all available workers
        and any workers that join afterwards.
        It is recommended to set this to the expected number of workers for
        dynamically sized clusters, so the query planner can determine the
        correct number of data partitions.
    lineage
        OpenLineage metadata for this query, typically provided by an orchestrator.

        .. warning::
            This functionality is considered **unstable**. It may be changed
            at any point without it being considered a breaking change.
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
            min_workers=min_workers,
            max_workers=max_workers,
            distributed=distributed,
            lineage=lineage,
            **optimizations,  # type: ignore[arg-type]
        )
        for lf_ in lf
    ]


def spawn_many_blocking(
    lf: list[LazyFrame],
    *,
    dst: Path | str | Dst,
    context: ClientContext | None = None,
    engine: Engine = "auto",
    plan_type: PlanTypePreference = "dot",
    labels: None | list[str] = None,
    shuffle_compression: ShuffleCompression = "auto",
    shuffle_format: ShuffleFormat = "auto",
    distributed: DistributionSettings | None | bool = None,
    n_retries: int = 0,
    min_workers: int | None = None,
    max_workers: int | None = None,
    lineage: LineageContext | None = None,
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
    min_workers : int | None
        The minimum number of workers that have to be available to start
        query execution. The cluster will wait until this many workers are
        available.
        When `min_workers=None`, it defaults to the number of workers the
        cluster is configured to have, or the current number of workers for
        dynamically sized clusters.
    max_workers : int | None
        The maximum number of workers to use for query execution.
        When `max_workers=None`, the query will use all available workers
        and any workers that join afterwards.
        It is recommended to set this to the expected number of workers for
        dynamically sized clusters, so the query planner can determine the
        correct number of data partitions.
    lineage
        OpenLineage metadata for this query, typically provided by an orchestrator.

        .. warning::
            This functionality is considered **unstable**. It may be changed
            at any point without it being considered a breaking change.
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
            min_workers=min_workers,
            max_workers=max_workers,
            distributed=distributed,
            lineage=lineage,
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
    engine: Engine = "auto",
    plan_type: PlanTypePreference = "dot",
    labels: None | list[str] = None,
    shuffle_compression: ShuffleCompression = "auto",
    shuffle_format: ShuffleFormat = "auto",
    shuffle_compression_level: int | None = None,
    distributed: DistributionSettings | None | bool = None,
    n_retries: int = 0,
    min_workers: int | None = None,
    max_workers: int | None = None,
    sink_to_single_file: bool | None = None,
    optimizations: pl.QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS,
    lineage: LineageContext | None = None,
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
    shuffle_compression_level
        Compression level of shuffle. If set to `None` it is decided by the optimizer.
    distributed
        Run as as distributed query with these settings. This may run partially
        distributed, depending on the operation, optimizer statistics
        and available machines.
    n_retries
        How often failed tasks should be retried.
    min_workers : int | None
        The minimum number of workers that have to be available to start
        query execution. The cluster will wait until this many workers are
        available.
        When `min_workers=None`, it defaults to the number of workers the
        cluster is configured to have, or the current number of workers for
        dynamically sized clusters.
    max_workers : int | None
        The maximum number of workers to use for query execution.
        When `max_workers=None`, the query will use all available workers
        and any workers that join afterwards.
        It is recommended to set this to the expected number of workers for
        dynamically sized clusters, so the query planner can determine the
        correct number of data partitions.
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
    lineage
        OpenLineage metadata for this query, typically provided by an orchestrator.

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

    # Do not call get_status() to avoid network call
    if isinstance(context, ComputeContext):
        # The actual cluster status can change at any time
        # so this is at best an educated guess
        if (
            context._last_known_status.is_stopped()
            or context._last_known_status.is_failed()
        ):
            msg = (
                f"cannot execute query, context status is {context._last_known_status}"
            )
            raise RuntimeError(msg)

        if context._compute_id is None:
            context.start()

    plan, settings = prepare_query(
        lf=lf,
        dst=dst,
        engine=engine,
        plan_type=plan_type,
        shuffle_compression=shuffle_compression,
        shuffle_format=shuffle_format,
        shuffle_compression_level=shuffle_compression_level,
        n_retries=n_retries,
        min_workers=min_workers,
        max_workers=max_workers,
        distributed_settings=distributed,
        sink_to_single_file=sink_to_single_file,
        optimizations=optimizations,
    )

    lineage_context = (
        pcr.PyLineageContext(
            job_namespace=lineage.job_namespace,
            job_name=lineage.job_name,
            parent_run_id=lineage.parent_run_id,
            parent_job_namespace=lineage.parent_job_namespace,
            parent_job_name=lineage.parent_job_name,
        )
        if lineage
        else None
    )

    if isinstance(context, ClusterContext) or (
        isinstance(context, ComputeContext) and context.connection_mode == "direct"
    ):
        client: pcr.SchedulerClient = context._get_direct_client()  # type: ignore[assignment]
        token = get_token(context)
        try:
            username = pc_cfg.Config.get(pc_cfg._USER_NAME)
            execution_id: str | None = None
            try:
                if _get_ipython is not None:
                    ip = _get_ipython()  # type: ignore[no-untyped-call]
                    if ip is not None and hasattr(ip, "kernel"):
                        parent = ip.kernel.get_parent("shell")
                        execution_id = parent.get("header", {}).get("msg_id")
            except Exception:
                pass
            q_id = client.do_query(
                plan=plan,
                settings=settings,
                token=token,
                username=username,
                execution_id=execution_id,
                lineage_context=lineage_context,
            )
        except pcr.EncodedPolarsError as e:
            raise decode_error(str(e)) from None

        if isinstance(context, ComputeContext):
            msg = f"View your query metrics on: https://cloud.pola.rs/portal/{context.workspace.id}/{context._compute_id}/queries/{q_id}"
            logger.debug(msg)
        return DirectQuery(q_id, client, context)
    # Check if we are using the cloud compute context
    elif isinstance(context, ComputeContext):
        assert context._compute_id is not None
        q_id = constants.API_CLIENT.submit_query(
            context._compute_id, plan, settings, labels, lineage_context
        )
        msg = f"View your query metrics on: https://cloud.pola.rs/portal/{context.workspace.id}/{context._compute_id}/queries/{q_id}"
        logger.debug(msg)
        return ProxyQuery(q_id, workspace_id=context.workspace.id)
    else:
        msg = f"Invalid client type: expected ComputeContext/ClusterContext, got: {type(context).__name__}"
        raise ValueError(msg)


def spawn_blocking(
    lf: LazyFrame,
    *,
    dst: Path | str | Dst,
    context: ClientContext | None = None,
    engine: Engine = "auto",
    plan_type: PlanTypePreference = "dot",
    labels: None | list[str] = None,
    shuffle_compression: ShuffleCompression = "auto",
    distributed: DistributionSettings | None | bool = None,
    n_retries: int = 0,
    sink_to_single_file: bool | None = None,
    optimizations: pl.QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS,
    lineage: LineageContext | None = None,
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
    lineage
        OpenLineage metadata for this query, typically provided by an orchestrator.

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
        engine=engine,
        plan_type=plan_type,
        labels=labels,
        shuffle_compression=shuffle_compression,
        distributed=distributed,
        n_retries=n_retries,
        sink_to_single_file=sink_to_single_file,
        optimizations=optimizations,
        lineage=lineage,
    )
    return in_process.await_result()
