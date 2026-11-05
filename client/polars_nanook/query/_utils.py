from __future__ import annotations

import contextlib
import difflib
import inspect
from typing import TYPE_CHECKING, Any, Final

import polars as pl
from polars._utils.cloud import prepare_cloud_plan
from polars.exceptions import ComputeError, InvalidOperationError

from polars_nanook.polars_nanook import PyNumWorkers
from polars_nanook.query.dst import (
    CallbackDst,
    ClientDst,
    CsvDst,
    IcebergDst,
    IpcDst,
    ParquetDst,
    TmpDst,
)

with contextlib.suppress(ImportError):  # Module not available when building docs
    from pathlib import Path

    import polars_nanook.polars_nanook as pc_core

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from polars import LazyFrame, QueryOptFlags

    from polars_nanook._typing import (
        Engine,
        ExecuteUntil,
        PlanTypePreference,
        ShuffleCompression,
        ShuffleFormat,
    )
    from polars_nanook.polars_nanook import PyQuerySettings
    from polars_nanook.query.dst import Dst
    from polars_nanook.query.query import DistributionSettings


def reject_unexpected_kwargs(
    func: Callable[..., Any], kwargs: Mapping[str, object]
) -> None:
    """Raise a `TypeError` for a keyword `func` doesn't take, with a close match."""
    if not kwargs:
        return
    name = next(iter(kwargs))
    keywords = [
        param.name
        for param in inspect.signature(func).parameters.values()
        if param.kind is inspect.Parameter.KEYWORD_ONLY
    ]
    msg = f"{func.__qualname__}() got an unexpected keyword argument {name!r}"
    if matches := difflib.get_close_matches(name, keywords, n=1):
        msg += f", did you mean {matches[0]!r}?"
    raise TypeError(msg)


LOCAL_ENGINE: Final = "streaming"
"""
Engine for reading a remote query's result back to this machine.

This is here to prevent `engine="auto"` from resolving to the remote engine (in case we
have set the affinity to `RemoteEngine` and are currently in the process of reading back
the result).
"""


def prepare_query(
    lf: LazyFrame,
    *,
    dst: str | Path | Dst,
    engine: Engine,
    plan_type: PlanTypePreference,
    shuffle_compression: ShuffleCompression,
    shuffle_format: ShuffleFormat,
    shuffle_compression_level: int | None = None,
    distributed_settings: DistributionSettings | None,
    n_retries: int,
    min_workers: int | None = None,
    max_workers: int | None = None,
    sink_to_single_file: bool | None = None,
    optimizations: QueryOptFlags,
    priority: int = 0,
    execute_until: ExecuteUntil = "execute",
) -> tuple[bytes, PyQuerySettings]:
    """Parse query inputs as a serialized plan and settings object."""
    if pl.get_index_type() == pl.UInt32:
        msg = "polars[rt32] not supported for this client version.\n\n Please run `pip install polars[rt64]`, restart the process and try again."
        raise RuntimeError(msg)

    sink_dst: str | Path | None
    if isinstance(dst, (str, Path)):
        sink_dst = dst
    elif isinstance(dst, (ParquetDst, CsvDst, IpcDst)) and isinstance(
        dst.uri, (str | Path)
    ):
        sink_dst = dst.uri
    elif isinstance(dst, TmpDst):
        sink_dst = None
    else:
        sink_dst = None

    # Verify that the sink_to_single_file is explicitly set or that the sink
    # path seems like a directory.
    seems_like_single_file = False
    if sink_dst is None:
        pass
    elif isinstance(sink_dst, str):
        last_slash = sink_dst.rfind("/") or 0
        seems_like_single_file = "." in sink_dst[last_slash:]
    elif isinstance(sink_dst, Path):
        seems_like_single_file = "." in sink_dst.name

    if (
        distributed_settings is not None
        and seems_like_single_file
        and sink_to_single_file is None
    ):
        msg = """\
Sink destination appears to be a single file, but `sink_to_single_file` is not set.

The distributed engine supports sinking to a single file, but it is discouraged \
as it will limit the amount of distributed work that can be done.

If you want to:
- sink to a directory with this name, set `sink_to_single_file=False`
- sink to a single file, set `sink_to_single_file=True`.
"""
        raise ValueError(msg)
    flight_ttl = None
    flight_maintain_order = None
    if isinstance(dst, ParquetDst):
        assert dst.uri is not None
        lf = lf.sink_parquet(
            path=dst.uri,
            compression=dst.compression,
            compression_level=dst.compression_level,
            statistics=dst.statistics,
            row_group_size=dst.row_group_size,
            data_page_size=dst.data_page_size,
            maintain_order=dst.maintain_order,
            storage_options=dst.storage_options,
            credential_provider=dst.credential_provider,
            metadata=dst.metadata,
            arrow_schema=dst.arrow_schema,
            lazy=True,
        )
    elif isinstance(dst, CsvDst):
        lf = lf.sink_csv(
            path=dst.uri,
            include_bom=dst.include_bom,
            include_header=dst.include_header,
            separator=dst.separator,
            line_terminator=dst.line_terminator,
            quote_char=dst.quote_char,
            batch_size=dst.batch_size,
            datetime_format=dst.datetime_format,
            date_format=dst.date_format,
            time_format=dst.time_format,
            float_scientific=dst.float_scientific,
            float_precision=dst.float_precision,
            null_value=dst.null_value,
            quote_style=dst.quote_style,
            maintain_order=dst.maintain_order,
            storage_options=dst.storage_options,
            credential_provider=dst.credential_provider,
            decimal_comma=dst.decimal_comma,
            lazy=True,
        )
    elif isinstance(dst, IpcDst):
        lf = lf.sink_ipc(
            path=dst.uri,
            compression=dst.compression,
            compat_level=dst.compat_level,
            maintain_order=dst.maintain_order,
            storage_options=dst.storage_options,
            credential_provider=dst.credential_provider,
            lazy=True,
        )
    elif isinstance(dst, IcebergDst):
        from polars.io.iceberg._sink import IcebergSinkState

        sink_state = IcebergSinkState.new(
            dst.target,
            mode=dst.mode,
            catalog=dst.catalog,
            storage_options=dst.storage_options,
        )
        lf = sink_state.attach_sink(lf)
    elif isinstance(dst, TmpDst):
        if hasattr(lf._ldf, "_node_name") and lf._ldf._node_name() == "SinkMultiple":
            # This is the `pl.collect_all(..., lazy=True)` branch.
            # This uses the sinks in the plan.
            pass
        else:
            lf = lf.sink_parquet(
                "<in-memory>",
                lazy=True,
            )
    elif isinstance(dst, CallbackDst):
        lf = lf.sink_batches(
            function=dst.function,
            chunk_size=dst.chunk_size,
            maintain_order=dst.maintain_order,
            lazy=True,
        )
    elif isinstance(dst, ClientDst):
        lf = lf.sink_parquet("<flight>", lazy=True)
        flight_ttl = dst.ttl
        flight_maintain_order = dst.maintain_order
    else:
        assert sink_dst is not None
        lf = lf.sink_parquet(
            sink_dst,
            credential_provider=None,
            lazy=True,
        )

    try:
        plan = prepare_cloud_plan(lf, optimizations=optimizations)

        if isinstance(plan, tuple):
            (plan, optimization_flags) = plan
        else:
            optimization_flags = None

    except (ComputeError, InvalidOperationError) as exc:
        msg = f"invalid cloud plan: {exc}"
        raise ValueError(msg) from exc

    if plan_type == "dot":
        plan_dot = True
    elif plan_type == "plain":
        plan_dot = False
    else:
        msg = f"'plan_type' must be one of: {{'dot', 'plain'}}, got {plan_type!r}"
        raise ValueError(msg)

    if distributed_settings is not None:
        if distributed_settings.single_worker_ops not in {"auto", "allow", "forbid"}:
            msg = f"`distributed_settings.single_worker_ops` must be one of {{'auto', 'allow', 'forbid'}}, got {distributed_settings.single_worker_ops!r}"
            raise ValueError(msg)

    shuffle_opts = pc_core.PyShuffleOpts.new(
        format=shuffle_format,
        compression=shuffle_compression,
        compression_level=shuffle_compression_level,
    )

    if (
        min_workers is not None
        and max_workers is not None
        and min_workers > max_workers
    ):
        msg = "min_workers > max_workers"
        raise ValueError(msg)

    py_num_workers = PyNumWorkers(min=min_workers, max=max_workers)
    settings = pc_core.serialize_query_settings(
        engine=engine,
        plan_dot=plan_dot,
        shuffle_opts=shuffle_opts,
        n_retries=n_retries,
        n_workers=py_num_workers,
        distributed_settings=distributed_settings,
        optimization_flags=optimization_flags,
        flight_ttl=flight_ttl,
        flight_maintain_order=flight_maintain_order,
        priority=priority,
        execute_until=execute_until,
    )

    return plan, settings
