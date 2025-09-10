from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import polars as pl
from polars._utils.cloud import prepare_cloud_plan
from polars.exceptions import ComputeError, InvalidOperationError

from polars_cloud.query.dst import CsvDst, IpcDst, ParquetDst, TmpDst

with contextlib.suppress(ImportError):  # Module not available when building docs
    from pathlib import Path

    import polars_cloud.polars_cloud as pc_core


if TYPE_CHECKING:
    from polars import LazyFrame, QueryOptFlags

    from polars_cloud._typing import (
        Engine,
        PlanTypePreference,
        ShuffleCompression,
        ShuffleFormat,
    )
    from polars_cloud.polars_cloud import PyQuerySettings
    from polars_cloud.query.dst import Dst
    from polars_cloud.query.query import DistributionSettings


def prepare_query(
    lf: LazyFrame,
    *,
    dst: str | Path | Dst,
    partition_by: None | str | list[str],
    engine: Engine,
    plan_type: PlanTypePreference,
    shuffle_compression: ShuffleCompression,
    shuffle_format: ShuffleFormat,
    distributed_settings: DistributionSettings | None,
    n_retries: int,
    sink_to_single_file: bool | None = None,
    optimizations: QueryOptFlags,
) -> tuple[bytes, PyQuerySettings]:
    """Parse query inputs as a serialized plan and settings object."""
    if pl.get_index_type() == pl.UInt64:
        msg = "polars[u64-idx] not supported for this client version"
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
            field_overwrites=dst.field_overwrites,
            lazy=True,
            engine=engine,
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
            engine=engine,
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
            engine=engine,
        )
    elif isinstance(dst, TmpDst):
        lf = lf.sink_parquet(
            "<in-memory>",
            lazy=True,
            engine=engine,
        )
    else:
        assert sink_dst is not None
        lf = lf.sink_parquet(
            sink_dst,
            credential_provider=None,
            lazy=True,
            engine=engine,
        )

    try:
        plan = prepare_cloud_plan(lf, optimizations=optimizations)
    except (ComputeError, InvalidOperationError) as exc:
        msg = f"invalid cloud plan: {exc}"
        raise ValueError(msg) from exc

    if plan_type == "dot":
        prefer_dot = True
    elif plan_type == "plain":
        prefer_dot = False
    else:
        msg = f"'plan_type' must be one of: {{'dot', 'plain'}}, got {plan_type!r}"
        raise ValueError(msg)

    if engine == "gpu":
        msg = "GPU mode is not yet supported, consider opening an issue"
        raise ValueError(msg)
    elif engine not in {"auto", "in-memory", "streaming"}:
        msg = f"`engine` must be one of {{'auto', 'in-memory', 'streaming', 'gpu'}}, got {engine!r}"
        raise ValueError(msg)

    shuffle_opts = pc_core.PyShuffleOpts.new(
        format=shuffle_format, compression=shuffle_compression
    )

    if isinstance(partition_by, str):
        partition_by = list(partition_by)

    settings = pc_core.serialize_query_settings(
        engine=engine,
        partition_by=partition_by,
        prefer_dot=prefer_dot,
        shuffle_opts=shuffle_opts,
        n_retries=n_retries,
        distributed_settings=distributed_settings,
    )

    return plan, settings
