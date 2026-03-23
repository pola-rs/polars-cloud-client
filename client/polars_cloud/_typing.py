"""Internal typing module.

Contains type aliases intended for private use.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

Engine: TypeAlias = Literal["auto", "streaming", "in-memory", "gpu"]
PlanTypePreference: TypeAlias = Literal["dot", "plain"]
ShuffleCompression: TypeAlias = Literal["auto", "uncompressed", "lz4", "zstd"]
ShuffleFormat: TypeAlias = Literal["auto", "ipc", "parquet"]
SingleWorkerOps: TypeAlias = Literal["auto", "allow", "forbid"]

Json: TypeAlias = dict[str, Any]
PlanType: TypeAlias = Literal["physical", "ir"]
ConnectionMode: TypeAlias = Literal["direct", "proxy"]
CPUArchitecture: TypeAlias = Literal["x86_64", "arm64"]
LogLevel: TypeAlias = Literal["info", "debug", "trace"]
FileType: TypeAlias = Literal["none", "parquet", "ipc", "csv", "ndjson", "json"]
ScalingMode: TypeAlias = Literal["auto", "single-node", "distributed"]
