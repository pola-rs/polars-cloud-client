"""Internal typing module.

Contains type aliases intended for private use.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from typing import TypeAlias

Engine: TypeAlias = Literal["auto", "streaming", "in-memory", "gpu"]
PlanTypePreference: TypeAlias = Literal["dot", "plain"]
ShuffleCompression: TypeAlias = Literal["auto", "uncompressed", "lz4", "zstd"]
ShuffleFormat: TypeAlias = Literal["auto", "ipc", "parquet"]
SingleWorkerOps: TypeAlias = Literal["auto", "allow", "forbid"]
Planner: TypeAlias = Literal["auto", "naive", "miso"]

Json: TypeAlias = dict[str, Any]
PlanType: TypeAlias = Literal["physical", "ir"]
# The plan stages double as the points the cluster can stop at. `PlanType` is
# the same set polars spells `PlanStage` in `show_graph(plan_stage=...)`.
ExecuteUntil: TypeAlias = Literal["execute", "physical", "ir"]
ConnectionMode: TypeAlias = Literal["direct", "proxy"]
CPUArchitecture: TypeAlias = Literal["x86_64", "arm64"]
LogLevel: TypeAlias = Literal["info", "debug", "trace"]
FileType: TypeAlias = Literal[
    "none", "unknown", "parquet", "ipc", "csv", "ndjson", "json"
]
ScalingMode: TypeAlias = Literal["auto", "single-node", "distributed"]
