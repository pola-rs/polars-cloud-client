"""Polars Cloud client.

Enables users to interact with workspaces, clusters, and queries on Polars Cloud.
"""

import atexit

from polars_cloud import exceptions
from polars_cloud._version import __version__
from polars_cloud.auth import authenticate, login
from polars_cloud.config import Config
from polars_cloud.context import (
    ClientContext,
    ClusterContext,
    ComputeContext,
    ComputeContextStatus,
    set_compute_context,
    show_versions,
)
from polars_cloud.organization import (
    Organization,
)
from polars_cloud.polars_cloud import (
    ClientOptions,
    LogLevelModel,
    QueryCloudObserver,
    TLSOptions,
    cli_main,
    flush_traces,
)
from polars_cloud.query import (
    CsvDst,
    DirectQuery,
    ExecuteRemote,
    IpcDst,
    LazyFrameRemote,
    ParquetDst,
    ProxyQuery,
    QueryDetail,
    QueryInfo,
    QueryPlanTiming,
    QueryResult,
    QueryStatus,
    StageStatistics,
    spawn,
    spawn_blocking,
    spawn_many,
    spawn_many_blocking,
)
from polars_cloud.workspace import (
    ProviderType,
    Workspace,
    WorkspaceDefaultComputeSpecs,
    WorkspaceProviderAWS,
    WorkspaceStatus,
)

__all__ = [
    "ClientContext",
    "ClientOptions",
    "ClusterContext",
    "ComputeContext",
    "ComputeContextStatus",
    "Config",
    "CsvDst",
    "DirectQuery",
    "ExecuteRemote",
    "IpcDst",
    "LazyFrameRemote",
    "LogLevelModel",
    "Organization",
    "ParquetDst",
    "ProviderType",
    "ProxyQuery",
    "QueryCloudObserver",
    "QueryDetail",
    "QueryInfo",
    "QueryPlanTiming",
    "QueryResult",
    "QueryStatus",
    "StageStatistics",
    "TLSOptions",
    "Workspace",
    "WorkspaceDefaultComputeSpecs",
    "WorkspaceProviderAWS",
    "WorkspaceStatus",
    "__version__",
    "authenticate",
    "cli_main",
    "exceptions",
    "login",
    "set_compute_context",
    "show_versions",
    "spawn",
    "spawn_blocking",
    "spawn_many",
    "spawn_many_blocking",
]

atexit.register(flush_traces)
