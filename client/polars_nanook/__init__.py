"""Polars Nanook client.

Enables users to interact with workspaces, clusters, and queries on Polars Nanook.
"""

import atexit
import importlib.metadata
import sys
import warnings

from polars_nanook import exceptions
from polars_nanook._version import __version__
from polars_nanook.auth import authenticate, login
from polars_nanook.config import Config
from polars_nanook.context import (
    ClientContext,
    ClusterContext,
    ComputeContext,
    ComputeContextStatus,
    set_compute_context,
    show_versions,
)
from polars_nanook.monitoring import QueryCloudObserver
from polars_nanook.organization import (
    Organization,
)
from polars_nanook.polars_nanook import (
    LEGACY_ENV_VARS,
    ClientOptions,
    LogLevelModel,
    TLSOptions,
    cli_main,
    flush_traces,
)
from polars_nanook.query import (
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
from polars_nanook.workspace import (
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

for _message in LEGACY_ENV_VARS:
    warnings.warn(_message, FutureWarning, stacklevel=2)

try:
    importlib.metadata.distribution("polars-cloud")
except importlib.metadata.PackageNotFoundError:
    pass
else:
    # `import polars_cloud` warns about the import itself
    if "polars_cloud" not in sys.modules:
        warnings.warn(
            "`polars-cloud` is renamed to `polars-nanook` and will stop receiving updates soon. "
            "It is still installed in this environment: uninstall it and replace your "
            "`polars-cloud` requirement with the latest `polars-nanook`",
            FutureWarning,
            stacklevel=2,
        )
