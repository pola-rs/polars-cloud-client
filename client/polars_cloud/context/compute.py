from __future__ import annotations

import datetime
import logging
import time
import warnings
from contextlib import ContextDecorator
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse
from uuid import UUID, uuid4

import polars_cloud
import polars_cloud.polars_cloud as pcr
from polars_cloud import constants
from polars_cloud._tracing import traced
from polars_cloud.context.compute_connect_select import select_compute_cluster
from polars_cloud.context.compute_status import ComputeContextStatus
from polars_cloud.polars_cloud import (
    ClientOptions,
    ComputeClusterMisspecified,
    TLSOptions,
)
from polars_cloud.workspace import Workspace

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import io
    import sys
    from collections.abc import Mapping
    from types import TracebackType
    from urllib.parse import ParseResult, SplitResult

    from polars_cloud._typing import ConnectionMode, CPUArchitecture, LogLevel
    from polars_cloud.organization import Organization

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self

_deprecation_sentinel = object()

DEFAULT_COMPUTE_CLUSTER_DOMAIN_NAME = "pola.rs"
DEFAULT_SCHEDULER_PORT = 5051
DEFAULT_OBSERVATORY_PORT = 3001


class ClientContext:
    """Client context in which queries can be submitted too.

    The client context is an abstraction over a low-level client that can
    submit queries to some compute destination.
    """

    def __init__(self) -> None:
        self._compute_id: UUID | None = None
        self._connection_mode = pcr.DBClusterModeModel.Direct
        self._direct_client: pcr.SchedulerClient | None = None

    def _get_direct_client(self) -> pcr.SchedulerClient | None:
        return self._direct_client

    def _get_token(self) -> str | None:
        return None

    def show_versions(self) -> None:
        """Print the versions of the compute plane components.

        Raises
        ------
        RuntimeError
            If the compute context is not accessible or not in direct connection mode.

        Examples
        --------
        >>> ctx = pc.ClusterContext(uri="http://localhost")
        >>> ctx.show_versions()
        Compute Plane Version: 0.1.0
        Polars Python Version: 1.38.1
        Polars Rust Revision: c1764bcd298d3d2cfab4629eb0a5c36be412786e
        """
        client = self._get_direct_client()
        if client is None:
            msg = "Cannot show versions, no direct client available"
            raise RuntimeError(msg)

        versions = client.get_compute_versions(token=self._get_token())
        print(f"Compute Plane Version: {versions.compute_plane_version}")
        print(f"Polars Python Version: {versions.polars_python_version}")
        print(f"Polars Rust Revision: {versions.polars_rust_revision}")


class ClusterContext(ClientContext):
    """Cluster context in which queries are executed.

    The cluster context is an abstraction of some remote cluster that is routable
    from the client. This will bypass any calls to the Polars Cloud control plane
    and talk to the cluster's scheduler directly.
    Will attempt to establish an gRPC connection on __init__.

    Parameters
    ----------
    uri
        URI of the scheduler reachable from the client, e.g. ``https://[HOSTNAME|IP]:5051``.
        If no port is specified, defaults to `5051`.
    domain_name
        Used as authority, host header (inc port) and for TLS verification.
        Make sure to use an [IP] in the uri when using this option.
        https://developer.mozilla.org/en-US/docs/Glossary/Domain_name
    extra_headers
        Additional HTTP headers to include with each request to the cluster.
        The following headers are forbidden and will raise an error if provided:
        ``host``, ``content-length``, ``transfer-encoding``.
    tls_options
        TLS options for the connection, including custom CA certificates
        and insecure mode. If not set, uses system defaults.
    observatory
        Custom client options for the observatory. If not set, observatory is
        automatically configured from `uri` using the default port `3001`.
        When specified, none of the other options will be copied by default.

    Examples
    --------
    >>> import polars as pl
    >>> import polars_cloud as pc

    >>> lf = pl.LazyFrame({"x": list(range(100))})

    >>> # Simple config, observatory will be automatically configured.
    >>> ctx = pc.ClusterContext(uri="https://[HOSTNAME|IP]")
    >>> lf.remote(ctx).execute()

    >>> # Config with custom CA, will be used for both scheduler and observatory.
    >>> with open("certificate.pem", "rb") as cert:
    ...     custom_ca = cert.read()
    >>> ctx = pc.ClusterContext(
    ...     uri="https://[HOSTNAME|IP]",
    ...     tls_options=TLSOptions(
    ...         ca_cert=custom_ca,  # Will also be used for the observatory.
    ...     ),
    ... )
    >>> lf.remote(ctx).execute()

    >>> # Custom config for observatory. Observatory will use default tls_options.
    >>> ctx = pc.ClusterContext(
    ...     uri="https://[IP]:5051",
    ...     domain_name="pola.rs",
    ...     tls_options=TLSOptions(
    ...         insecure=True,  # Will NOT be used for the observatory.
    ...     ),
    ...     observatory=ClientOptions(uri="https://[IP]:3001", authority="obs.pola.rs"),
    ... )
    >>> lf.remote(ctx).execute()
    """

    def __init__(
        self,
        uri: str,
        *,
        domain_name: str | None = None,
        extra_headers: Mapping[str, str] | None = None,
        tls_options: TLSOptions | None = None,
        observatory: ClientOptions | None = None,
    ) -> None:
        self._connection_mode = pcr.DBClusterModeModel.Direct
        self._compute_id = uuid4()

        scheduler_uri = urlparse(uri)
        if not scheduler_uri.port:
            scheduler_uri = scheduler_uri._replace(
                netloc=self._format_host(scheduler_uri, DEFAULT_SCHEDULER_PORT)
            )

        scheduler_options = ClientOptions(
            uri=scheduler_uri.geturl(),
            domain_name=domain_name,
            extra_headers=extra_headers,
            tls_options=tls_options,
        )

        observatory_options = observatory or ClientOptions(
            uri=scheduler_uri._replace(
                netloc=self._format_host(scheduler_uri, DEFAULT_OBSERVATORY_PORT)
            ).geturl(),
            domain_name=domain_name,
            extra_headers=extra_headers,
            tls_options=tls_options,
        )

        self._direct_client = pcr.SchedulerClient(
            scheduler=scheduler_options,
            observatory=observatory_options,
        )

    @staticmethod
    def _format_host(parsed: SplitResult | ParseResult, port: int) -> str:
        host = parsed.hostname or ""
        return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


class ComputeContext(ClientContext, ContextDecorator):
    """Compute context in which queries are executed.

    The compute context is an abstraction of the underlying hardware
    (either a single node or multiple nodes in case of a cluster).

    Parameters
    ----------
    name
        The name of the registered ComputeContext to start, or connect to
        if already running.
    cpus
        The number of CPUs each instance in the compute context
        should have access to. This acts as a lower bound, Polars Cloud
        finds the smallest available machine that satisfies both cpu and
        memory requirements.
    memory
        The amount of RAM (in GB) each instance in the compute context
        should have access to. This acts as a lower bound, see `cpus`.
    cpu_architectures
        The cpu architectures to consider. Defaults currently to x86 for compatibility
        reasons. In the future include arm64 as well in the default list.
    instance_type
        The instance type to use.
    storage
        The amount of disk space (in GB) each instance in the compute context
        should have access to.
    cluster_size
        The number of machines to spin up in the cluster. Defaults to `1`.
        Includes the optional big worker instance.
    requirements
        Path to a file or a file-like object [#filelike]_ containing dependencies to
        install in the compute context, in the `requirements.txt format`_.
    env_vars
        `dict` of environment variables to override while running Polars queries.
    connection_mode
        How the context will connect to the compute cluster.
        - direct: connect directly to the compute cluster.
        - proxy: send queries to the compute cluster via the control plane.
        Defaults to `direct`.
    workspace
        The workspace to run this context in.
        You may specify the name (str), the id (UUID) or the Workspace object.
        If you're in multiple organizations that have a workspace with the same name
        then you need to explicitly specify the organization.
    labels
        Labels of the workspace (will be implicitly created)
    log_level : {'info', 'debug', 'trace'}
        Override the log level of the context for debug purposes.
    idle_timeout_mins
        How many minutes a cluster can be idle before it will be automatically killed.
        The minimum is 10 minutes, by default it is set to 1 hour.

    Examples
    --------
    >>> ctx = pc.ComputeContext(
    ...     workspace="workspace-name",
    ...     cpus=24,
    ...     memory=24,
    ...     cluster_size=2,
    ...     labels="docs",
    ... )
    >>> ctx
    ComputeContext(
        id=None,
        cpus=24,
        memory=24,
        instance_type=None,
        storage=None,
        cluster_size=2,
        mode="direct",
        workspace_name="workspace-name",
        labels=["docs"],
        log_level=LogLevelModel.Info,
    )
    >>> ctx.register("compute-name")
    >>> pc.ComputeContext(workspace="workspace-name", name="compute-name")
    ComputeContext(
        name="cluster-name",
        workspace_name="workspace-name",
        id=None,
        cpus=24,
        memory=24,
        instance_type=None,
        storage=None,
        cluster_size=2,
        mode="direct",
        labels=["docs"],
        log_level=LogLevelModel.Info,
    )

    The ComputeContext can also be used as a decorator or context manager,
    which will set it as the default context within its scope,
    automatically starting and stopping the underlying compute: ::

        @pc.ComputeContext(workspace="workspace-name", cpus=96, memory=256)
        def run_queries():
            ...
            query1.remote().execute()
            query2.remote().execute()


        # or

        with pc.ComputeContext(workspace="workspace-name", cpus=96, memory=256):
            ...
            query1.remote().execute()
            query2.remote().execute()

    Notes
    -----
    .. note::
     If the `cpus`, `memory`, and `instance_type` parameters are not set, the parameters
     are resolved with the default context specs of the workspace.

    .. rubric:: Footnotes
    .. [#filelike] By “file-like object” we refer to objects that have a read() method,
                   such as a file handler like the builtin open function, or a BytesIO
                   instance.
    .. _requirements.txt format: https://pip.pypa.io/en/stable/reference/requirements-file-format/

    """

    @traced
    def __init__(
        self,
        *,
        name: str | None = None,
        cpus: int | None = None,
        memory: int | None = None,
        cpu_architectures: list[CPUArchitecture] | None = None,
        instance_type: str | None = None,
        storage: int | None = None,
        cluster_size: int | None = None,
        requirements: str | Path | io.IOBase | bytes | None = None,
        env_vars: dict[str, str] | None = None,
        connection_mode: ConnectionMode | None = None,
        workspace: str | UUID | Workspace | None = None,
        labels: list[str] | str | None = None,
        log_level: LogLevel | None = None,
        idle_timeout_mins: int | None = None,
        insecure: bool = False,
    ) -> None:
        self._workspace = Workspace._parse(workspace)
        self._insecure = insecure
        self._direct_client = None
        self._compute_id: UUID | None = None
        self._compute_token: str | None = None
        self._requirements_txt: str | None
        self._env_vars: dict[str, str]
        self._name: str | None = None
        self._last_known_status: ComputeContextStatus

        if name is not None:
            if (
                cpus is not None
                or memory is not None
                or cpu_architectures is not None
                or instance_type is not None
                or storage is not None
                or cluster_size is not None
                or requirements is not None
                or connection_mode is not None
                or labels is not None
                or log_level is not None
                or idle_timeout_mins is not None
                or env_vars is not None
            ):
                msg = "cannot specify both `name` and any other specs"
                raise ComputeClusterMisspecified(msg)
            self._name = name
            m = constants.API_CLIENT.get_compute_cluster_manifest(
                self._workspace.id, name
            )
            self._specs = pcr.ComputeContextSpecs(
                cpus=m.req_cpu_cores,
                memory=m.req_ram_gb,
                cpu_architectures=m.cpu_architectures,
                instance_type=m.instance_type,
                storage=m.req_storage,
                big_instance_type=m.big_instance_type,
                big_instance_multiplier=m.req_big_instance_multiplier,
                cluster_size=m.cluster_size,
            )

            if m.live_cluster_id is None:
                self._last_known_status = ComputeContextStatus.UNINITIALIZED
            else:
                cluster = constants.API_CLIENT.get_compute_cluster(
                    self.workspace.id, m.live_cluster_id
                )
                self._compute_id = cluster.id
                self._last_known_status = ComputeContextStatus._from_api_model(
                    cluster.status
                )

            # TODO: Get the labels as well
            self._labels = None
            self._connection_mode: pcr.DBClusterModeModel = m.mode
            self._log_level: pcr.LogLevelModel = m.log_level
            self._idle_timeout_mins = m.idle_timeout_mins
            self._env_vars = m.env_vars

            self._polars_version = pcr.polars_version()
            if self._polars_version != m.polars_version:
                msg = f"locally installed polars version ({self._polars_version}) does not match polars version specified in manifest ({m.polars_version})"
                raise ComputeClusterMisspecified(msg)

            python_version = pcr.python_version()
            if python_version != m.python_version:
                msg = f"locally installed python version ({python_version}) does not match python version specified in manifest ({m.python_version})"
                raise ComputeClusterMisspecified(msg)

        else:
            self._specs = pcr.resolve_compute_context_specs(
                self._workspace.id,
                cpus=cpus,
                memory=memory,
                cpu_architectures=[
                    pcr.DBCPUArchitectureModel.from_str(a) for a in cpu_architectures
                ]
                if cpu_architectures
                else None,
                instance_type=instance_type,
                storage=storage,
                cluster_size=cluster_size,
            )

            self._labels = [labels] if isinstance(labels, str) else labels
            self._connection_mode = pcr.DBClusterModeModel.from_str(connection_mode)
            self._log_level = pcr.LogLevelModel.from_str(log_level)
            self._idle_timeout_mins = idle_timeout_mins
            self._polars_version = pcr.polars_version()
            self._last_known_status = ComputeContextStatus.UNINITIALIZED
            self._env_vars = env_vars or {}

            if requirements is not None:
                if isinstance(requirements, (str, Path)):
                    self._requirements_txt = Path(requirements).read_text()
                else:
                    if isinstance(requirements, bytes):
                        bytes_ = requirements
                    else:
                        bytes_ = requirements.read()
                    if isinstance(bytes_, str):
                        self._requirements_txt = bytes_
                    else:
                        self._requirements_txt = str(bytes_, encoding="utf-8")
            else:
                self._requirements_txt = None

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"id={self._compute_id}, "
            f"cpus={self._specs.cpus!r}, "
            f"memory={self._specs.memory!r}, "
            f"cpu_architectures={self._specs.cpu_architectures!r}, "
            f"instance_type={self._specs.instance_type!r}, "
            f"storage={self._specs.storage!r}, "
            f"cluster_size={self._specs.cluster_size!r}, "
            f"connection_mode={self.connection_mode!r}, "
            f"insecure={self._insecure!r}, "
            f"workspace_name={self.workspace.name!r}, "
            f"labels={self.labels!r}, "
            f"log_level={self._log_level!r}"
        )

    @classmethod
    def _from_api_model(cls, model: pcr.ComputeModel) -> Self:
        self = cls(
            cpus=model.req_cpu_cores,
            memory=model.req_ram_gb,
            cpu_architectures=[a.as_str() for a in model.cpu_architectures]
            if model.cpu_architectures
            else None,
            cluster_size=model.cluster_size,
            instance_type=model.instance_type,
            storage=model.req_storage,
            workspace=Workspace(id=model.workspace_id),
            connection_mode=model.mode.as_str(),
            # TODO: Get the labels as well
            labels=None,
            log_level=model.log_level.as_str(),
        )
        self._compute_id = model.id
        self._polars_version = model.polars_version
        self._last_known_status = ComputeContextStatus._from_api_model(model.status)
        return self

    def get_status(self) -> ComputeContextStatus:
        """Get the status of the compute context.

        Examples
        --------
        >>> ctx = pc.ComputeContext(workspace="workspace-name", cpus=24, memory=24)
        >>> ctx.get_status()
        UNINITIALIZED
        """
        if self._compute_id is None:
            return ComputeContextStatus.UNINITIALIZED

        # When unnamed the _last_known_state is only correct about terminal states.
        # When named it might have been started in the meantime, but we don't recheck
        if not self._last_known_status.is_terminal():
            status = constants.API_CLIENT.get_compute_cluster(
                self.workspace.id, self._compute_id
            ).status
            self._last_known_status = ComputeContextStatus._from_api_model(status)

        return self._last_known_status

    def register(self, name: str) -> None:
        """Register the compute cluster specs under the given name.

        This does not start the compute cluster, instead allows the cluster to be
        started with this name in the future.

        Parameters
        ----------
        name
            Name to register the compute cluster under.

        Examples
        --------
        >>> ctx = pc.ComputeContext(workspace="workspace-name", cpus=24, memory=24)
        >>> ctx.register("my-cluster")
        >>> ctx.start()
        >>> ctx = pc.ComputeContext(workspace="workspace-name", name="my-cluster")
        >>> ctx.start()
        """
        self._name = name

        constants.API_CLIENT.register_compute_cluster_manifest(
            workspace_id=self.workspace.id,
            name=self._name,
            cluster_size=self._specs.cluster_size,
            mode=self._connection_mode,
            cpus=self._specs.cpus,
            ram_gb=self._specs.memory,
            cpu_architectures=self._specs.cpu_architectures,
            instance_type=self._specs.instance_type,
            storage=self._specs.storage,
            big_instance_type=self._specs.big_instance_type,
            big_instance_multiplier=self._specs.big_instance_multiplier,
            big_instance_storage=self._specs.big_instance_storage,
            requirements_txt=self._requirements_txt,
            labels=self._labels,
            log_level=self.log_level,
            idle_timeout_mins=self._idle_timeout_mins,
            env_vars=self._env_vars,
        )

    def unregister(self) -> None:
        if self._name is None:
            msg = "can't unregister ComputeContext without name"
            raise RuntimeError(msg)

        constants.API_CLIENT.unregister_compute_cluster_manifest(
            workspace_id=self.workspace.id, name=self._name
        )

        self._name = None

    @traced
    def start(self, *, wait: bool = False) -> None:
        """Start the compute context.

        This boots up the underlying node(s) of the compute context.

        Parameters
        ----------
        wait
            Wait for the compute context to be ready before returning.
            If the `ComputeContext` is in direct connection mode, it will always
            wait until ready.

        Examples
        --------
        >>> ctx = pc.ComputeContext(workspace="workspace-name", cpus=24, memory=24)
        >>> ctx.start()
        """
        status = self.get_status()
        if status.is_failed():
            msg = "can't start compute in a failed state"
            raise RuntimeError(msg)
        elif status.is_available():
            return

        if self._name is not None:
            compute = constants.API_CLIENT.start_compute_cluster_manifest(
                workspace_id=self.workspace.id, name=self._name
            )
            self._compute_id = compute.id
        else:
            compute = constants.API_CLIENT.start_compute(
                workspace_id=self.workspace.id,
                cluster_size=self._specs.cluster_size,
                mode=self._connection_mode,
                cpus=self._specs.cpus,
                ram_gb=self._specs.memory,
                cpu_architectures=self._specs.cpu_architectures,
                instance_type=self._specs.instance_type,
                storage=self._specs.storage,
                big_instance_type=self._specs.big_instance_type,
                big_instance_multiplier=self._specs.big_instance_multiplier,
                big_instance_storage=self._specs.big_instance_storage,
                requirements_txt=self._requirements_txt,
                labels=self._labels,
                log_level=self.log_level,
                idle_timeout_mins=self._idle_timeout_mins,
                env_vars=self._env_vars,
            )
            self._compute_id = compute.id

        self._last_known_status = ComputeContextStatus.STARTING

        msg = f"View your compute metrics on: https://cloud.pola.rs/portal/{self.organization.id}/{self.workspace.id}/compute/{self._compute_id}"
        logger.info(msg)

        wait = True if self._connection_mode == pcr.DBClusterModeModel.Direct else wait
        if wait:
            _poll_compute_status_until_started(self)

    @traced
    def stop(self, *, wait: Any = _deprecation_sentinel) -> None:
        """Stop the compute context.

        Parameters
        ----------
        wait
            Deprecated. This parameter no longer affects behavior.

        Examples
        --------
        >>> ctx = pc.ComputeContext(workspace="workspace-name", cpus=24, memory=24)
        >>> ctx.stop()
        """
        if wait is not _deprecation_sentinel:
            warnings.warn(
                "The 'wait' parameter is deprecated and will be removed in a future version. "
                "Please remove it from your function call.",
                DeprecationWarning,
                stacklevel=2,
            )

        if self._compute_id is None:
            msg = "nothing to stop, context is not running"
            raise RuntimeError(msg)

        if self._last_known_status.is_stopped() or self._last_known_status.is_failed():
            logger.warning(
                "The context was already in a stopped state: Status=%s",
                self._last_known_status,
            )
            return

        constants.API_CLIENT.stop_compute_cluster(self.workspace.id, self._compute_id)
        self._last_known_status = ComputeContextStatus.STOPPED
        self._direct_client = None

    @classmethod
    def list(
        cls, workspace: Workspace | UUID | str
    ) -> list[tuple[Self, ComputeContextStatus]]:
        """List all compute contexts in the workspace and the current status for each.

        Parameters
        ----------
        workspace
            Name or ID of the workspace the compute context lives in

        Examples
        --------
        >>> pc.ComputeContext.list(workspace="YourWorkspace")
        [(ComputeContext(...), ComputeContextStatus)],
        [(ComputeContext(...), ComputeContextStatus)],
        [(ComputeContext(...), ComputeContextStatus)],
        """
        w = Workspace._parse(workspace)
        compute_contexts = constants.API_CLIENT.get_compute_clusters(w.id)

        result = []
        for c in compute_contexts:
            ctx = cls._from_api_model(c)
            result.append((ctx, ctx._last_known_status))
        return result

    @classmethod
    def connect(
        cls,
        compute_id: str | UUID,
        workspace: str | UUID | Workspace | None = None,
    ) -> Self:
        """Reconnect with an already running compute context by id.

        Parameters
        ----------
        workspace
            The workspace in which the compute context lives
        compute_id
            The unique identifier of the existing compute context.

        Examples
        --------
        >>> ctx = pc.ComputeContext.connect(
        ...     workspace="WorkspaceName",
        ...     compute_id="xxxxxxxx-1860-7521-829d-40444726cbca",
        ... )
        """
        compute_uuid = compute_id if isinstance(compute_id, UUID) else UUID(compute_id)
        workspace = Workspace._parse(workspace)
        details = constants.API_CLIENT.get_compute_cluster(workspace.id, compute_uuid)
        ctx = cls._from_api_model(details)
        if not ctx._last_known_status.is_available():
            msg = f"Context is in an incorrect state: {ctx._last_known_status}"
            raise RuntimeError(msg)
        return ctx

    @classmethod
    def select(
        cls,
        workspace: str | UUID | Workspace | None = None,
    ) -> Self | None:
        """Connect to existing compute context interactively.

        Parameters
        ----------
        workspace
            The workspace in which the compute context lives

        Examples
        --------
        >>> ctx = pc.ComputeContext.select(
        ...     workspace="WorkspaceName",
        ... )
        """
        workspaces = [Workspace._parse(workspace)] if workspace else Workspace.list()
        contexts = [
            (w, context)
            for w in workspaces
            for context in constants.API_CLIENT.get_compute_clusters(
                w.id,
                status=[
                    pcr.ComputeStatusModel.Starting,
                    pcr.ComputeStatusModel.Idle,
                    pcr.ComputeStatusModel.Running,
                ],
            )
        ]
        contexts.sort(key=lambda x: x[1].request_time, reverse=True)
        idx = select_compute_cluster(contexts)
        if idx is not None:
            return cls._from_api_model(contexts[idx][1])
        return None

    @property
    def cpus(self) -> int | None:
        """The number of CPUs each instance has access to."""
        return self._specs.cpus

    @property
    def memory(self) -> int | None:
        """The amount of RAM (in GB) each instance has access to."""
        return self._specs.memory

    @property
    def cpu_architectures(self) -> list[CPUArchitecture] | None:  # type: ignore[valid-type]
        """The cpu_architectures of the compute context."""
        return (
            [a.as_str() for a in self._specs.cpu_architectures]
            if self._specs.cpu_architectures
            else None
        )

    @property
    def instance_type(self) -> str | None:
        """The instance type of the compute context."""
        return self._specs.instance_type

    @property
    def storage(self) -> int | None:
        """The amount of disk space (in GB) each instance has access to."""
        return self._specs.storage

    @property
    def cluster_size(self) -> int:
        """The number of compute nodes in the context."""
        return self._specs.cluster_size

    @property
    def connection_mode(self) -> str:
        """In what way to connect to the compute cluster."""
        return self._connection_mode.as_str()

    @property
    def labels(self) -> list[str] | None:  # type: ignore[valid-type]
        """The labels of the compute context."""
        return self._labels

    @property
    def organization(self) -> Organization:
        """The organization to run the compute context in."""
        return self._workspace.organization

    @property
    def workspace(self) -> Workspace:
        """The workspace to run the compute context in."""
        return self._workspace

    @property
    def log_level(self) -> pcr.LogLevelModel:
        return self._log_level

    @property
    def env_vars(self) -> dict[str, str]:
        return self._env_vars

    def _get_direct_client(self) -> pcr.SchedulerClient | None:
        if self.connection_mode != "direct":
            return None
        if self._direct_client is not None:
            return self._direct_client

        logger.debug("Getting compute server info")
        assert self._compute_id is not None, (
            "Compute id undefined while getting direct client"
        )
        server_info = constants.API_CLIENT.get_compute_server_info(
            self.workspace.id, self._compute_id
        )
        logger.debug("Successfully obtained compute server info")

        scheme = "https" if not self._insecure else "http"
        tls_options = TLSOptions(ca_cert=str.encode(server_info.public_server_key))

        scheduler_options = ClientOptions(
            uri=f"{scheme}://{server_info.public_address}:{DEFAULT_SCHEDULER_PORT}",
            domain_name=DEFAULT_COMPUTE_CLUSTER_DOMAIN_NAME,
            tls_options=tls_options if not self._insecure else None,
        )
        observatory_options = ClientOptions(
            uri=f"{scheme}://{server_info.public_address}:{DEFAULT_OBSERVATORY_PORT}",
            domain_name=DEFAULT_COMPUTE_CLUSTER_DOMAIN_NAME,
            tls_options=tls_options if not self._insecure else None,
        )

        self._direct_client = pcr.SchedulerClient(
            scheduler=scheduler_options,
            observatory=observatory_options,
        )

        return self._direct_client

    def _get_token(self) -> str | None:
        assert self._compute_id is not None, "Compute id undefined while getting token"

        if self.connection_mode != "direct":
            return None

        if self._direct_client is None:
            self._get_direct_client()

        if self._compute_token is None or pcr.py_is_token_expired(
            self._compute_token, datetime.timedelta(minutes=5)
        ):
            response = constants.API_CLIENT.get_compute_cluster_token(
                self.workspace.id, self._compute_id
            )
            self._compute_token = response.token

        return self._compute_token

    @property
    def polars_version(self) -> str:
        return self._polars_version

    def __enter__(self) -> ComputeContext:
        self._cm = polars_cloud.set_compute_context(self)
        self._cm.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException],
        exc_value: BaseException,
        traceback: TracebackType,
    ) -> bool | None:
        self.stop()
        return self._cm.__exit__(exc_type, exc_value, traceback)


def show_versions(
    context: ComputeContext | ClientContext | None = None,
) -> None:
    """Print the versions of the compute plane components.

    Parameters
    ----------
    context
        The compute context to query. If not provided, uses the current
        cached context set via `set_compute_context`.

    Raises
    ------
    RuntimeError
        If no context is provided and no cached context exists,
        or if the context is not started or not in direct connection mode.

    Examples
    --------
    >>> ctx = pc.ComputeContext(workspace="workspace-name", cpus=24, memory=24)
    >>> ctx.start()
    >>> pc.set_compute_context(ctx)
    >>> pc.show_versions()
    Compute Plane Version: 0.1.0
    Polars Python Version: 1.38.1
    Polars Rust Revision: c1764bcd298d3d2cfab4629eb0a5c36be412786e
    """
    from polars_cloud.context import cache as compute_cache

    if context is None:
        context = compute_cache.cached_context

    if context is None:
        msg = "No compute context provided and no cached context available"
        raise RuntimeError(msg)

    context.show_versions()


@traced
def _poll_compute_status_until_started(
    compute: ComputeContext,
    start_delay: int = 30,
    interval: int = 3,
) -> ComputeContextStatus:
    """Poll the compute status until the compute context is in desired_state."""
    # Poll at least once for a fast response
    start = time.time()
    next_deadline = time.time() + start_delay
    assert compute._compute_id is not None
    while True:
        logger.debug("Polling compute status (%d seconds elapsed)", time.time() - start)
        compute_model = constants.API_CLIENT.get_compute_cluster(
            compute.workspace.id, compute._compute_id
        )
        status = ComputeContextStatus._from_api_model(compute_model.status)

        logger.debug("Got compute status %s", status)
        if status.is_started():
            return status
        elif status.is_terminal():
            msg = "Compute cluster in terminal state"
            if (
                compute_model.termination
                and compute_model.termination.termination_message
            ):
                msg += f" ({compute_model.termination.termination_message})"
            logger.info(msg)
            raise RuntimeError(msg)
        else:
            # Behavior is like tokio::time::Interval with MissedTickBehavior::Delay
            sleep_time = max(next_deadline - time.time(), 0.0)
            time.sleep(sleep_time)
            next_deadline = time.time() + interval
