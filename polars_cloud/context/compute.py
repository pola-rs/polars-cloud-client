from __future__ import annotations

import logging
import time
from contextlib import ContextDecorator
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import polars_cloud
import polars_cloud.polars_cloud as pcr
from polars_cloud import constants
from polars_cloud.context.compute_connect_select import select_compute_cluster
from polars_cloud.context.compute_specs import resolve_compute_context_specs
from polars_cloud.context.compute_status import ComputeContextStatus
from polars_cloud.exceptions import ComputeClusterMisspecified
from polars_cloud.polars_cloud import ClientOptions
from polars_cloud.workspace import Workspace, WorkspaceStatus

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import io
    import sys
    from types import TracebackType

    from polars_cloud._typing import ConnectionMode, LogLevel
    from polars_cloud.organization import Organization

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self

SCHEDULER_PORT = 5051


class ClientContext:
    """Client context in which queries can be submitted too.

    The client context is an abstraction over a low-level client that can
    submit queries to some compute destination.
    """

    def __init__(self) -> None:
        self._compute_id: UUID | None = None
        self._connection_mode: str = "direct"
        self._direct_client: pcr.SchedulerClient | None = None

    def _get_direct_client(self) -> pcr.SchedulerClient | None:
        return self._direct_client


class ClusterContext(ClientContext):
    """Cluster context in which queries are executed.

    The cluster context is an abstraction of some remote cluster that is routable
    from the client. This will bypass any calls to the Polars Cloud control plane
    and talk to the cluster's scheduler directly.
    """

    def __init__(
        self,
        compute_address: str,
        *,
        insecure: bool = False,
        tls_cert_domain: str | None = None,
        public_server_crt: bytes | None = None,
        tls_certificate: bytes | None = None,
        tls_private_key: bytes | None = None,
    ) -> None:
        self._connection_mode = "direct"
        self._compute_id = uuid4()
        client_options = ClientOptions()

        client_options.tls_cert_domain = tls_cert_domain
        client_options.public_server_crt = public_server_crt
        client_options.tls_certificate = tls_certificate
        client_options.tls_private_key = tls_private_key
        client_options.insecure = insecure
        compute_address = f"{compute_address}:{SCHEDULER_PORT}"
        self._direct_client = pcr.SchedulerClient(compute_address, client_options)


class ComputeContext(ClientContext, ContextDecorator):
    """Compute context in which queries are executed.

    The compute context is an abstraction of the underlying hardware
    (either a single node or multiple nodes in case of a cluster).

    Parameters
    ----------
    cpus
        The number of CPUs each instance in the compute context
        should have access to. This acts as a lower bound, Polars Cloud
        finds the smallest available machine that satisfies both cpu and
        memory requirements.
    memory
        The amount of RAM (in GB) each instance in the compute context
        should have access to. This acts as a lower bound, see `cpus`.
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
        workspace="workspace-name", cpus=24, memory=24, cluster_size=2, labels="docs"
    )
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
        log_level=LogLevelSchema.Info,
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

    def __init__(
        self,
        *,
        cpus: int | None = None,
        memory: int | None = None,
        instance_type: str | None = None,
        storage: int | None = None,
        cluster_size: int | None = None,
        requirements: str | Path | io.IOBase | bytes | None = None,
        connection_mode: ConnectionMode = "direct",
        workspace: str | UUID | Workspace | None = None,
        labels: list[str] | str | None = None,
        log_level: LogLevel | None = None,
        idle_timeout_mins: int | None = None,
        insecure: bool = False,
    ) -> None:
        self._workspace = Workspace._parse(workspace)

        self._specs = resolve_compute_context_specs(
            self._workspace,
            cpus=cpus,
            memory=memory,
            instance_type=instance_type,
            storage=storage,
            cluster_size=cluster_size,
        )

        if log_level is None or log_level == "info":
            log_level_schema = pcr.LogLevelSchema.Info
        elif log_level == "debug":
            log_level_schema = pcr.LogLevelSchema.Debug
        elif log_level == "trace":
            log_level_schema = pcr.LogLevelSchema.Trace
        else:
            msg = f"'log_level' should be one of {{'info', 'debug', 'trace'}}, got {log_level}"
            raise ValueError(msg)

        self._labels = [labels] if isinstance(labels, str) else labels
        self._connection_mode = connection_mode
        self._direct_client = None
        self._insecure = insecure
        self._compute_id: UUID | None = None
        self._log_level = log_level_schema
        self._idle_timeout_mins = idle_timeout_mins
        self._requirements_txt: str | None
        self._polars_version: str | None = None
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
    def _from_api_schema(
        cls, schema: pcr.ComputeSchema
    ) -> tuple[Self, ComputeContextStatus]:
        connection_mode: ConnectionMode
        if schema.mode == pcr.DBClusterModeSchema.Direct:
            connection_mode = "direct"
        else:
            connection_mode = "proxy"

        self = cls(
            cpus=schema.req_cpu_cores,
            memory=schema.req_ram_gb,
            cluster_size=schema.cluster_size,
            instance_type=schema.instance_type,
            storage=schema.req_storage,
            workspace=Workspace(id=schema.workspace_id),
            connection_mode=connection_mode,
            # TODO: Get the labels as well
            labels=None,
            # TODO: Persist the loglevel
            log_level=None,
        )
        self._compute_id = schema.id
        self._polars_version = schema.polars_version
        return self, ComputeContextStatus._from_api_schema(schema.status)

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
        else:
            status = constants.API_CLIENT.get_compute_cluster(
                self.workspace.id, self._compute_id
            ).status
            return ComputeContextStatus._from_api_schema(status)

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

        wait = True if self.connection_mode == "direct" else wait

        if self.connection_mode == "direct":
            mode = pcr.DBClusterModeSchema.Direct
        else:
            mode = pcr.DBClusterModeSchema.Proxy

        str_id = constants.API_CLIENT.start_compute(
            workspace_id=self.workspace.id,
            cluster_size=self._specs.cluster_size,
            mode=mode,
            cpus=self._specs.cpus,
            ram_gb=self._specs.memory,
            instance_type=self._specs.instance_type,
            storage=self._specs.storage,
            big_instance_type=self._specs.big_instance_type,
            big_instance_multiplier=self._specs.big_instance_multiplier,
            big_instance_storage=self._specs.big_instance_storage,
            requirements_txt=self._requirements_txt,
            labels=self._labels,
            log_level=self.log_level,
            idle_timeout_mins=self._idle_timeout_mins,
        )
        self._compute_id = UUID(str_id)
        if wait:
            _poll_compute_status_until(self, ComputeContextStatus.IDLE)

    def stop(self, *, wait: bool = False) -> None:
        """Stop the compute context.

        Parameters
        ----------
        wait
            If True, this will block this thread until context is stopped.

        Examples
        --------
        >>> ctx = pc.ComputeContext(workspace="workspace-name", cpus=24, memory=24)
        >>> ctx.stop()
        """
        if self._compute_id is None:
            msg = "nothing to stop, context is not running"
            raise RuntimeError(msg)

        constants.API_CLIENT.stop_compute_cluster(self.workspace.id, self._compute_id)
        if wait:
            _poll_compute_status_until(self, ComputeContextStatus.STOPPED, 10, 5, 100)

        self._compute_id = None
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
        return [cls._from_api_schema(c) for c in compute_contexts]

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
        ctx, status = cls._from_api_schema(details)
        if not status.is_available():
            msg = f"Context is in an incorrect state: {status}"
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
        if workspace:
            workspace = Workspace._parse(workspace)
            if workspace.status != WorkspaceStatus.Active:
                msg = "`connect your workspace with your cloud first before connecting to compute contexts.`"
                raise ComputeClusterMisspecified(msg)
            workspaces = [workspace]
        else:
            workspaces = [
                w for w in Workspace.list() if w.status == WorkspaceStatus.Active
            ]
        contexts = [
            (w, context)
            for w in workspaces
            for context in constants.API_CLIENT.get_compute_clusters(w.id)
            if ComputeContextStatus._from_api_schema(context.status).is_available()
        ]
        contexts.sort(key=lambda x: x[1].request_time, reverse=True)
        idx = select_compute_cluster(contexts)
        if idx is not None:
            ctx, status = cls._from_api_schema(contexts[idx][1])
            return ctx
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
        return self._connection_mode

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
    def log_level(self) -> pcr.LogLevelSchema | None:
        return self._log_level

    def _get_direct_client(self) -> pcr.SchedulerClient | None:
        if self.connection_mode != "direct":
            return None
        if self._direct_client is not None:
            return self._direct_client

        logger.debug("Getting compute server info")
        assert self._compute_id is not None, (
            "Compute ID undefined while getting direct client"
        )
        server_info = constants.API_CLIENT.get_compute_server_info(
            self.workspace.id, self._compute_id
        )
        logger.debug("Successfully obtained compute server info")

        client_options = ClientOptions()
        client_options.public_server_crt = str.encode(server_info.public_server_key)
        client_options.insecure = self._insecure
        compute_address = f"{server_info.public_address}:{SCHEDULER_PORT}"
        client = pcr.SchedulerClient(compute_address, client_options)
        self._direct_client = client
        return self._direct_client

    @property
    def polars_version(self) -> str | None:
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


def _poll_compute_status_until(
    compute: ComputeContext,
    desired_state: ComputeContextStatus,
    start_delay: int = 60,
    interval: int = 3,
    timeout: int = 300,
) -> ComputeContextStatus:
    """Poll the compute status until the compute context is in desired_state."""
    max_polls = int((timeout - start_delay) / interval)
    time.sleep(start_delay)
    for i in range(max_polls):
        logger.debug("Polling compute status (try %s)", str(i))
        status = compute.get_status()
        logging.debug("Got compute status %s", status)
        if status == desired_state:
            return status
        elif status == ComputeContextStatus.FAILED:
            msg = "Compute cluster in failed state"
            logger.info(msg)
            raise RuntimeError(msg)
        else:
            time.sleep(interval)
    else:
        msg = f"Compute context failed to reach desired state {desired_state} after polling for {timeout} seconds"
        logger.info(msg)
        raise RuntimeError(msg)
