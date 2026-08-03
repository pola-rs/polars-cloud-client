from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, final
from uuid import UUID

from polars_cloud._typing import (
    ConnectionMode,
    CPUArchitecture,
    FileType,
    LogLevel,
)
from polars_cloud.query.query import DistributionSettings

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "ApiClient",
    "AuthLoadError",
    "ClientOptions",
    "ComputeClusterMisspecified",
    "ComputeClusterNodeInfoModel",
    "ComputeClusterPublicInfoModel",
    "ComputeContextSpecs",
    "ComputeModel",
    "ComputeStatusModel",
    "ComputeTokenModel",
    "ComputeVersionsPy",
    "DBCPUArchitectureModel",
    "DBClusterModeModel",
    "DefaultComputeSpecs",
    "DeleteWorkspaceModel",
    "EncodedPolarsError",
    "FileTypeModel",
    "LogLevelModel",
    "ManifestModel",
    "NotFoundError",
    "OrganizationModel",
    "OrganizationSubscriptionStateModel",
    "PlanFormatPy",
    "PyLineageContext",
    "PyNumWorkers",
    "PyQuerySettings",
    "PyShuffleOpts",
    "QueryDetailPy",
    "QueryEngineModel",
    "QueryInfoPy",
    "QueryModel",
    "QueryPlanTimingPy",
    "QueryPlansPy",
    "QueryStateTimingModel",
    "QueryStatusCodeModel",
    "QueryTypeModel",
    "QueryWithStateTimingAndResultModel",
    "ResultModel",
    "SchedulerClient",
    "StageStatsPy",
    "StatusModel",
    "TLSOptions",
    "TerminationModel",
    "TerminationReasonModel",
    "UserModel",
    "WorkspaceApiToken",
    "WorkspaceApiTokenWithNameModel",
    "WorkspaceClusterDefaultsModel",
    "WorkspaceDeploymentModel",
    "WorkspaceModel",
    "WorkspaceSetupUrlModel",
    "WorkspaceStateModel",
    "WorkspaceWithUrlModel",
    "cli_main",
    "polars_version",
    "py_is_token_expired",
    "python_version",
    "resolve_compute_context_specs",
    "serialize_query_settings",
]

@final
class PyShuffleOpts:
    @staticmethod
    def new(
        compression: str, format: str, compression_level: int | None
    ) -> PyShuffleOpts: ...

@final
class PyQuerySettings:
    pass

@final
class PyNumWorkers:
    def __new__(cls, min: int | None, max: int | None) -> PyNumWorkers: ...

def serialize_query_settings(
    *,
    engine: str,
    plan_dot: bool,
    shuffle_opts: PyShuffleOpts,
    n_retries: int,
    n_workers: PyNumWorkers | None,
    distributed_settings: DistributionSettings | None,
    optimization_flags: int | None,
) -> PyQuerySettings: ...
def py_is_token_expired(
    token: str, reject_tokens_expiring_in_less_than: timedelta | None
) -> bool: ...
def polars_version() -> str: ...
def python_version() -> str: ...
def cli_main() -> None: ...

@final
class ComputeTokenModel:
    id: UUID
    """Compute id"""

    token: str
    """Compute Token"""

@final
class WorkspaceStateModel(Enum):
    """Represents the state of a workspace."""

    Uninitialized = 0
    Pending = 1
    Active = 2
    Failed = 3
    Deleted = 4

@final
class WorkspaceDeploymentModel(Enum):
    """Represents the deployment location of a workspace."""

    Aws = 0
    OnPrem = 1

@final
class WorkspaceModel:
    """Represents a workspace model."""

    id: UUID
    """Workspace ID (UUID v7)."""

    organization_id: UUID
    """Organization ID (UUID v7)."""

    name: str
    """Workspace Name."""

    description: str
    """Workspace Description."""

    deployment: WorkspaceDeploymentModel
    """Which location the workspace is deployed in."""

    creator_id: UUID
    """User who owns the Workspace."""

    status: WorkspaceStateModel
    """Status of the Workspace."""

    cloud_resources_url: str | None
    """Url to deployed resources for this workspace. For AWS this is a direct link to
    the cloudformation stack"""

    idle_timeout_mins: int
    """The time a cluster can be idle before it will be automatically killed"""

    created_at: datetime
    """Creation timestamp."""

    updated_at: datetime
    """Last update timestamp."""

    deleted_at: datetime | None
    """Timestamp of the last deletion."""

@final
class ComputeClusterNodeInfoModel:
    """Represents a single node within a compute cluster."""

    cluster_id: UUID
    private_address: str | None
    cpus: int | None
    memory_mb: int | None
    storage_mb: int | None

@final
class DefaultComputeSpecs:
    """Represents the default compute specifications."""

    instance_type: str | None
    """The type of instance (e.g., t3.micro)."""

    cpus: int | None
    """Number of CPUs."""

    ram_gb: int | None
    """Amount of RAM in GiB."""

    storage: int | None
    """Amount of disk storage in GiB."""

    cluster_size: int
    """Number of compute nodes."""

@final
class WorkspaceClusterDefaultsModel:
    """Represents the default cluster specifications for a workspace."""

    instance_specs: Any
    """Instance specifications."""

    storage: int | None
    """Amount of disk storage in GiB."""

    cluster_size: int
    """Number of compute nodes."""

@final
class QueryModel:
    """Represents the model for a query."""

    id: UUID
    """Query ID."""

    workspace_id: UUID
    """The workspace the query is being run in."""

    cluster_id: UUID
    """The virtual machine it is sent to."""

    user_id: UUID | None
    """The user account that started the query."""

    request_time: datetime
    """The time the query was requested."""

    output_location: str | None
    """The output location for the query."""

    query_type: QueryTypeModel | None
    """The query type (single or distributed)."""

    engine: QueryEngineModel | None
    """The engine used for the query."""

    status_code: QueryStatusCodeModel
    """The status code of the query."""

    status_updated_at: datetime
    """The time the query was last updated."""

    started_at: datetime | None
    """The time the query was started at."""

    ended_at: datetime | None
    """The time the query reached a done state."""

    created_at: datetime
    """Creation timestamp."""

    updated_at: datetime
    """Last update timestamp."""

    deleted_at: datetime | None
    """Timestamp of the last deletion."""

@final
class QueryStatusCodeModel(Enum):
    """Represents the status codes for a query."""

    Queued = 0
    Scheduled = 1
    InProgress = 2
    Success = 3
    Failed = 4
    Canceled = 5

@final
class QueryTypeModel(Enum):
    Single = 0
    Distributed = 1

@final
class QueryEngineModel(Enum):
    InMemory = 0
    Streaming = 1

@final
class FileTypeModel(Enum):
    Parquet = 0
    IPC = 1
    Csv = 2
    NDJSON = 3
    JSON = 4

@final
class ResultModel:
    total_stages: int
    finished_stages: int
    failed_stages: int
    n_rows_result: int | None
    file_type_sink: FileTypeModel | None
    errors: list[str]

@final
class StatusModel:
    status_time: datetime
    """Start time for the status."""
    code: QueryStatusCodeModel
    """Status code."""

@final
class QueryStateTimingModel:
    final_known_state: QueryStatusCodeModel | None
    final_status_time: datetime | None
    last_known_state: QueryStatusCodeModel
    last_known_status_time: datetime
    last_progress_time: datetime | None
    latest_status: QueryStatusCodeModel
    """Latest state for this query."""
    latest_status_time: datetime
    """Latest state transition time for this query."""

@final
class QueryWithStateTimingAndResultModel:
    query: QueryModel
    """Details about the state of the query"""
    state_timing: QueryStateTimingModel
    result: ResultModel | None

@final
class TerminationReasonModel(Enum):
    """Enum representing the reasons for termination."""

    StoppedByUser = 0
    """The instance was stopped by the user."""

    StoppedInactive = 1
    """The instance was stopped due to inactivity."""

    Failed = 2
    """The instance failed."""

@final
class TerminationModel:
    """Represents the termination details of a compute instance."""

    termination_reason: TerminationReasonModel
    """Reason for termination."""

    termination_time: datetime
    """Timestamp when termination occurred."""

    termination_message: str | None
    """Optional message providing details about the termination."""

@final
class DBClusterModeModel(Enum):
    """Mode of the database cluster."""

    @staticmethod
    def from_str(s: ConnectionMode | None) -> DBClusterModeModel: ...
    def as_str(self) -> ConnectionMode: ...

    Proxy = 0
    Direct = 1

@final
class DBCPUArchitectureModel(Enum):
    """CPU Architecture."""

    @staticmethod
    def from_str(s: CPUArchitecture | None) -> DBCPUArchitectureModel: ...
    def as_str(self) -> CPUArchitecture: ...

    X86_64 = 0
    Arm64 = 1

@final
class ManifestModel:
    """Represents the model for a compute cluster manifest."""

    id: UUID
    """Unique identifier for the manifest."""

    workspace_id: UUID
    """ID of the workspace the manifest belongs to."""

    name: str
    """Name of the manifest, unique within a workspace."""

    instance_type: str | None
    """Type of instance (e.g., instance type string)."""

    req_ram_gb: int | None
    """Requested RAM in GiB."""

    req_cpu_cores: int | None
    """Requested number of CPU cores."""

    cpu_architectures: list[DBCPUArchitectureModel] | None
    """Requested cpu_architectures for the compute cluster."""

    req_storage: int | None
    """Requested disk storage in GiB."""

    big_instance_type: str | None
    """Type of the optional big worker instance (e.g., instance type string)."""

    req_big_instance_multiplier: int | None
    """Requested big worker multiplier."""

    req_big_instance_storage: int | None
    """Requested big worker disk storage in GiB."""

    cluster_size: int
    """Number of compute nodes in the cluster."""

    mode: DBClusterModeModel
    """Mode of the database cluster."""

    idle_timeout_mins: int | None
    """How many minutes a cluster can be idle before it will be automatically killed."""

    log_level: LogLevelModel
    """Log level of the compute cluster."""

    polars_version: str
    """Version of polars the manifest was created with."""

    python_version: str
    """Version of python the manifest was created with."""

    requirements_txt: str | None
    """Requirements.txt file contents."""

    env_vars: dict[str, str]
    """Environment variable overrides"""

    live_cluster_id: UUID | None
    """"ID of the cluster for this manifest if one is active"""

@final
class ComputeModel:
    """Represents the model for a compute cluster."""

    id: UUID
    """Unique identifier for the compute cluster."""

    user_id: UUID
    """ID of the user associated with the compute cluster."""

    workspace_id: UUID
    """ID of the workspace the compute cluster belongs to."""

    name: str | None
    """Name of the compute cluster, unique within a workspace."""

    instance_type: str | None
    """Type of instance (e.g., instance type string)."""

    cpu_architectures: list[DBCPUArchitectureModel] | None
    """Requested cpu_architectures for the compute cluster."""

    req_ram_gb: int | None
    """Requested RAM in GiB."""

    ram_mib: int | None
    """Actual RAM in MiB."""

    req_cpu_cores: int | None
    """Requested number of CPU cores."""

    vcpus: int | None
    """Actual number of CPU cores."""

    req_storage: int | None
    """Requested disk storage in GiB."""

    big_instance_type: str | None
    """Type of the optional big worker instance (e.g., instance type string)."""

    req_big_instance_multiplier: int | None
    """Requested big worker multiplier."""

    req_big_instance_storage: int | None
    """Requested big worker disk storage in GiB."""

    cluster_size: int
    """Number of compute nodes in the cluster."""

    termination: TerminationModel | None
    """Termination settings, if applicable."""

    gc_inactive_hours: int
    """Number of hours before garbage collection of inactive instances."""

    request_time: datetime
    """Timestamp when the compute cluster was requested."""

    mode: DBClusterModeModel
    """Mode of the database cluster."""

    log_level: LogLevelModel
    """Log level of the compute cluster."""

    polars_version: str
    """The version of polars running on the cluster."""

    compute_plane_version: str | None
    """The version of the compute cluster."""

    tunnel_addr: str | None
    """Address for the compute cluster tunnel."""

    created_at: datetime
    """Timestamp when the compute cluster was created."""

    updated_at: datetime
    """Timestamp when the compute cluster was last updated."""

    deleted_at: datetime | None
    """Timestamp when the compute cluster was deleted, if applicable."""

    status: ComputeStatusModel
    """Status of the compute cluster."""

@final
class LogLevelModel(Enum):
    """Log level for a compute cluster."""

    @staticmethod
    def from_str(s: LogLevel | None) -> LogLevelModel: ...
    def as_str(self) -> LogLevel: ...

    Info = 0
    Debug = 1
    Trace = 2

@final
class ComputeClusterPublicInfoModel:
    cluster_id: UUID
    public_address: str
    public_server_key: str

@final
class ComputeStatusModel(Enum):
    Starting = 0
    Idle = 1
    Running = 2
    Stopping = 3
    Stopped = 4
    Failed = 5

@final
class WorkspaceWithUrlModel:
    workspace: WorkspaceModel
    full_url: str
    barebones_url: str

@final
class WorkspaceSetupUrlModel:
    full_setup_url: str
    barebones_setup_url: str
    full_template_url: str
    barebones_template_url: str

@final
class WorkspaceApiTokenWithNameModel:
    id: UUID
    name: str
    workspace_id: UUID
    description: str | None
    created_at: datetime

@final
class WorkspaceApiToken:
    id: UUID
    username: str
    api_secret: str
    workspace_id: UUID
    description: str | None
    created_at: datetime

@final
class DeleteWorkspaceModel:
    stack_name: str
    url: str

@final
class UserModel:
    id: UUID
    """User id."""
    first_name: str | None
    """First name."""
    last_name: str | None
    """Email."""
    email: str | None
    """Last name."""
    avatar_url: str | None
    """Avatar url."""
    default_workspace_id: UUID | None
    """The default workspace id (if None specified)."""
    newsletter_updates: bool
    """Whether to receive newsletter updates."""
    personal_emails: bool
    """Whether to receive personal updates."""

class NotFoundError(Exception):
    """Exception raised when a resource is not found."""

class AuthLoadError(Exception):
    """Exception raised when no authentication could be loaded."""

class EncodedPolarsError(Exception):
    """Polars Error raised by the compute plane."""

class ComputeClusterMisspecified(Exception):
    """Exception raised when the cluster settings were misspecified."""

@final
class StageStatsPy:
    num_workers_used: int

@final
class QueryInfoPy:
    total_stages: int
    finished_stages: int
    failed_stages: int
    head: bytes | None
    n_rows_result: int | None
    errors: list[str]
    sink_dst: list[str]
    file_type_sink: FileType
    ir_plan_explain: str | None
    ir_plan_dot: str | None
    phys_plan_explain: str | None
    phys_plan_dot: str | None
    stages_stats: Any | None

@final
class TLSOptions:
    ca_cert: bytes | None
    insecure: bool
    def __new__(
        cls, *, ca_cert: bytes | None = None, insecure: bool = False
    ) -> TLSOptions: ...

@final
class ClientOptions:
    uri: str
    domain_name: str | None
    extra_headers: Mapping[str, str] | None
    tls_options: TLSOptions | None
    def __new__(
        cls,
        uri: str,
        *,
        domain_name: str | None = None,
        extra_headers: Mapping[str, str] | None = None,
        tls_options: TLSOptions | None = None,
    ) -> ClientOptions: ...

@final
class QueryPlanTimingPy:
    plan_start_time: str | None
    parse_start_time: str | None
    optimize_start_time: str | None
    distribute_start_time: str | None
    distribute_end_time: str | None
    plan_end_time: str | None

@final
class QueryDetailPy:
    id: str
    user_name: str
    status: str
    request_time: str
    start_time: str | None
    end_time: str | None
    query_plan_timing: QueryPlanTimingPy
    total_num_stages: int | None
    failed_stage: int | None
    in_progress_stages: list[int]
    finished_stages: list[int]
    total_bytes_shuffled: int | None
    total_node_time_ns: int
    percentage_time_shuffling: float | None
    total_num_files: int | None
    original_num_files: int | None
    total_rows_read: int | None
    errors: list[str] | None
    engine: str
    query_type: str
    output_location: str | None
    output_files: int | None
    output_rows: int | None

@final
class OrganizationModel:
    """Represents an organization model."""

    id: UUID
    """Organization ID (UUID v7)."""

    name: str
    """Organization Name."""

    description: str
    """Organization Description."""

    avatar_url: str | None
    """Organization avatar."""

    creator_id: UUID
    """User who owns the Organization."""

    subscription_state: OrganizationSubscriptionStateModel
    """Subscription state of the Organization."""

    trial_started_at: datetime | None
    """Timestamp the trial started, if applicable."""

    trial_expires_at: datetime | None
    """Timestamp the trial expires, if applicable."""

    created_at: datetime
    """Creation timestamp."""

    updated_at: datetime
    """Last update timestamp."""

    deleted_at: datetime | None
    """Timestamp of the last deletion."""

@final
class OrganizationSubscriptionStateModel(Enum):
    """Subscription state of an organization."""

    PreTrial = 0
    Trial = 1
    TrialExpired = 2
    Subscribing = 3
    Subscribed = 4
    Unsubscribed = 5

@final
class PyLineageContext:
    def __new__(
        cls,
        job_namespace: str,
        job_name: str,
        parent_run_id: UUID | None = None,
        parent_job_namespace: str | None = None,
        parent_job_name: str | None = None,
    ) -> PyLineageContext: ...

@final
class ApiClient:
    def authenticate(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        interactive: bool = True,
    ) -> None: ...
    def login(self) -> None: ...
    def clear_authentication(self) -> None: ...
    def get_auth_header(self) -> str: ...

    # Workspace methods
    def create_workspace(
        self, name: str, organization_id: UUID
    ) -> WorkspaceWithUrlModel: ...
    def get_workspace_setup_url(self, workspace_id: UUID) -> WorkspaceSetupUrlModel: ...
    def delete_workspace(self, workspace_id: UUID) -> DeleteWorkspaceModel | None: ...

    # On-prem workspace methods
    def create_on_prem_workspace(
        self, name: str, organization_id: UUID
    ) -> WorkspaceModel: ...
    def delete_on_prem_workspace(self, workspace_id: UUID) -> None: ...
    def get_workspace(self, workspace_id: UUID) -> WorkspaceModel: ...
    def get_workspaces(
        self, name: str | None = None, organization_id: UUID | None = None
    ) -> list[WorkspaceModel]: ...
    def set_workspace_cluster_defaults(
        self,
        workspace_id: UUID,
        instance_type: str | None,
        cpus: int | None,
        ram_gb: int | None,
        storage: int | None,
        cpu_architectures: list[DBCPUArchitectureModel] | None,
        cluster_size: int,
    ) -> None: ...
    def get_workspace_default_compute_specs(
        self, workspace_id: UUID
    ) -> DefaultComputeSpecs | None: ...
    def get_workspace_cluster_defaults(
        self, workspace_id: UUID
    ) -> WorkspaceClusterDefaultsModel | None: ...

    # Compute methods
    def get_compute_cluster(
        self, workspace_id: UUID, compute_id: UUID
    ) -> ComputeModel: ...
    def get_compute_cluster_manifest(
        self, workspace_id: UUID, manifest_name: str
    ) -> ManifestModel: ...
    def stop_compute_cluster(self, workspace_id: UUID, compute_id: UUID) -> None: ...
    def get_compute_server_info(
        self, workspace_id: UUID, compute_id: UUID
    ) -> ComputeClusterPublicInfoModel: ...
    def register_compute_cluster_manifest(
        self,
        workspace_id: UUID,
        name: str,
        cluster_size: int,
        mode: DBClusterModeModel,
        cpus: int | None,
        ram_gb: int | None,
        cpu_architectures: list[DBCPUArchitectureModel] | None,
        instance_type: str | None,
        storage: int | None,
        big_instance_type: str | None,
        big_instance_multiplier: int | None,
        big_instance_storage: int | None,
        requirements_txt: str | None,
        env_vars: dict[str, str],
        labels: list[str] | None,
        log_level: LogLevelModel,
        idle_timeout_mins: int | None,
    ) -> ManifestModel: ...
    def unregister_compute_cluster_manifest(
        self,
        workspace_id: UUID,
        name: str,
    ) -> None: ...
    def start_compute_cluster_manifest(
        self, workspace_id: UUID, name: str
    ) -> ComputeModel: ...
    def start_compute(
        self,
        workspace_id: UUID,
        cluster_size: int,
        mode: DBClusterModeModel,
        cpus: int | None,
        ram_gb: int | None,
        cpu_architectures: list[DBCPUArchitectureModel] | None,
        instance_type: str | None,
        storage: int | None,
        big_instance_type: str | None,
        big_instance_multiplier: int | None,
        big_instance_storage: int | None,
        requirements_txt: str | None,
        env_vars: dict[str, str],
        labels: list[str] | None,
        log_level: LogLevelModel | None,
        idle_timeout_mins: int | None,
    ) -> ComputeModel: ...
    def get_compute_clusters(
        self, workspace_id: UUID, *, status: list[ComputeStatusModel] | None = None
    ) -> list[ComputeModel]: ...
    def get_compute_cluster_token(
        self, workspace_id: UUID, compute_id: UUID
    ) -> ComputeTokenModel: ...
    def get_compute_cluster_nodes(
        self, workspace_id: UUID, compute_id: UUID
    ) -> list[ComputeClusterNodeInfoModel]: ...

    # Organization methods
    def get_organization(self, organization_id: UUID) -> OrganizationModel: ...
    def create_organization(self, name: str) -> OrganizationModel: ...
    def delete_organization(self, organization_id: UUID) -> None: ...
    def get_organizations(self, name: str | None) -> list[OrganizationModel]: ...

    # Query methods
    def get_query(
        self, workspace_id: UUID, query_id: UUID
    ) -> QueryWithStateTimingAndResultModel: ...
    def cancel_proxy_query(self, workspace_id: UUID, query_id: UUID) -> None: ...
    def get_queries(self, workspace_id: UUID) -> list[QueryModel]: ...

    # User methods
    def get_user(self) -> UserModel: ...
    def get_query_result(self, query_id: UUID) -> QueryInfoPy: ...
    def submit_query(
        self,
        compute_id: UUID,
        plan: bytes,
        settings: PyQuerySettings,
        labels: list[str] | None,
        lineage_context: PyLineageContext | None,
    ) -> UUID: ...
    def get_service_accounts(
        self, workspace_id: UUID
    ) -> list[WorkspaceApiTokenWithNameModel]: ...
    def create_service_account(
        self, workspace_id: UUID, name: str, description: str | None
    ) -> WorkspaceApiToken: ...
    def delete_service_account(self, workspace_id: UUID, user_id: UUID) -> None: ...

@final
class QueryPlansPy:
    format: PlanFormatPy
    ir_plan: str | None
    phys_plan: str | None

@final
class ComputeVersionsPy:
    compute_plane_version: str
    polars_python_version: str
    polars_rust_revision: str

@final
class SchedulerClient:
    def __new__(
        cls,
        scheduler: ClientOptions,
        observatory: ClientOptions,
    ) -> SchedulerClient: ...
    def cancel_direct_query(self, query_id: UUID, token: str | None) -> None: ...
    def delete_direct_query_result(self, query_id: UUID, token: str | None) -> None: ...
    def get_direct_query_status(
        self, query_id: UUID, token: str | None
    ) -> QueryStatusCodeModel: ...
    def get_direct_query_result(
        self, query_id: UUID, token: str | None
    ) -> QueryInfoPy: ...
    def do_query(
        self,
        plan: bytes,
        settings: PyQuerySettings,
        token: str | None,
        username: str | None = None,
        labels: list[str] | None = None,
        execution_id: str | None = None,
        lineage_context: PyLineageContext | None = None,
    ) -> UUID: ...
    def get_direct_query_plan(
        self, query_id: UUID, token: str | None, phys: bool = False, ir: bool = False
    ) -> QueryPlansPy: ...
    def get_compute_versions(self, token: str | None) -> ComputeVersionsPy: ...
    def get_query_details(self, query_id: UUID, token: str | None) -> QueryDetailPy: ...

@final
class PlanFormatPy(Enum):
    Dot = 0
    Explain = 1

@final
class ComputeContextSpecs:
    def __new__(
        cls,
        *,
        cpus: int | None = None,
        memory: int | None = None,
        cpu_architectures: list[DBCPUArchitectureModel] | None = None,
        instance_type: str | None = None,
        big_instance_type: str | None = None,
        big_instance_multiplier: int | None = None,
        storage: int | None = None,
        big_instance_storage: int | None = None,
        cluster_size: int,
    ) -> ComputeContextSpecs: ...
    cpus: int | None
    memory: int | None
    cpu_architectures: list[DBCPUArchitectureModel] | None
    instance_type: str | None
    big_instance_type: str | None
    big_instance_multiplier: int | None
    storage: int | None
    big_instance_storage: int | None
    cluster_size: int

def resolve_compute_context_specs(
    workspace_id: UUID,
    cpus: int | None = None,
    memory: int | None = None,
    cpu_architectures: list[DBCPUArchitectureModel] | None = None,
    instance_type: str | None = None,
    storage: int | None = None,
    big_instance_type: str | None = None,
    big_instance_multiplier: int | None = None,
    big_instance_storage: int | None = None,
    cluster_size: int | None = None,
) -> ComputeContextSpecs: ...
