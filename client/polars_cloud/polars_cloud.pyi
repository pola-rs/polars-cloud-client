from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from uuid import UUID

from polars_cloud._typing import ConnectionMode, CPUArchitecture, FileType, LogLevel
from polars_cloud.query.query import DistributionSettings

def serialize_query_settings(
    *,
    engine: str,
    distributed: bool | None = ...,
    prefer_dot: bool = ...,
    shuffle_opts: PyShuffleOpts = ...,
    n_retries: int = ...,
    distributed_settings: DistributionSettings | None = ...,
    optimization_flags: int | None,
) -> PyQuerySettings: ...
def py_is_token_expired(
    token: str, reject_tokens_expiring_in_less_than: timedelta | None
) -> bool: ...
def polars_version() -> str: ...
def python_version() -> str: ...
def cli_main() -> None: ...

class PyQuerySettings:
    pass

class ComputeTokenModel:
    id: UUID
    """Compute id"""

    token: str
    """Compute Token"""

class PyShuffleOpts:
    @staticmethod
    def new(
        format: str, compression: str, compression_level: int | None
    ) -> PyShuffleOpts: ...

class WorkspaceStateModel(Enum):
    """Represents the state of a workspace."""

    Uninitialized: int
    Pending: int
    Active: int
    Failed: int
    Deleted: int

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

    def __init__(self, id: UUID, name: str, status: WorkspaceStateModel) -> None: ...

class ComputeClusterNodeInfoModel:
    """Represents a single node within a compute cluster."""

    cluster_id: UUID
    private_address: str | None
    cpus: int | None
    memory_mb: int | None
    storage_mb: int | None

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

class QueryModel:
    """Represents the model for a query."""

    id: UUID
    """Query ID."""

    workspace_id: UUID
    """The workspace the query is being run in."""

    cluster_id: UUID
    """The virtual machine it is sent to."""

    user_id: UUID
    """The user account that started the query."""

    request_time: datetime
    """The time the query was requested."""

    created_at: datetime
    """Creation timestamp."""

    updated_at: datetime
    """Last update timestamp."""

    deleted_at: datetime | None
    """Timestamp of the last deletion."""

class QueryStatusCodeModel(Enum):
    """Represents the status codes for a query."""

    Queued: int
    Scheduled: int
    InProgress: int
    Success: int
    Failed: int
    Canceled: int

class StatusModel:
    """Represents the status information for a query."""

    status_time: datetime
    """Start time for the status."""

    code: QueryStatusCodeModel
    """Status code."""

class QueryWithStatusModel:
    """Represents a query with its associated status."""

    query: QueryModel
    """Details of the query."""

    status: StatusModel
    """Current status of the query"""

class QueryStateTimingModel:
    latest_status: QueryStatusCodeModel
    """Last known status for query"""
    started_at: datetime | None
    """When this query last changed to in_progress"""
    ended_at: datetime | None
    """When this query reached a done state (failed, canceled, success)"""

class QueryWithStateTimingModel:
    query: QueryModel
    """Details of the query."""
    state_timing: QueryStateTimingModel
    """Details about the state of the query"""

class FileTypeModel(Enum):
    Parquet: int
    IPC: int
    Csv: int
    NDJSON: int
    JSON: int

class ResultModel:
    total_stages: int
    finished_stages: int
    failed_stages: int
    n_rows_result: int | None
    file_type_sink: FileTypeModel | None
    errors: list[str]

class QueryWithStateTimingAndResultModel:
    query: QueryModel
    """Details of the query."""
    state_timing: QueryStateTimingModel
    """Details about the state of the query"""
    result: ResultModel | None

class QueryPlansModel:
    id: UUID
    """Query ID."""
    ir_plan: str | None
    """The intermediate representation in dotfile format."""
    phys_plan: str | None
    """The physical plan in dotfile format."""

class TerminationReasonModel(Enum):
    """Enum representing the reasons for termination."""

    StoppedByUser: int
    """The instance was stopped by the user."""

    StoppedInactive: int
    """The instance was stopped due to inactivity."""

    Failed: int
    """The instance failed."""

class TerminationModel:
    """Represents the termination details of a compute instance."""

    termination_reason: TerminationReasonModel
    """Reason for termination."""

    termination_time: datetime
    """Timestamp when termination occurred."""

    termination_message: str | None
    """Optional message providing details about the termination."""

class DBClusterModeModel(Enum):
    """Mode of the database cluster."""

    @staticmethod
    def from_str(s: ConnectionMode | None) -> DBClusterModeModel: ...
    def as_str(self) -> ConnectionMode: ...

    Proxy: int
    Direct: int

class DBCPUArchitectureModel(Enum):
    """CPU Architecture."""

    @staticmethod
    def from_str(s: CPUArchitecture | None) -> DBCPUArchitectureModel: ...
    def as_str(self) -> CPUArchitecture: ...

    X86_64: int
    Arm64: int

class ManifestModel:
    """Represents the model for a compute cluster manifest."""

    id: UUID
    """Unique identifier for the manifest."""

    name: str
    """Name of the manifest, unique within a workspace."""

    instance_type: str | None
    """Type of instance (e.g., instance type string)."""

    req_ram_gb: int | None
    """Requested RAM in GiB."""

    ram_mib: int | None
    """Actual RAM in MiB."""

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

    live_cluster_id: UUID | None
    """"ID of the cluster for this manifest if one is active"""

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

class LogLevelModel(Enum):
    """Log level for a compute cluster."""

    @staticmethod
    def from_str(s: LogLevel | None) -> LogLevelModel: ...
    def as_str(self) -> LogLevel: ...

    Info: int
    Debug: int
    Trace: int

class ComputeClusterPublicInfoModel:
    cluster_id: UUID
    public_address: str
    public_server_key: str

class ComputeStatusModel(Enum):
    Starting: int
    Idle: int
    Running: int
    Stopping: int
    Stopped: int
    Failed: int

class WorkspaceWithUrlModel:
    workspace: WorkspaceModel
    full_url: str
    barebones_url: str

class WorkspaceSetupUrlModel:
    full_setup_url: str
    barebones_setup_url: str
    full_template_url: str
    barebones_template_url: str

class WorkspaceApiTokenWithNameModel:
    id: UUID
    name: str
    workspace_id: UUID
    description: str | None
    created_at: datetime

class WorkspaceApiToken:
    id: UUID
    username: str
    api_secret: str
    workspace_id: UUID
    description: str | None
    created_at: datetime

class DeleteWorkspaceModel:
    stack_name: str
    url: str

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

class StageStatsPy:
    num_workers_used: int

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

class ClientOptions:
    tls_cert_domain: str | None
    public_server_crt: bytes | None
    tls_certificate: bytes | None
    tls_private_key: bytes | None
    insecure: bool

class QueryProfilePy:
    tag: bytes
    total_stages: int | None
    phys_plan_explain: str | None
    phys_plan_dot: str | None
    data: bytes | None
    errors: list[str]

class QueryPlanTimingPy:
    plan_start_time: str | None
    parse_start_time: str | None
    optimize_start_time: str | None
    distribute_start_time: str | None
    distribute_end_time: str | None
    plan_end_time: str | None

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

class OrganizationModel:
    """Represents an organization model."""

    id: UUID
    """Organization ID (UUID v7)."""

    name: str
    """Organization Name."""

    description: str
    """Organization Description."""

    avatar_url: str
    """Organization avatar."""

    creator_id: UUID
    """User who owns the Organization."""

    status: WorkspaceStateModel
    """Status of the Workspace."""

    created_at: datetime
    """Creation timestamp."""

    updated_at: datetime
    """Last update timestamp."""

    deleted_at: datetime | None
    """Timestamp of the last deletion."""

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
        big_instance_type: str | None,
        big_instance_multiplier: int | None,
        storage: int | None,
        big_instance_storage: int | None,
        requirements_txt: str | None,
        labels: list[str] | None,
        log_level: LogLevelModel | None,
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
        big_instance_type: str | None,
        big_instance_multiplier: int | None,
        storage: int | None,
        big_instance_storage: int | None,
        requirements_txt: str | None,
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
    def get_queries(self, workspace_id: UUID) -> list[QueryWithStateTimingModel]: ...

    # User methods
    def get_user(self) -> UserModel: ...
    def get_query_result(self, query_id: UUID) -> QueryInfoPy: ...
    def submit_query(
        self,
        compute_id: UUID,
        plan: bytes,
        settings: PyQuerySettings,
        labels: list[str] | None,
    ) -> UUID:
        pass

    def get_service_accounts(
        self, workspace_id: UUID
    ) -> list[WorkspaceApiTokenWithNameModel]: ...
    def create_service_account(
        self, workspace_id: UUID, name: str, description: str | None
    ) -> WorkspaceApiToken: ...
    def delete_service_account(self, workspace_id: UUID, user_id: UUID) -> None: ...

class SchedulerClient:
    def __init__(
        self,
        address: str,
        grpc_port: int,
        observatory_port: int,
        client_options: ClientOptions,
    ): ...
    def cancel_direct_query(self, query_id: UUID, token: str | None) -> None: ...
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
    ) -> UUID: ...
    def get_direct_query_profile(
        self, query_id: UUID, tag: bytes | None, token: str | None
    ) -> QueryProfilePy | None: ...
    def get_direct_query_plan(
        self, query_id: UUID, token: str | None, phys: bool = False, ir: bool = False
    ) -> QueryPlansPy: ...
    def get_compute_versions(self, token: str | None) -> ComputeVersionsPy: ...
    def get_query_details(self, query_id: UUID, token: str | None) -> QueryDetailPy: ...

class PlanFormatPy(Enum):
    Dot: int
    Explain: int

class QueryPlansPy:
    format: PlanFormatPy
    ir_plan: str | None
    phys_plan: str | None

class ComputeVersionsPy:
    compute_plane_version: str
    polars_python_version: str
    polars_rust_revision: str

class ComputeContextSpecs:
    def __init__(
        self,
        *,
        cpus: int | None = None,
        memory: int | None = None,
        cpu_architectures: list[DBCPUArchitectureModel] | None = None,
        instance_type: str | None = None,
        big_instance_type: str | None = None,
        big_instance_multiplier: int | None = None,
        storage: int | None = None,
        big_instance_storage: int | None = None,
        cluster_size: int = ...,
    ) -> None: ...
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
