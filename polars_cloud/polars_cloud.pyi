from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from polars_cloud._typing import FileType
from polars_cloud.query.query import DistributionSettings

def serialize_query_settings(
    *,
    engine: str,
    partition_by: list[str] | None = ...,
    distributed: bool | None = ...,
    prefer_dot: bool = ...,
    shuffle_opts: PyShuffleOpts = ...,
    n_retries: int = ...,
    distributed_settings: DistributionSettings | None = ...,
) -> PyQuerySettings: ...

class PyQuerySettings:
    pass

class PyShuffleOpts:
    @staticmethod
    def new(format: str, compression: str) -> PyShuffleOpts: ...

class WorkspaceStateSchema(Enum):
    """Represents the state of a workspace."""

    Uninitialized: int
    Pending: int
    Active: int
    Failed: int
    Deleted: int

class WorkspaceSchema:
    """Represents a workspace schema."""

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

    status: WorkspaceStateSchema
    """Status of the Workspace."""

    idle_timeout_mins: int
    """The time a cluster can be idle before it will be automatically killed"""

    created_at: datetime
    """Creation timestamp."""

    updated_at: datetime
    """Last update timestamp."""

    deleted_at: datetime | None
    """Timestamp of the last deletion."""

    def __init__(self, id: UUID, name: str, status: WorkspaceStateSchema) -> None: ...

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

class QuerySchema:
    """Represents the schema for a query."""

    id: UUID
    """Query ID."""

    workspace_id: UUID
    """The workspace the query is being run in."""

    cluster_id: UUID
    """The virtual machine it is sent to."""

    user_id: UUID
    """The user account that started the instance."""

    request_time: datetime
    """The time the query was requested."""

    created_at: datetime
    """Creation timestamp."""

    updated_at: datetime
    """Last update timestamp."""

    deleted_at: datetime | None
    """Timestamp of the last deletion."""

class QueryStatusCodeSchema(Enum):
    """Represents the status codes for a query."""

    Queued: int
    Scheduled: int
    InProgress: int
    Success: int
    Failed: int
    Canceled: int

class StatusSchema:
    """Represents the status information for a query."""

    status_time: datetime
    """Start time for the status."""

    code: QueryStatusCodeSchema
    """Status code."""

class QueryWithStatusSchema:
    """Represents a query with its associated status."""

    query: QuerySchema
    """Details of the query."""

    status: StatusSchema
    """Current status of the query"""

class QueryStateTimingSchema:
    final_known_state: QueryStatusCodeSchema | None
    """Last known state for this query"""
    final_status_time: datetime | None
    """Time for the final state for this query"""
    last_known_state: QueryStatusCodeSchema
    """The last known state that this query has"""
    last_known_status_time: datetime
    """Last known status time for this query, belongs to last_known_state"""
    last_progress_time: datetime | None
    """Time for the last InProgress time"""

class QueryWithStateTimingSchema:
    query: QuerySchema
    """Details of the query."""
    state_timing: QueryStateTimingSchema
    """Details about the state of the query"""

class FileTypeSchema(Enum):
    Parquet: int
    IPC: int
    Csv: int
    NDJSON: int
    JSON: int

class ResultSchema:
    total_stages: int
    finished_stages: int
    failed_stages: int
    n_rows_result: int | None
    file_type_sink: FileTypeSchema | None
    errors: list[str]

class QueryWithStateTimingAndResultSchema:
    query: QuerySchema
    """Details of the query."""
    state_timing: QueryStateTimingSchema
    """Details about the state of the query"""
    result: ResultSchema | None

class QueryPlansSchema:
    id: UUID
    """Query ID."""
    ir_plan: str | None
    """The intermediate representation in dotfile format."""
    phys_plan: str | None
    """The physical plan in dotfile format."""

class TerminationReasonSchema(Enum):
    """Enum representing the reasons for termination."""

    StoppedByUser: int
    """The instance was stopped by the user."""

    StoppedInactive: int
    """The instance was stopped due to inactivity."""

    Failed: int
    """The instance failed."""

class TerminationSchema:
    """Represents the termination details of a compute instance."""

    termination_reason: TerminationReasonSchema
    """Reason for termination."""

    termination_time: datetime
    """Timestamp when termination occurred."""

    termination_message: str | None
    """Optional message providing details about the termination."""

class DBClusterModeSchema(Enum):
    Proxy: int
    Direct: int

class ComputeSchema:
    """Represents the schema for a compute instance."""

    id: UUID
    """Unique identifier for the compute instance."""

    user_id: UUID
    """ID of the user associated with the compute instance."""

    workspace_id: UUID
    """ID of the workspace the compute instance belongs to."""

    instance_type: str | None
    """Type of instance (e.g., instance type string)."""

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

    termination: TerminationSchema | None
    """Termination settings, if applicable."""

    gc_inactive_hours: int
    """Number of hours before garbage collection of inactive instances."""

    request_time: datetime
    """Timestamp when the compute instance was requested."""

    mode: DBClusterModeSchema
    """Mode of the database cluster."""

    polars_version: str
    """The version of polars running on the cluster."""

    created_at: datetime
    """Timestamp when the compute instance was created."""

    updated_at: datetime
    """Timestamp when the compute instance was last updated."""

    deleted_at: datetime | None
    """Timestamp when the compute instance was deleted, if applicable."""

    status: ComputeStatusSchema
    """Status of the compute instance."""

class LogLevelSchema(Enum):
    """Log level for a compute cluster."""

    Info: int
    Debug: int
    Trace: int

class ComputeClusterPublicInfoSchema:
    cluster_id: UUID
    public_address: str
    public_server_key: str

class ComputeStatusSchema(Enum):
    Starting: int
    Idle: int
    Running: int
    Stopping: int
    Stopped: int
    Failed: int

class WorkspaceWithUrlSchema:
    workspace: WorkspaceSchema
    full_url: str
    barebones_url: str

class WorkspaceSetupUrlSchema:
    full_setup_url: str
    barebones_setup_url: str
    full_template_url: str
    barebones_template_url: str

class DeleteWorkspaceSchema:
    stack_name: str
    url: str

class UserSchema:
    id: UUID
    """User id."""
    first_name: str | None
    """First name."""
    last_name: str | None
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

class QueryProgressPy:
    tag: bytes
    total_stages: int | None
    phys_plan_explain: str | None
    phys_plan_dot: str | None
    data: bytes | None

class OrganizationSchema:
    """Represents an organization schema."""

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

    status: WorkspaceStateSchema
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
    ) -> WorkspaceWithUrlSchema: ...
    def get_workspace_setup_url(
        self, workspace_id: UUID
    ) -> WorkspaceSetupUrlSchema: ...
    def delete_workspace(self, workspace_id: UUID) -> DeleteWorkspaceSchema | None: ...
    def get_workspace(self, workspace_id: UUID) -> WorkspaceSchema: ...
    def list_workspaces(
        self, name: str | None = None, organization_id: UUID | None = None
    ) -> list[WorkspaceSchema]: ...
    def get_workspace_default_compute_specs(
        self, workspace_id: UUID
    ) -> DefaultComputeSpecs | None: ...

    # Compute methods
    def get_compute_cluster(
        self, workspace_id: UUID, compute_id: UUID
    ) -> ComputeSchema: ...
    def stop_compute_cluster(self, workspace_id: UUID, compute_id: UUID) -> None: ...
    def get_compute_server_info(
        self, workspace_id: UUID, compute_id: UUID
    ) -> ComputeClusterPublicInfoSchema: ...
    def start_compute(
        self,
        workspace_id: UUID,
        cluster_size: int,
        mode: DBClusterModeSchema,
        cpus: int | None,
        ram_gb: int | None,
        instance_type: str | None,
        big_instance_type: str | None,
        big_instance_multiplier: int | None,
        storage: int | None,
        big_instance_storage: int | None,
        requirements_txt: str | None,
        labels: list[str] | None,
        log_level: LogLevelSchema | None,
        idle_timeout_mins: int | None,
    ) -> str: ...
    def get_compute_clusters(self, workspace_id: UUID) -> list[ComputeSchema]: ...

    # Organization methods
    def get_organization(self, organization_id: UUID) -> OrganizationSchema: ...
    def create_organization(self, name: str) -> OrganizationSchema: ...
    def delete_organization(self, organization_id: UUID) -> None: ...
    def get_organizations(self, name: str | None) -> list[OrganizationSchema]: ...

    # Query methods
    def get_query(
        self, workspace_id: UUID, query_id: UUID
    ) -> QueryWithStateTimingAndResultSchema: ...
    def get_query_plans(
        self, workspace_id: UUID, query_id: UUID
    ) -> QueryPlansSchema: ...
    def cancel_proxy_query(self, workspace_id: UUID, query_id: UUID) -> None: ...
    def list_queries(self, workspace_id: UUID) -> list[QueryWithStateTimingSchema]: ...

    # User methods
    def get_user(self) -> UserSchema: ...
    def get_query_result(self, query_id: UUID) -> QueryInfoPy: ...
    def submit_query(
        self,
        compute_id: UUID,
        plan: bytes,
        settings: PyQuerySettings,
        labels: list[str] | None,
    ) -> UUID:
        pass

class SchedulerClient:
    def __init__(self, compute_addr: str, client_options: ClientOptions): ...
    def cancel_direct_query(self, query_id: UUID) -> None: ...
    def get_direct_query_status(self, query_id: UUID) -> QueryStatusCodeSchema: ...
    def get_direct_query_result(self, query_id: UUID) -> QueryInfoPy: ...
    def do_query(
        self,
        plan: bytes,
        settings: PyQuerySettings,
        labels: list[str] | None = None,
    ) -> UUID: ...
    def get_direct_query_progress(
        self, query_id: UUID, tag: bytes | None
    ) -> QueryProgressPy | None: ...
    def get_direct_query_plan(
        self, query_id: UUID, phys: bool = False, ir: bool = False
    ) -> QueryPlansPy: ...

class PlanFormatPy(Enum):
    Dot: int
    Explain: int

class QueryPlansPy:
    format: PlanFormatPy
    ir_plan: str | None
    phys_plan: str | None
