use std::ops::Deref;

use chrono::{DateTime, Utc};
use deprecation_macro::deprecated_since_client;
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DefaultSortDirection, EntityOrdering, csv_vec_opt};

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all, eq, eq_int))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum QueryStatusCodeModel {
    Queued,
    Scheduled,
    InProgress,
    Success,
    Failed,
    Canceled,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct QueryModel {
    /// Query ID
    pub id: Uuid,
    /// The workspace the query is being run in
    pub workspace_id: Uuid,
    /// The cluster its run on, if non-local
    pub cluster_id: Option<Uuid>,
    /// The user account that started the query
    pub user_id: Option<Uuid>,
    /// The time the query was requested
    pub request_time: DateTime<Utc>,
    /// The output location for the query
    pub output_location: Option<String>,
    /// The query type (single or distributed)
    pub query_type: Option<QueryTypeModel>,
    /// The engine used for the query
    pub engine: Option<QueryEngineModel>,
    /// The status of the query
    pub status_code: QueryStatusCodeModel,
    /// The time the status code was updated
    pub status_updated_at: DateTime<Utc>,
    /// The time the query was started at
    pub started_at: Option<DateTime<Utc>>,
    /// The time the query reached a done state
    pub ended_at: Option<DateTime<Utc>>,
    /// The time the query was created
    pub created_at: DateTime<Utc>,
    /// The time the query was last updated
    pub updated_at: DateTime<Utc>,
    /// The time the query was deleted
    pub deleted_at: Option<DateTime<Utc>>,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ComputeVersionsModel {
    /// Compute Plane Version
    pub compute_plane_version: String,
    /// Polars Python Version
    pub polars_python_version: String,
    /// Polars Rust Revision
    pub polars_rust_revision: String,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StatusModel {
    /// Start time for the status
    pub status_time: DateTime<Utc>,
    /// Status Code
    pub code: QueryStatusCodeModel,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ResultModel {
    /// Number of stages for this query
    pub total_stages: i32,
    /// Number of finished stages
    pub finished_stages: i32,
    /// Number of failed stages
    pub failed_stages: i32,
    /// Number of result rows
    pub n_rows_result: Option<i64>,
    /// File type
    pub file_type_sink: Option<FileTypeModel>,
    /// Errors for query
    pub errors: Vec<String>,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, eq, eq_int))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, PartialEq, Deserialize, Serialize, Debug)]
pub enum FileTypeModel {
    Parquet,
    IPC,
    Csv,
    NDJSON,
    JSON,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, eq, eq_int))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Copy, PartialEq, Deserialize, Serialize, Debug)]
pub enum QueryTypeModel {
    Single,
    Distributed,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, eq, eq_int))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Copy, PartialEq, Deserialize, Serialize, Debug)]
pub enum QueryEngineModel {
    InMemory,
    Streaming,
}

#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct QueryWithStatusAndResultModel {
    #[serde(flatten)]
    pub query: QueryModel,
    pub result: Option<ResultModel>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct GetQueryArgs {
    pub cluster_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    #[serde(default)]
    #[serde(deserialize_with = "csv_vec_opt")]
    pub status: Option<Vec<QueryStatusCodeModel>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct QueryCountArgs {
    pub cluster_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct QueryCountModel {
    pub timestamp: DateTime<Utc>,
    // signed to be able to deserialize from postgres
    pub count: i64,
    pub count_successful: i64,
    pub count_failed: i64,
    pub count_in_progress: i64,
}

fn default_datetime() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

fn default_status_code() -> QueryStatusCodeModel {
    QueryStatusCodeModel::Canceled
}

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[deprecated_since_client("0.9.0")]
pub struct QueryStateTimingModel {
    /// Last known state for this query
    #[deprecated_since_client("0.7.0")]
    #[serde(default)]
    pub final_known_state: Option<QueryStatusCodeModel>,
    /// Time for the final state for this query
    #[deprecated_since_client("0.7.0")]
    #[serde(default)]
    pub final_status_time: Option<DateTime<Utc>>,
    /// The last known state that this query has
    #[deprecated_since_client("0.7.0")]
    #[serde(default = "default_status_code")]
    pub last_known_state: QueryStatusCodeModel,
    /// Last known status time for this query, belongs to last_known_state
    #[deprecated_since_client("0.7.0")]
    #[serde(default = "default_datetime")]
    pub last_known_status_time: DateTime<Utc>,
    /// Time for the last InProgress time
    #[deprecated_since_client("0.7.0")]
    #[serde(default)]
    pub last_progress_time: Option<DateTime<Utc>>,

    /// Latest state for this query
    #[deprecated_since_client("0.9.0")]
    pub latest_status: QueryStatusCodeModel,
    /// Latest state transition time for this query
    #[deprecated_since_client("0.9.0")]
    pub latest_status_time: DateTime<Utc>,
}

impl QueryStateTimingModel {
    pub fn new_from(
        latest_status: QueryStatusCodeModel,
        latest_status_time: DateTime<Utc>,
    ) -> Self {
        QueryStateTimingModel {
            // Bogus values as these are unused by newer client/frontend versions
            final_known_state: None,
            final_status_time: None,

            // Only value used by older client versions
            last_known_state: latest_status.clone(),

            // Bogus values as these are unused by newer client/frontend versions
            last_known_status_time: Utc::now(),
            last_progress_time: None,

            latest_status,
            latest_status_time,
        }
    }
}

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct QueryWithStateTimingModel {
    #[serde(flatten)]
    pub query: QueryModel,
    #[serde(flatten)]
    pub state_timing: QueryStateTimingModel,
    pub label_ids: Option<Vec<Uuid>>,
}

impl EntityOrdering for QueryWithStateTimingModel {
    fn order_fields() -> &'static [&'static str] {
        &["id", "latest_status_time", "request_time"]
    }

    fn default_ordering() -> Option<(&'static str, DefaultSortDirection)> {
        Some(("id", DefaultSortDirection::Desc))
    }
}

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object, get_all))]
#[derive(Debug, Deserialize, Serialize)]
pub struct QueryWithStateTimingAndResultModel {
    #[serde(flatten)]
    pub query: QueryModel,
    #[serde(flatten)]
    pub state_timing: QueryStateTimingModel,
    pub result: Option<ResultModel>,
}

impl Deref for QueryWithStateTimingAndResultModel {
    type Target = QueryModel;
    fn deref(&self) -> &Self::Target {
        &self.query
    }
}

#[cfg_attr(
    feature = "server",
    derive(Validate, JsonSchema),
    garde(allow_unvalidated)
)]
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Default)]
pub struct QueryObservedPlanArgs {
    pub ir_plan: Option<serde_json::Value>,
    pub phys_plan: Option<serde_json::Value>,
}

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum QueryPhysNodeKind {
    Other,
    Scan,
    Sink,
}

#[cfg_attr(
    feature = "server",
    derive(Validate, JsonSchema),
    garde(allow_unvalidated)
)]
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct QueryPhysNodeMetricsModel {
    pub phys_node_key: u64,
    pub query_phys_node_kind: QueryPhysNodeKind,
    pub total_polls: u64,
    pub total_stolen_polls: u64,
    pub total_poll_time_ns: u64,
    pub max_poll_time_ns: u64,
    pub total_state_updates: u64,
    pub total_state_update_time_ns: u64,
    pub max_state_update_time_ns: u64,
    pub morsels_sent: u64,
    pub rows_sent: u64,
    pub largest_morsel_sent: u64,
    pub morsels_received: u64,
    pub rows_received: u64,
    pub largest_morsel_received: u64,
    pub io_total_active_ns: u64,
    pub io_total_bytes_requested: u64,
    pub io_total_bytes_received: u64,
    pub io_total_bytes_sent: u64,
    pub total_time_ns: u64,
    pub done: bool,
}

#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct QueryStartedArgs {
    #[cfg_attr(feature = "server", garde(skip))]
    pub timestamp: DateTime<Utc>,
}

#[cfg_attr(
    feature = "server",
    derive(Validate, JsonSchema),
    garde(allow_unvalidated)
)]
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct QueryUpdateArgs {
    pub timestamp: DateTime<Utc>,
    #[cfg_attr(feature = "server", garde(dive))]
    pub plan: Option<QueryObservedPlanArgs>,
    #[cfg_attr(feature = "server", garde(dive))]
    pub metrics: Option<Vec<QueryPhysNodeMetricsModel>>,
    #[serde(default)]
    pub is_final: bool,
}

#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct QueryFailedArgs {
    #[cfg_attr(feature = "server", garde(skip))]
    pub timestamp: DateTime<Utc>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub error: String,
}
