use chrono::{DateTime, Utc};
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::EntityOrdering;
use crate::query_status::QueryStatusCodeModel;

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct QueryModel {
    /// Query ID
    pub id: Uuid,
    /// The workspace the query is being run in
    pub workspace_id: Uuid,
    /// The virtual machine it is sent to
    pub cluster_id: Uuid,
    /// The user account that started the instance
    pub user_id: Option<Uuid>,
    /// The time the query was requested
    pub request_time: DateTime<Utc>,
    /// Timestamp when the query was created
    pub created_at: DateTime<Utc>,
    /// Last update timestamp
    pub updated_at: DateTime<Utc>,
    /// Timestamp of the last update
    pub deleted_at: Option<DateTime<Utc>>,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct QueryPlansModel {
    /// Query ID
    pub id: Uuid,
    /// The immediate representation in dotfile format
    pub ir_plan: Option<String>,
    /// The physical plan in dotfile format
    pub phys_plan: Option<String>,
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

#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct QueryWithStatusAndResultModel {
    #[serde(flatten)]
    pub query: QueryModel,
    pub status: StatusModel,
    pub result: Option<ResultModel>,
}

#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct QueryWithStatusModel {
    #[serde(flatten)]
    pub query: QueryModel,
    pub status: StatusModel,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct GetQueryArgs {
    pub cluster_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
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

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryStateTimingModel {
    // TODO: Remove once version client version 0.3.0 is unused
    /// Last known state for this query
    pub final_known_state: Option<QueryStatusCodeModel>,
    // TODO: Remove once version client version 0.3.0 is unused
    /// Time for the final state for this query
    pub final_status_time: Option<chrono::DateTime<chrono::Utc>>,
    // TODO: Remove once version client version 0.3.0 is unused
    /// The last known state that this query has
    pub last_known_state: QueryStatusCodeModel,
    // TODO: Remove once version client version 0.3.0 is unused
    /// Last known status time for this query, belongs to last_known_state
    pub last_known_status_time: chrono::DateTime<chrono::Utc>,
    // TODO: Remove once version client version 0.3.0 is unused
    /// Time for the last InProgress time
    pub last_progress_time: Option<chrono::DateTime<chrono::Utc>>,

    /// Latest state for this query
    pub latest_status: QueryStatusCodeModel,
    /// Latest state transition time for this query
    pub latest_status_time: DateTime<Utc>,
    /// When this query last changed to in_progress
    pub started_at: Option<DateTime<Utc>>,
    /// When this query reached a done state (failed, canceled, success)
    pub ended_at: Option<DateTime<Utc>>,
}

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object, get_all))]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct QueryWithStateTimingModel {
    #[serde(flatten)]
    pub query: QueryModel,
    #[serde(flatten)]
    pub state_timing: QueryStateTimingModel,
}

impl EntityOrdering for QueryWithStateTimingModel {
    fn order_fields() -> &'static [&'static str] {
        &["id", "latest_status_time", "request_time"]
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
