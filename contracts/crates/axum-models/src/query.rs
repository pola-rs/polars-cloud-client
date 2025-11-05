use chrono::{DateTime, Utc};
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::ToSchema;
use uuid::Uuid;

use crate::EntityOrdering;
use crate::query_status::QueryStatusCodeSchema;

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct QuerySchema {
    /// Query ID
    pub id: Uuid,
    /// The workspace the query is being run in
    pub workspace_id: Uuid,
    /// The virtual machine it is sent to
    pub cluster_id: Uuid,
    /// The user account that started the instance
    pub user_id: Uuid,
    /// The time the query was requested
    pub request_time: DateTime<Utc>,
    /// Timestamp when the query was created
    pub created_at: DateTime<Utc>,
    /// Last update timestamp
    pub updated_at: DateTime<Utc>,
    /// Timestamp of the last update
    pub deleted_at: Option<DateTime<Utc>>,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct QueryPlansSchema {
    /// Query ID
    pub id: Uuid,
    /// The immediate representation in dotfile format
    pub ir_plan: Option<String>,
    /// The physical plan in dotfile format
    pub phys_plan: Option<String>,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StatusSchema {
    /// Start time for the status
    pub status_time: DateTime<Utc>,
    /// Status Code
    pub code: QueryStatusCodeSchema,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ResultSchema {
    /// Number of stages for this query
    pub total_stages: i32,
    /// Number of finished stages
    pub finished_stages: i32,
    /// Number of failed stages
    pub failed_stages: i32,
    /// Number of result rows
    pub n_rows_result: Option<i64>,
    /// File type
    pub file_type_sink: Option<FileTypeSchema>,
    /// Errors for query
    pub errors: Vec<String>,
}

#[cfg_attr(feature = "pyo3", pyclass(eq, eq_int))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Clone, PartialEq, Deserialize, Serialize, Debug)]
pub enum FileTypeSchema {
    Parquet,
    IPC,
    Csv,
    NDJSON,
    JSON,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct QueryWithStatusAndResultSchema {
    #[serde(flatten)]
    pub query: QuerySchema,
    pub status: StatusSchema,
    pub result: Option<ResultSchema>,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct QueryWithStatusSchema {
    #[serde(flatten)]
    pub query: QuerySchema,
    pub status: StatusSchema,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct QueryParamsFilter {
    pub cluster_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct QueryCountParams {
    pub cluster_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct QueryCountSchema {
    pub timestamp: DateTime<Utc>,
    // signed to be able to deserialize from postgres
    pub count: i64,
    pub count_successful: i64,
    pub count_failed: i64,
    pub count_in_progress: i64,
}

#[cfg_attr(feature = "server", derive(ToSchema))]
#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryStateTimingSchema {
    // TODO: Remove once version client version 0.3.0 is unused
    /// Last known state for this query
    pub final_known_state: Option<QueryStatusCodeSchema>,
    // TODO: Remove once version client version 0.3.0 is unused
    /// Time for the final state for this query
    pub final_status_time: Option<chrono::DateTime<chrono::Utc>>,
    // TODO: Remove once version client version 0.3.0 is unused
    /// The last known state that this query has
    pub last_known_state: QueryStatusCodeSchema,
    // TODO: Remove once version client version 0.3.0 is unused
    /// Last known status time for this query, belongs to last_known_state
    pub last_known_status_time: chrono::DateTime<chrono::Utc>,
    // TODO: Remove once version client version 0.3.0 is unused
    /// Time for the last InProgress time
    pub last_progress_time: Option<chrono::DateTime<chrono::Utc>>,

    /// Latest state for this query
    pub latest_status: QueryStatusCodeSchema,
    /// Latest state transition time for this query
    pub latest_status_time: DateTime<Utc>,
    /// When this query last changed to in_progress
    pub started_at: Option<DateTime<Utc>>,
    /// When this query reached a done state (failed, canceled, success)
    pub ended_at: Option<DateTime<Utc>>,
}

#[cfg_attr(feature = "server", derive(ToSchema))]
#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct QueryWithStateTimingSchema {
    #[serde(flatten)]
    pub query: QuerySchema,
    #[serde(flatten)]
    pub state_timing: QueryStateTimingSchema,
}

impl EntityOrdering for QueryWithStateTimingSchema {
    fn order_fields() -> &'static [&'static str] {
        &["id", "latest_status_time", "request_time"]
    }
}

#[cfg_attr(feature = "server", derive(ToSchema))]
#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[derive(Debug, Deserialize, Serialize)]
pub struct QueryWithStateTimingAndResultSchema {
    #[serde(flatten)]
    pub query: QuerySchema,
    #[serde(flatten)]
    pub state_timing: QueryStateTimingSchema,
    pub result: Option<ResultSchema>,
}
