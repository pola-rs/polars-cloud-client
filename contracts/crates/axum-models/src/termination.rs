use chrono::{DateTime, Utc};
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::ToSchema;

#[cfg_attr(feature = "pyo3", pyclass(get_all, eq, eq_int))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub enum TerminationReasonSchema {
    StoppedByUser,
    StoppedInactive,
    Failed,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminationSchema {
    pub termination_reason: TerminationReasonSchema,
    pub termination_time: DateTime<Utc>,
    pub termination_message: Option<String>,
}
