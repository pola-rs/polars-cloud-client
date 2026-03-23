use chrono::{DateTime, Utc};
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all, eq, eq_int))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub enum TerminationReasonModel {
    StoppedByUser,
    StoppedInactive,
    Failed,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminationModel {
    pub termination_reason: TerminationReasonModel,
    pub termination_time: DateTime<Utc>,
    pub termination_message: Option<String>,
}
