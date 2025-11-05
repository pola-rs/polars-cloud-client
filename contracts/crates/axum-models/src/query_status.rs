#[cfg(feature = "pyo3")]
use pyo3::pyclass;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::ToSchema;

#[cfg_attr(feature = "pyo3", pyclass(get_all, eq, eq_int))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum QueryStatusCodeSchema {
    Queued,
    Scheduled,
    InProgress,
    Success,
    Failed,
    Canceled,
}
