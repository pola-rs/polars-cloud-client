#[cfg(feature = "pyo3")]
use pyo3::pyclass;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
