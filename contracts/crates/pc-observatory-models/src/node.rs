use chrono::{DateTime, Utc};
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct NodeModel {
    pub id: String,
    pub started_at: DateTime<Utc>,
    pub terminated_at: Option<DateTime<Utc>>,
    /// Number of CPU cores reserved for the compute-plane on this node
    pub cpu_reserved: f64,
    /// Maximum number of CPU cores available for the compute-plane on this node (possibly shared with other processes)
    pub cpu_limit: f64,
    pub memory_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct NodeMetricsModel {
    pub timestamp_ms: Vec<u64>,
    /// CPU load % of reserved CPU cores. Can be above >100% if more than the reserved was available.
    pub cpu_percentage: Vec<f64>,
    /// RAM usage % of total available
    pub ram_percentage: Vec<f64>,
    /// Inbound network traffic in bytes/s
    pub network_bytes_in_s: Vec<f64>,
    /// Outbound network traffic in bytes/s
    pub network_bytes_out_s: Vec<f64>,
}
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct NodeMetricModel {
    pub node_id: String,
    pub timestamp_ms: u64,
    /// CPU load % of reserved CPU cores. Can be above >100% if more than the reserved was available.
    pub cpu_percentage: f64,
    /// RAM usage % of total available
    pub ram_percentage: f64,
    /// Inbound network traffic in bytes/s
    pub network_bytes_in_s: f64,
    /// Outbound network traffic in bytes/s
    pub network_bytes_out_s: f64,
}
