use chrono::{DateTime, Utc};
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub enum ClusterModeModel {
    AWS,
    Kubernetes,
    BareMetal,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct ScratchpadConfigModel {
    pub local_scheduler_port: Option<u16>,
    pub local_observatory_port: Option<u16>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct ClusterModel {
    pub scheduler_started_at: DateTime<Utc>,
    pub node_count: u32,
    pub region: Option<String>,
    pub mode: ClusterModeModel,
    pub scratchpad_config: Option<ScratchpadConfigModel>,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct ClusterLiveMetricsModel {
    pub timestamp_ms: u64,
    /// CPU load % of reserved CPU cores. Can be above >100% if more than the reserved was available.
    pub cpu_percentage: f64,
    /// RAM used % of the total available
    pub ram_percentage: f64,
    /// Inbound network traffic in bytes/s
    pub network_bytes_in_s: f64,
    /// Outbound network traffic in bytes/s
    pub network_bytes_out_s: f64,
    /// Disk usage in bytes
    pub disk_bytes_used: Option<f64>,
    /// Disk read traffic in bytes/s
    pub disk_bytes_read_s: Option<f64>,
    /// Disk write traffic in bytes/s
    pub disk_bytes_written_s: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct ClusterMetricsModel {
    pub timestamp_ms: Vec<u64>,
    /// CPU load % of reserved CPU cores. Can be above >100% if more than the reserved was available.
    pub cpu_percentage: Vec<f64>,
    /// RAM usage % of total available
    pub ram_percentage: Vec<f64>,
    /// Inbound network traffic in bytes/s
    pub network_bytes_in_s: Vec<f64>,
    /// Outbound network traffic in bytes/s
    pub network_bytes_out_s: Vec<f64>,
    /// Disk usage in bytes
    pub disk_bytes_used: Option<Vec<f64>>,
    /// Disk read traffic in bytes/s
    pub disk_bytes_read_s: Option<Vec<f64>>,
    /// Disk write traffic in bytes/s
    pub disk_bytes_written_s: Option<Vec<f64>>,
}
