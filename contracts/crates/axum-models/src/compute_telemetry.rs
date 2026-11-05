use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(
    feature = "server",
    derive(Validate, JsonSchema),
    garde(allow_unvalidated)
)]
pub struct GetComputeLogsArgs {
    /// Only return logs of this query. Takes precedence over `instance_id`.
    pub query_id: Option<Uuid>,
    /// Only return logs of this instance of the cluster.
    pub instance_id: Option<String>,
    /// Defaults to 5 minutes before `end`.
    pub start: Option<DateTime<Utc>>,
    /// Defaults to now.
    pub end: Option<DateTime<Utc>>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(
    feature = "server",
    derive(Validate, JsonSchema),
    garde(allow_unvalidated)
)]
pub struct GetComputeMetricsArgs {
    /// Only return metrics of this instance of the cluster. Network metrics are only returned
    /// when this is set.
    pub instance_id: Option<String>,
    /// Defaults to 5 minutes before `end`.
    pub start: Option<DateTime<Utc>>,
    /// Defaults to now.
    pub end: Option<DateTime<Utc>>,
    /// Minimum interval between data points, in seconds.
    pub interval: Option<u32>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct ComputeLogLineModel {
    pub timestamp: DateTime<Utc>,
    pub line: String,
    pub instance: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct MetricPointModel {
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct MetricSeriesModel {
    pub labels: BTreeMap<String, String>,
    pub points: Vec<MetricPointModel>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct ComputeMetricsModel {
    /// CPU utilization as a ratio between 0 and 1.
    pub cpu: Vec<MetricSeriesModel>,
    /// Filesystem utilization as a ratio between 0 and 1.
    pub disk: Vec<MetricSeriesModel>,
    /// Memory utilization as a ratio between 0 and 1.
    pub memory: Vec<MetricSeriesModel>,
    /// Network throughput in bytes per second, one series per direction.
    pub network: Vec<MetricSeriesModel>,
}
