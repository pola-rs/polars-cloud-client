#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct AggregatedPhysicalMetricFieldsModel {
    pub total_polls: i64,
    pub total_stolen_polls: i64,
    pub total_poll_time_ns: i64,
    pub max_poll_time_ns: i64,

    pub total_state_updates: i64,
    pub total_state_update_time_ns: i64,
    pub max_state_update_time_ns: i64,

    pub morsels_sent: i64,
    pub rows_sent: Option<i64>,
    pub largest_morsel_sent: i64,
    pub morsels_received: i64,
    pub rows_received: Option<i64>,
    pub largest_morsel_received: i64,

    pub io_total_active_ns: i64,
    pub io_total_bytes_requested: i64,
    pub io_total_bytes_received: i64,
    pub io_total_bytes_sent: i64,

    pub total_time_ns: i64,
    pub custom: Vec<CustomPhysicalMetricModel>,
    pub done: bool,
}

/// UCUM units custom metric OTel instruments are exported with
pub const CUSTOM_METRIC_UNIT_UNIT: &str = "1";
pub const CUSTOM_METRIC_UNIT_BYTES: &str = "By";
pub const CUSTOM_METRIC_UNIT_DURATION_NS: &str = "ns";

#[derive(Clone, Copy, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub enum PhysicalMetricUnitModel {
    Unit,
    Bytes,
    DurationNs,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct CustomPhysicalMetricModel {
    pub key: String,
    pub unit: PhysicalMetricUnitModel,
    pub value: i64,
}
