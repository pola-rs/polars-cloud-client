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
    pub rows_sent: i64,
    pub largest_morsel_sent: i64,
    pub morsels_received: i64,
    pub rows_received: i64,
    pub largest_morsel_received: i64,

    pub io_total_active_ns: i64,
    pub io_total_bytes_requested: i64,
    pub io_total_bytes_received: i64,
    pub io_total_bytes_sent: i64,

    pub total_time_ns: i64,
    pub done: bool,
}
