use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use pc_observatory_types::{
    DurationNs, NodeId, OutputRows, PhysNodeId, QueryId, ShuffleBytes, ShuffleId,
    StageAttemptNumber, StageNumber,
};
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::AggregatedPhysicalMetricFieldsModel;
use crate::ir::IRVisualizationData;
use crate::phys::PhysicalPlanVisualizationData;
use crate::stages::StageGraphVisualizationData;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct UserModel {
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, strum_macros::IntoStaticStr)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub enum QueryStatusCodeModel {
    Queued,
    Scheduled,
    InProgress,
    Success,
    Failed,
    Canceled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct QueryModel {
    pub id: Uuid,
    pub user: UserModel,
    pub status: QueryStatusCodeModel,
    pub request_time: DateTime<Utc>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct QueryPlanTimingModel {
    pub plan_start_time: Option<DateTime<Utc>>,
    pub parse_start_time: Option<DateTime<Utc>>,
    pub optimize_start_time: Option<DateTime<Utc>>,
    pub distribute_start_time: Option<DateTime<Utc>>,
    pub distribute_end_time: Option<DateTime<Utc>>,
    pub plan_end_time: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, strum_macros::IntoStaticStr)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub enum EngineTypeModel {
    Auto,
    Streaming,
    InMemory,
    Gpu,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, strum_macros::IntoStaticStr)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub enum QueryType2Model {
    Single,
    Distributed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct QueryDetailModel {
    pub id: QueryId,
    pub user: UserModel,
    pub status: QueryStatusCodeModel,
    pub request_time: DateTime<Utc>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub query_plan_timing: QueryPlanTimingModel,
    pub total_num_stages: Option<u32>,
    pub failed_stage: Option<StageNumber>,
    pub in_progress_stages: HashSet<StageNumber>,
    pub finished_stages: HashSet<StageNumber>,
    pub total_bytes_shuffled: Option<i64>,
    pub total_node_time_ns: i64,
    pub percentage_time_shuffling: Option<f64>,
    pub total_num_files: Option<u64>,
    pub original_num_files: Option<u64>,
    pub total_rows_read: Option<i64>,
    pub errors: Option<Vec<String>>,
    pub engine: EngineTypeModel,
    pub query_type: QueryType2Model,
    pub output_location: Option<String>,
    pub output_files: Option<i64>,
    pub output_rows: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct LogicalPlanModel {
    pub ir_visualization_data: Option<IRVisualizationData>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct StageGraphModel {
    pub sg_visualization_data: Option<StageGraphVisualizationData>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct PhysPlanVariationModel {
    /// The `PhysPlanVariation` hash as a decimal string. Serialized as a string
    /// because the underlying `u64` can exceed JS `Number.MAX_SAFE_INTEGER`.
    pub variation_hash: String,
    pub worker_count: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct StageModel {
    pub ir_visualization_data: Option<IRVisualizationData>,
    /// Physical plan variations for this stage, ranked from most to least common.
    pub phys_plan_variations: Vec<PhysPlanVariationModel>,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct PhysicalMetricsModel {
    pub metrics: HashMap<PhysNodeId, AggregatedPhysicalMetricFieldsModel>,
    pub attempt_number: StageAttemptNumber,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct StagePhysicalModel {
    pub phys_visualization_data: Option<PhysicalPlanVisualizationData>,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct HostMetricsAggregatedModel {
    pub node_count: i64,
    pub avg_cpu_percent: f64,
    pub peak_memory_percent: f64,
    pub avg_memory_percent: f64,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct StageDetailMetricsModel {
    pub stage_number: StageNumber,
    pub attempt_number: StageAttemptNumber,
    pub resolved_time: Option<DateTime<Utc>>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time_ns: Option<DateTime<Utc>>,
    pub output_rows: Option<OutputRows>,
    pub shuffle_bytes_read: Option<ShuffleBytes>,
    pub shuffle_bytes_written: Option<ShuffleBytes>,
    pub total_node_time_ns: Option<DurationNs>,
    pub used_workers: Option<i64>,
    pub available_workers: Option<i64>,
    pub host_metrics_aggregated: Option<HostMetricsAggregatedModel>,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct StageMetricsModel {
    pub stage_number: StageNumber,
    pub attempt_number: StageAttemptNumber,
    pub resolved_time: Option<DateTime<Utc>>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time_ns: Option<DateTime<Utc>>,
    pub output_rows: Option<OutputRows>,
    pub total_node_time_ns: Option<DurationNs>,
    pub used_workers: Option<i64>,
    pub available_workers: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct PartitionStatsModel {
    pub stage_number: StageNumber,
    pub shuffle_id: ShuffleId,
    pub partition_number: i64,
    pub min_bytes: i64,
    pub max_bytes: i64,
    pub percentile_25: i64,
    pub percentile_50: i64,
    pub percentile_75: i64,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct NodeStageDurationModel {
    pub node_id: NodeId,
    pub duration_ns: Option<i64>,
}
