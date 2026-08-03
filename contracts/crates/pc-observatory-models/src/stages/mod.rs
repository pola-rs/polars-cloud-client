mod fmt;
mod plans;
use pc_observatory_types::StageNumber;
pub use plans::QueryPlans;
#[cfg(feature = "server")]
use schemars::JsonSchema;

use crate::ir::models::IRVisualizationData;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct StageGraphVisualizationData {
    pub title: String,
    /// Number of nodes from the start of `nodes` that are root nodes.
    pub num_roots: u64,
    pub nodes: Vec<PhysStageInfo>,
    pub edges: Vec<StageEdge>,
}

impl StageGraphVisualizationData {
    pub fn get_node_data(self, stage_number: u32) -> Option<IRVisualizationData> {
        self.nodes
            .into_iter()
            .find_map(|phys| (phys.stage_number == stage_number).then_some(phys.data))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct PhysStageInfo {
    pub stage_number: u32,
    pub title: String,
    pub data: IRVisualizationData,
    pub properties: PhysStageProperties,
    pub explain_ir: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct PhysStageProperties {}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct LogicalNodeKey(pub u64);

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct NodeSpec {
    pub stage: StageNumber,
    pub node: LogicalNodeKey,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct StageEdge {
    pub source: NodeSpec,
    pub target: NodeSpec,
}
