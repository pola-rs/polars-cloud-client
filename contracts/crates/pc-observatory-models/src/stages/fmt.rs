use std::cmp::Reverse;
use std::fmt::{Display, Write as _};

use crate::fmt::PrefixWrite;
use crate::stages::{PhysStageInfo, StageGraphVisualizationData};

impl StageGraphVisualizationData {
    pub fn explain(&self) -> StageGraphDisplay<'_> {
        let mut sinks_to_sources_stage_order: Vec<_> = (0..self.nodes.len()).collect();
        sinks_to_sources_stage_order.sort_by_key(|&idx| Reverse(self.nodes[idx].stage_number));

        StageGraphDisplay {
            data: self,
            sinks_to_sources_stage_order,
        }
    }
}

#[derive(Debug)]
pub struct StageGraphDisplay<'a> {
    data: &'a StageGraphVisualizationData,
    // Topological stage order from sources to sinks
    sinks_to_sources_stage_order: Vec<usize>,
}

impl PhysStageInfo {
    pub fn explain(&self) -> PhysStageInfoDisplay<'_> {
        PhysStageInfoDisplay(self)
    }
}

pub struct PhysStageInfoDisplay<'a>(&'a PhysStageInfo);

impl Display for StageGraphDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut f = PrefixWrite::new(f, 2);

        // Go sink to source, only writing each stage once
        for &idx in &self.sinks_to_sources_stage_order {
            f.set_indent_level(0);
            let stage = &self.data.nodes[idx];
            writeln!(f, "{}", stage.explain())?;
        }

        Ok(())
    }
}

impl Display for PhysStageInfoDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut f = PrefixWrite::new(f, 2);
        write!(f, "STAGE {}:", self.0.stage_number)?;
        f.with_indent(1, |f| write!(f, "{}", self.0.data.explain()))
    }
}
