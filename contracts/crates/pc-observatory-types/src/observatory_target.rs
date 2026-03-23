//! Typed target names for observatory instrumentation.
//!
//! This module provides type-safe target names for tracing spans that are processed
//! by the observatory service.

use std::fmt;

use serde::{Deserialize, Serialize};

// Constants for use in #[instrument] macros and as the single source of truth
pub const PLAN_QUERY: &str = "observatory::plan_query";
pub const PARSE_QUERY: &str = "observatory::parse_query";
pub const OPTIMIZE_QUERY: &str = "observatory::optimize_query";
pub const DISTRIBUTE_QUERY: &str = "observatory::distribute_query";
pub const SHUFFLE_READ: &str = "observatory::shuffle_read";
pub const EXECUTE_IR: &str = "observatory::execute_ir";
pub const EXECUTE_STAGE: &str = "observatory::execute_stage";
pub const LOWER_STREAMING_PHYS: &str = "observatory::lower_streaming_phys";
pub const PREPARE_AND_EXECUTE_TASK: &str = "observatory::prepare_and_execute_task";

// Span description of the final attempt failure message, to know when no retries are left
pub const FINAL_ATTEMPT_FAILURE_MESSAGE: &str = "final_attempt";

/// Observatory span target names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObservatorySpanTarget {
    PlanQuery,
    ParseQuery,
    OptimizeQuery,
    DistributeQuery,
    ShuffleRead,
    ExecuteIr,
    ExecuteStage,
    LowerStreamingPhys,
    PrepareAndExecuteTask,
}

impl ObservatorySpanTarget {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PlanQuery => PLAN_QUERY,
            Self::ParseQuery => PARSE_QUERY,
            Self::OptimizeQuery => OPTIMIZE_QUERY,
            Self::DistributeQuery => DISTRIBUTE_QUERY,
            Self::ShuffleRead => SHUFFLE_READ,
            Self::ExecuteIr => EXECUTE_IR,
            Self::ExecuteStage => EXECUTE_STAGE,
            Self::LowerStreamingPhys => LOWER_STREAMING_PHYS,
            Self::PrepareAndExecuteTask => PREPARE_AND_EXECUTE_TASK,
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            PLAN_QUERY => Some(Self::PlanQuery),
            PARSE_QUERY => Some(Self::ParseQuery),
            OPTIMIZE_QUERY => Some(Self::OptimizeQuery),
            DISTRIBUTE_QUERY => Some(Self::DistributeQuery),
            SHUFFLE_READ => Some(Self::ShuffleRead),
            EXECUTE_IR => Some(Self::ExecuteIr),
            EXECUTE_STAGE => Some(Self::ExecuteStage),
            LOWER_STREAMING_PHYS => Some(Self::LowerStreamingPhys),
            PREPARE_AND_EXECUTE_TASK => Some(Self::PrepareAndExecuteTask),
            _ => None,
        }
    }
}

impl fmt::Display for ObservatorySpanTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<ObservatorySpanTarget> for &'static str {
    fn from(target: ObservatorySpanTarget) -> Self {
        target.as_str()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionWriteStatistics {
    pub partitions: Vec<i64>,
    pub bytes: Vec<i64>,
}
