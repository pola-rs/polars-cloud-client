pub mod query;

pub const MAX_SUBTITLE_LENGTH: usize = 32;

use chrono::{DateTime, Utc};
pub use query::*;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::Deserialize;

pub mod cluster;
#[cfg(feature = "convert")]
pub mod convert;
mod fmt;
pub mod ir;
pub mod node;
pub mod phys;
pub mod physical;
pub mod stages;
pub mod subtitle;

pub use cluster::*;
pub use node::*;
pub use physical::*;

#[derive(Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct TimeWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

/// How the samples inside a bin are collapsed into a single value.
///
/// When absent from a request, every metric keeps its own default (rates are averaged, gauges are maxed). When present, it overrides all of them.
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum Aggregation {
    Min,
    Avg,
    Max,
}

#[derive(Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct BinSize {
    pub bin_size_seconds: u64,
    #[serde(default)]
    pub aggregation: Option<Aggregation>,
}

#[derive(Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct MetricWindow {
    #[serde(flatten)]
    pub window: TimeWindow,
    // Aide will only support chrono::Duration from 0.16 onwards (depends on Schemars 1.x)
    pub interval_seconds: u64,
    #[serde(default)]
    pub aggregation: Option<Aggregation>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Copy, Clone)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct Edge {
    pub source: u64,
    pub target: u64,
}

impl Edge {
    pub fn new<T, U>(source: T, target: U) -> Self
    where
        u64: TryFrom<T> + TryFrom<U>,
        <u64 as TryFrom<T>>::Error: std::fmt::Debug,
        <u64 as TryFrom<U>>::Error: std::fmt::Debug,
    {
        Self {
            source: source.try_into().unwrap(),
            target: target.try_into().unwrap(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct SortColumn {
    pub expr: String,
    pub descending: bool,
    pub nulls_last: bool,
}
