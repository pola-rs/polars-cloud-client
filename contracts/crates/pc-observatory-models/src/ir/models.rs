#[cfg(feature = "server")]
use schemars::JsonSchema;

use crate::{Edge, SortColumn};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct IRVisualizationData {
    pub title: String,
    /// Number of nodes from the start of `nodes` that are root nodes.
    pub num_roots: u64,
    pub nodes: Vec<IRNodeInfo>,
    pub edges: Vec<Edge>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct IRNodeInfo {
    pub id: u64,
    pub type_id: String,
    pub title: Option<String>,
    pub subtitle: Option<String>,
    pub properties: IRNodeProperties,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, strum_macros::IntoStaticStr)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[serde(tag = "type")]
pub enum IRNodeProperties {
    Cache {
        id: String,
    },
    DataFrameScan {
        n_rows: u64,
        schema_names: Vec<String>,
    },
    Distinct {
        subset: Option<Vec<String>>,
        maintain_order: bool,
        keep_strategy: String,
        slice: Option<(i64, u64)>,
    },
    ExtContext {
        num_contexts: u64,
        schema_names: Vec<String>,
    },
    Filter {
        predicate: String,
    },
    GroupBy {
        keys: Vec<String>,
        aggs: Vec<String>,
        maintain_order: bool,
        slice: Option<(i64, u64)>,
    },
    HConcat {
        num_inputs: u64,
        schema_names: Vec<String>,
        strict: bool,
    },
    HStack {
        exprs: Vec<String>,
        duplicate_check: bool,
        should_broadcast: bool,
    },
    #[default]
    Invalid,
    Join {
        how: String,
        left_on: Vec<String>,
        right_on: Vec<String>,
        nulls_equal: bool,
        coalesce: String,
        maintain_order: String,
        validation: String,
        suffix: Option<String>,
        slice: Option<(i64, u64)>,
    },
    CrossJoin {
        maintain_order: String,
        slice: Option<(i64, u64)>,
        predicate: Option<String>,
        suffix: Option<String>,
    },
    MapFunction {
        function: String,
    },
    Scan {
        scan_type: String,
        num_sources: u64,
        first_source: Option<String>,
        file_columns: Option<Vec<String>>,
        projection: Option<Vec<String>>,
        row_index_name: Option<String>,
        row_index_offset: Option<u64>,
        pre_slice: Option<(i64, u64)>,
        predicate: Option<String>,
        predicate_file_skip_applied: Option<PredicateFileSkip>,
        has_table_statistics: bool,
        include_file_paths: Option<String>,
        column_mapping_type: Option<String>,
        hive_columns: Option<Vec<String>>,
    },
    Select {
        exprs: Vec<String>,
    },
    SimpleProjection {
        columns: Vec<String>,
    },
    Sink {
        sink_type: String,
        file_format: Option<String>,
        location: Option<String>,
    },
    SinkMultiple {
        num_inputs: u64,
    },
    Slice {
        offset: i64,
        len: u64,
    },
    Sort {
        sort_columns: Vec<SortColumn>,
        slice: Option<(i64, u64, Option<String>)>,
        maintain_order: bool,
        limit: Option<u64>,
    },
    Union {
        maintain_order: bool,
        slice: Option<(i64, u64)>,
    },
    //
    // Feature gated
    //
    AsOfJoin {
        left_on: String,
        right_on: String,
        left_by: Option<Vec<String>>,
        right_by: Option<Vec<String>>,
        strategy: String,
        /// [value, dtype_str]
        tolerance: Option<[String; 2]>,
        suffix: Option<String>,
        slice: Option<(i64, u64)>,
        coalesce: String,
        allow_eq: bool,
        check_sortedness: bool,
    },
    IEJoin {
        left_on: Vec<String>,
        right_on: Vec<String>,
        inequality_operators: Vec<String>,
        suffix: Option<String>,
        slice: Option<(i64, u64)>,
    },
    DynamicGroupBy {
        index_column: String,
        every: String,
        period: String,
        offset: String,
        label: String,
        include_boundaries: bool,
        closed_window: String,
        group_by: Vec<String>,
        start_by: String,
    },
    RollingGroupBy {
        keys: Vec<String>,
        aggs: Vec<String>,
        index_column: String,
        period: String,
        offset: String,
        closed_window: String,
        slice: Option<(i64, u64)>,
    },
    MergeSorted {
        key: String,
    },
    PythonScan {
        scan_source_type: String,
        n_rows: Option<u64>,
        projection: Option<Vec<String>>,
        predicate: Option<String>,
        schema_names: Vec<String>,
        is_pure: bool,
        validate_schema: bool,
    },

    // New TaskPlan specific variants
    ShuffleRead {
        shuffle_number: u32,
        partitioning: PartitioningModel,
        is_local: bool,
        schema_names: Vec<String>,
    },
    ShuffleWrite {
        shuffle_number: u32,
        partitioning: Option<PartitioningModel>,
        collect_samples_col: Option<String>,
    },
    Sink2 {
        sink_type: String,
        file_format: String,
        location: String,
        partition_strategy: Option<String>,
    },
}

#[derive(
    Default, Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct PredicateFileSkip {
    pub no_residual_predicate: bool,
    pub original_len: usize,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[serde(tag = "partition_type")]
pub enum PartitioningModel {
    RoundRobin,
    Local,
    Single,
    Broadcast,
    Hash { by: String },
    Range,
}
