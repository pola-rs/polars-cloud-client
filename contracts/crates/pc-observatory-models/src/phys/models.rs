use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

#[cfg(feature = "server")]
use schemars::JsonSchema;

use crate::ir::models::PredicateFileSkip;
use crate::phys::warning::PhysNodeWarning;
use crate::{Edge, SortColumn};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct PhysicalPlanVisualizationData {
    pub title: String,
    /// Number of nodes from the start of `nodes` that are root nodes.
    pub num_roots: u64,
    pub nodes: Vec<PhysNodeInfo>,
    pub edges: Vec<Edge>,
}

impl PhysicalPlanVisualizationData {
    /// Hash the graph structure (node variant names + edges) and return a hex string.
    pub fn phys_plan_variation(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        for node in &self.nodes {
            node.properties.variant_name().hash(&mut hasher);
        }

        for edge in &self.edges {
            edge.source.hash(&mut hasher);
            edge.target.hash(&mut hasher);
        }

        hasher.finish()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct PhysNodeInfo {
    pub id: u64,
    // Type node used for lookup purposes in frontend
    pub type_id: String,
    pub title: Option<String>,
    pub subtitle: Option<String>,
    pub properties: PhysNodeProperties,
    pub warnings: Vec<PhysNodeWarning>,
}

#[derive(serde::Serialize, serde::Deserialize, Default, Debug, strum_macros::IntoStaticStr)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[serde(tag = "type")]
pub enum PhysNodeProperties {
    #[default]
    Default,
    CallbackSink {
        maintain_order: bool,
        chunk_size: Option<NonZeroUsize>,
    },
    DynamicSlice,
    FileSink {
        target: String,
        file_format: String,
        maintain_order: bool,
        shuffle_id: Option<u32>,
    },
    Filter {
        predicate: String,
    },
    GatherEvery {
        n: usize,
        offset: usize,
    },
    GroupBy {
        num_inputs: usize,
        key_per_input: Vec<Vec<String>>,
        aggs_per_input: Vec<Vec<String>>,
    },
    DynamicGroupBy {
        index_column: String,
        period: String,
        every: String,
        offset: String,
        start_by: String,
        label: String,
        include_boundaries: bool,
        closed_window: String,
        aggs: Vec<String>,
        slice: Option<(u64, u64)>,
    },
    RollingGroupBy {
        index_column: String,
        period: String,
        offset: String,
        closed_window: String,
        slice: Option<(u64, u64)>,
        aggs: Vec<String>,
    },
    SortedGroupBy {
        key: String,
        aggs: Vec<String>,
        slice: Option<(u64, u64)>,
    },
    InMemoryMap {
        format_str: String,
    },
    InMemorySink,
    InMemorySource {
        n_rows: usize,
        schema_names: Vec<String>,
    },
    InputIndependentSelect {
        selectors: Vec<String>,
    },
    // Joins
    AsOfJoin {
        left_on: String,
        right_on: String,
        left_by: Option<Vec<String>>,
        right_by: Option<Vec<String>>,
        strategy: String,
        /// [value, dtype_str]
        tolerance: Option<[String; 2]>,
        suffix: Option<String>,
        slice: Option<(i64, usize)>,
        coalesce: String,
        allow_eq: bool,
        check_sortedness: bool,
    },
    RangeJoin {
        left_on: Vec<String>,
        right_on: Vec<String>,
        suffix: Option<String>,
        slice: Option<(i64, usize)>,
        coalesce: String,
        descending: bool,
    },
    CrossJoin {
        maintain_order: String,
        suffix: Option<String>,
    },
    EquiJoin {
        how: String,
        left_on: Vec<String>,
        right_on: Vec<String>,
        nulls_equal: bool,
        coalesce: String,
        maintain_order: String,
        validation: String,
        suffix: Option<String>,
    },
    MergeJoin {
        how: String,
        left_on: Vec<String>,
        right_on: Vec<String>,
        keys_row_encoded: bool,
        descending: bool,
        nulls_last: bool,
        nulls_equal: bool,
        coalesce: String,
        maintain_order: String,
        validation: String,
        suffix: Option<String>,
    },
    InMemoryJoin {
        how: String,
        left_on: Vec<String>,
        right_on: Vec<String>,
        nulls_equal: bool,
        coalesce: String,
        maintain_order: String,
        validation: String,
        suffix: Option<String>,
        slice: Option<(i64, usize)>,
    },
    InMemoryAsOfJoin {
        left_on: Vec<String>,
        right_on: Vec<String>,
        left_by: Option<Vec<String>>,
        right_by: Option<Vec<String>>,
        strategy: String,
        /// [value, dtype_str]
        tolerance: Option<[String; 2]>,
        suffix: Option<String>,
        slice: Option<(i64, usize)>,
        coalesce: String,
        allow_eq: bool,
        check_sortedness: bool,
    },
    InMemoryIEJoin {
        left_on: Vec<String>,
        right_on: Vec<String>,
        inequality_operators: Vec<String>,
        suffix: Option<String>,
        slice: Option<(i64, usize)>,
    },
    Map,
    MultiScan {
        scan_type: String,
        num_sources: usize,
        first_source: Option<String>,
        projected_file_columns: Vec<String>,
        row_index_name: Option<String>,
        row_index_offset: Option<u64>,
        predicate: Option<String>,
        predicate_file_skip_applied: Option<PredicateFileSkip>,
        has_table_statistics: bool,
        include_file_paths: Option<String>,
        deletion_files_type: Option<String>,
        hive_columns: Option<Vec<String>>,
        shuffle_id: Option<u32>,
    },
    Multiplexer,
    NegativeSlice {
        offset: i64,
        length: usize,
    },
    OrderedUnion {
        num_inputs: usize,
    },
    UnorderedUnion {
        num_inputs: usize,
    },
    PartitionSink {
        base_path: String,
        file_path_provider: String,
        file_format: String,
        partition_strategy: String,
        partition_key_exprs: Option<Vec<String>>,
        include_keys: Option<bool>,
        maintain_order: bool,
        max_rows_per_file: u64,
        approximate_bytes_per_file: u64,
        shuffle_id: Option<u32>,
    },
    PeakMin,
    PeakMax,
    Reduce {
        exprs: Vec<String>,
    },
    Repeat,
    Rle,
    RleId,
    Select {
        selectors: Vec<String>,
        extend_original: bool,
    },
    Shift {
        has_fill: bool,
    },
    ForwardFill {
        limit: Option<u64>,
    },
    BackwardFill {
        limit: Option<u64>,
    },
    SimpleProjection {
        columns: Vec<String>,
    },
    SinkMultiple {
        num_sinks: usize,
    },
    Sort {
        sort_columns: Vec<SortColumn>,
        slice: Option<(i64, usize)>,
        multithreaded: bool,
        maintain_order: bool,
        limit: Option<u64>,
    },
    SortedUnique {
        keys: Vec<String>,
    },
    IsFirstDistinct {
        keys: Vec<String>,
    },
    Slice {
        offset: i64,
        length: usize,
    },
    TopK {
        by_exprs: Vec<String>,
        reverse: Vec<bool>,
        nulls_last: Vec<bool>,
        dyn_pred: Option<String>,
    },
    WithRowIndex {
        name: String,
        offset: Option<u64>,
    },
    Zip {
        num_inputs: usize,
        zip_behavior: String,
    },
    //
    // Feature gated
    //
    CumAgg {
        kind: String,
    },
    Ewm {
        variant: String,
        alpha: f64,
        adjust: bool,
        bias: bool,
        min_periods: usize,
        ignore_nulls: bool,
    },
    SemiAntiJoin {
        left_on: Vec<String>,
        right_on: Vec<String>,
        nulls_equal: bool,
        output_as_bool: bool,
    },
    MergeSorted {
        maintain_order: bool,
    },
    PythonScan {
        scan_source_type: String,
        n_rows: Option<usize>,
        projection: Option<Vec<String>>,
        predicate: Option<String>,
        schema_names: Vec<String>,
        is_pure: bool,
        validate_schema: bool,
    },
    StrptimeInfer {
        format: Option<String>,
        strict: bool,
        exact: bool,
    },
    Interpolate {
        method: String,
    },
    Gather {
        null_on_oob: bool,
    },
    ColumnarFunction {
        num_inputs: usize,
        name: Option<String>,
    },
    IsSorted {
        descending: Option<bool>,
        nulls_last: Option<bool>,
        output_name: String,
    },
}

impl PhysNodeProperties {
    pub fn variant_name(&self) -> String {
        String::from(<&'static str>::from(self))
    }
}
