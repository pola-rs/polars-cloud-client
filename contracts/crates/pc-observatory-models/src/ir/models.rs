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
    pub title: Option<String>,
    pub subtitle: Option<String>,
    #[serde(flatten)]
    pub properties: IRNodeProperties,
}

#[derive(serde::Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[serde(transparent)]
pub struct Predicate(pub Vec<String>);

impl<'a> serde::Deserialize<'a> for Predicate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        use serde::de::{self, Visitor};

        struct StringOrVec;
        impl<'de> Visitor<'de> for StringOrVec {
            type Value = Vec<String>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("string or list")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(vec![value.to_owned()])
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                de::Deserialize::deserialize(de::value::SeqAccessDeserializer::new(seq))
            }
        }

        Ok(Self(deserializer.deserialize_any(StringOrVec)?))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub enum AggKind {
    #[serde(rename = "aggs")]
    Aggs(Vec<String>),
    #[serde(rename = "apply")]
    Apply,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, strum_macros::IntoStaticStr)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[serde(tag = "type_id", content = "properties")]
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
        predicate: Predicate,
    },
    Gather {
        null_on_oob: bool,
    },
    GroupBy {
        keys: Vec<String>,
        #[serde(flatten)]
        agg_kind: AggKind,
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
        predicate: Option<Predicate>,
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
        predicate: Option<Predicate>,
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
        #[serde(flatten)]
        agg_kind: AggKind,
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
        #[serde(flatten)]
        agg_kind: AggKind,
        index_column: String,
        period: String,
        offset: String,
        closed_window: String,
        slice: Option<(i64, u64)>,
    },
    MergeSorted {
        keys: Vec<String>,
        maintain_order: bool,
    },
    PythonScan {
        scan_source_type: String,
        n_rows: Option<u64>,
        projection: Option<Vec<String>>,
        predicate: Option<Predicate>,
        schema_names: Vec<String>,
        is_pure: bool,
        validate_schema: bool,
    },

    // New TaskPlan specific variants
    PythonMultiScan {
        n_scans: u64,
        maintain_order: bool,
    },
    ShuffleRead {
        shuffle_number: u32,
        partitioning: PartitioningModel,
        is_local: bool,
        schema_names: Vec<String>,
    },
    ShuffleWrite {
        shuffle_number: u32,
        partitioning: PartitioningModel,
        collect_samples_col: Option<String>,
    },
    Sink2 {
        sink_type: String,
        file_format: String,
        location: String,
        partition_strategy: Option<String>,
    },
    CallbackSink {
        maintain_order: bool,
    },
    UnoptimizedDispatch {
        operation: String,
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

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
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
