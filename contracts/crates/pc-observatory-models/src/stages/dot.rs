use std::fmt::{self, Display, Write as _};

use crate::Edge;
use crate::fmt::display_trunc;
use crate::ir::IRNodeInfo;
use crate::ir::models::{AggKind, IRNodeProperties};
use crate::stages::{StageEdge, StageGraphVisualizationData};

const INDENT: &str = "  ";

impl StageGraphVisualizationData {
    /// Renders the stage graph as a [Graphviz] document, one cluster per stage.
    ///
    /// [Graphviz]: https://graphviz.org/doc/info/lang.html
    pub fn display_dot(&self) -> StageGraphDotDisplay<'_> {
        StageGraphDotDisplay(self)
    }
}

#[derive(Debug)]
pub struct StageGraphDotDisplay<'a>(&'a StageGraphVisualizationData);

impl Display for StageGraphDotDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "digraph stage_graph {{")?;
        // Sources at the bottom, sinks at the top, inputs in the order written below.
        writeln!(f, "{INDENT}rankdir=\"BT\"")?;
        writeln!(f, "{INDENT}ordering=\"in\"")?;
        writeln!(f, "{INDENT}compound=true")?;
        writeln!(f, "{INDENT}node [fontname=\"Monospace\", shape=\"box\"]")?;
        writeln!(f, "{INDENT}edge [fontname=\"Monospace\"]")?;
        writeln!(f, "{INDENT}graph [fontname=\"Monospace\", labelloc=\"b\"]")?;

        for stage in &self.0.nodes {
            let stage_number = stage.stage_number;
            writeln!(f, "{INDENT}subgraph cluster_{stage_number} {{")?;
            writeln!(f, "{INDENT}{INDENT}label=\"{}\"", Escaped(&stage.title))?;

            for node in &stage.data.nodes {
                writeln!(
                    f,
                    "{INDENT}{INDENT}{} [label=\"{}\"]",
                    NodeId(stage_number, node.id),
                    Escaped(NodeLabel(node)),
                )?;
            }

            // An `Edge` points at an input, so draw it the other way around to follow
            // the data.
            for Edge { source, target } in &stage.data.edges {
                writeln!(
                    f,
                    "{INDENT}{INDENT}{} -> {}",
                    NodeId(stage_number, *target),
                    NodeId(stage_number, *source),
                )?;
            }

            writeln!(f, "{INDENT}}}")?;
        }

        // A `StageEdge` already points from the shuffle write to the shuffle read.
        for StageEdge { source, target } in &self.0.edges {
            writeln!(
                f,
                "{INDENT}{} -> {}",
                NodeId(source.stage.0, source.node.0),
                NodeId(target.stage.0, target.node.0),
            )?;
        }

        writeln!(f, "}}")
    }
}

/// Node keys are only unique within their stage.
struct NodeId(u32, u64);

impl Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s{}_n{}", self.0, self.1)
    }
}

/// A label for a single node.
///
/// [`IRVisualizationData::explain`](crate::ir::IRVisualizationData::explain) renders
/// a plan as a nested tree, so each node only writes the part its position does not
/// already imply. A graph draws every node on its own, and needs this instead: the
/// operator and its structural options, with the expressions it works on left to
/// [`IRNodeInfo::subtitle`].
struct NodeLabel<'a>(&'a IRNodeInfo);

impl Display for NodeLabel<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use IRNodeProperties as P;

        match &self.0.title {
            Some(title) => f.write_str(title),
            None => match &self.0.properties {
                P::AsOfJoin { strategy, .. } => write!(f, "ASOF JOIN[strategy: {strategy}]"),
                P::Cache { id } => write!(f, "CACHE[id: {id}]"),
                P::CallbackSink { .. } => f.write_str("SINK (Callback)"),
                P::CrossJoin { .. } => f.write_str("NESTED LOOP JOIN"),
                P::DataFrameScan { schema_names, .. } => {
                    write!(f, "DF {}", display_trunc(schema_names, 4))
                },
                P::Distinct {
                    maintain_order,
                    keep_strategy,
                    ..
                } => write!(
                    f,
                    "UNIQUE[maintain_order: {maintain_order}, keep_strategy: {keep_strategy}]"
                ),
                P::DynamicGroupBy {
                    agg_kind,
                    every,
                    period,
                    ..
                } => write!(
                    f,
                    "{} DYNAMIC BY[every: {every}, period: {period}]",
                    agg_name(agg_kind)
                ),
                P::ExtContext { .. } => f.write_str("EXTERNAL_CONTEXT"),
                P::Filter { .. } => f.write_str("FILTER"),
                P::FlightSink { .. } => f.write_str("SINK (Flight)"),
                P::Gather { null_on_oob } => write!(f, "GATHER[null_on_oob: {null_on_oob}]"),
                P::GroupBy {
                    agg_kind,
                    maintain_order,
                    ..
                } => write!(
                    f,
                    "{}[maintain_order: {maintain_order}]",
                    agg_name(agg_kind)
                ),
                P::HConcat { .. } => f.write_str("HCONCAT"),
                P::HStack { .. } => f.write_str("WITH_COLUMNS"),
                P::IEJoin { .. } => f.write_str("IEJOIN"),
                P::Invalid => f.write_str("INVALID"),
                P::Join { how, .. } => write!(f, "{how} JOIN"),
                P::MapFunction { function } => write!(f, "{function}"),
                P::MergeSorted { maintain_order, .. } => {
                    write!(f, "MERGE SORTED[maintain_order: {maintain_order}]")
                },
                P::PythonMultiScan { n_scans, .. } => {
                    write!(f, "PYTHON MULTI SCAN[{n_scans} sources]")
                },
                P::PythonScan { .. } => f.write_str("PYTHON SCAN"),
                P::RemoveOverlap => f.write_str("REMOVE PARTITION OVERLAP"),
                P::Window { .. } => f.write_str("WINDOW"),
                P::RollingGroupBy {
                    agg_kind, period, ..
                } => write!(f, "{} ROLLING BY[period: {period}]", agg_name(agg_kind)),
                P::Scan {
                    scan_type,
                    num_sources,
                    ..
                } => write!(f, "{scan_type} SCAN[{num_sources} sources]"),
                P::Select { .. } => f.write_str("SELECT"),
                P::ShuffleRead {
                    shuffle_number,
                    partitioning,
                    is_local,
                    ..
                } => write!(
                    f,
                    "SHUFFLE READ ({shuffle_number})\n[partitioning: {partitioning}, is_local: {is_local}]"
                ),
                P::ShuffleWrite {
                    shuffle_number,
                    partitioning,
                    add_order_tag,
                    ..
                } => {
                    write!(f, "SHUFFLE WRITE ({shuffle_number})\n[")?;
                    if *add_order_tag {
                        write!(f, "add_order_tag: true, ")?;
                    }
                    write!(f, "partitioning: {partitioning}]")
                },
                P::SimpleProjection { .. } => f.write_str("simple π"),
                P::Sink { sink_type, .. } | P::Sink2 { sink_type, .. } => {
                    write!(f, "SINK ({sink_type})")
                },
                P::SinkMultiple { .. } => f.write_str("SINK_MULTIPLE"),
                P::Slice { offset, len } => write!(f, "SLICE[offset: {offset}, len: {len}]"),
                P::Sort { maintain_order, .. } => {
                    write!(f, "SORT[maintain_order: {maintain_order}]")
                },
                P::Union { maintain_order, .. } => {
                    write!(f, "UNION[maintain_order: {maintain_order}]")
                },
                P::UnoptimizedDispatch { operation, .. } => {
                    write!(f, "UNOPTIMIZED DISPATCH TO {operation}")
                },
            },
        }?;

        if let Some(subtitle) = &self.0.subtitle {
            write!(f, "\n{subtitle}")?;
        }

        Ok(())
    }
}

fn agg_name(agg_kind: &AggKind) -> &'static str {
    match agg_kind {
        AggKind::Aggs(_) => "AGGREGATE",
        AggKind::Apply => "MAP_GROUPS",
    }
}

/// Escapes a value for use inside a quoted Graphviz attribute.
struct Escaped<T>(T);

impl<T: Display> Display for Escaped<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(EscapeWriter(f), "{}", self.0)
    }
}

/// Escapes as it writes, so a multi-part label never has to be built up first.
struct EscapeWriter<'a, 'b>(&'a mut fmt::Formatter<'b>);

impl fmt::Write for EscapeWriter<'_, '_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let mut rest = s;
        while let Some(idx) = rest.find(['"', '\\', '\n']) {
            let (before, after) = rest.split_at(idx);
            self.0.write_str(before)?;
            self.0.write_str(match &after[..1] {
                "\"" => "\\\"",
                "\\" => "\\\\",
                // `\l` breaks the line and left-aligns the one before it.
                _ => "\\l",
            })?;
            rest = &after[1..];
        }
        self.0.write_str(rest)
    }
}

#[cfg(test)]
mod tests {
    use pc_observatory_types::StageNumber;

    use super::*;
    use crate::ir::IRVisualizationData;
    use crate::ir::models::{IRNodeInfo, IRNodeProperties, PartitioningModel};
    use crate::stages::{LogicalNodeKey, NodeSpec, PhysStageInfo, PhysStageProperties};

    /// Two stages joined by a shuffle: stage 0 reads and writes, stage 1 reads the
    /// shuffle back and sinks it.
    fn stage_graph() -> StageGraphVisualizationData {
        let node = |id, subtitle: Option<&str>, properties| IRNodeInfo {
            id,
            title: None,
            subtitle: subtitle.map(str::to_owned),
            properties,
        };
        let stage = |stage_number, nodes, edges| PhysStageInfo {
            stage_number,
            title: format!("stage-{stage_number}"),
            data: IRVisualizationData {
                title: format!("stage-{stage_number}"),
                num_roots: 1,
                nodes,
                edges,
            },
            properties: PhysStageProperties {},
            explain_ir: None,
        };

        StageGraphVisualizationData {
            title: String::new(),
            num_roots: 1,
            nodes: vec![
                stage(
                    0,
                    vec![
                        node(
                            1,
                            None,
                            IRNodeProperties::ShuffleWrite {
                                shuffle_number: 0,
                                partitioning: PartitioningModel::Hash {
                                    by: "col(\"category\")".to_owned(),
                                },
                                collect_samples_col: None,
                                add_order_tag: false,
                            },
                        ),
                        node(
                            2,
                            Some("category"),
                            IRNodeProperties::Select {
                                exprs: vec!["col(\"category\")".to_owned()],
                            },
                        ),
                    ],
                    // The shuffle write consumes the select.
                    vec![Edge {
                        source: 1,
                        target: 2,
                    }],
                ),
                stage(
                    1,
                    // Node keys restart per stage, so both stages have a node 1.
                    vec![node(
                        1,
                        None,
                        IRNodeProperties::ShuffleRead {
                            shuffle_number: 0,
                            partitioning: PartitioningModel::Partitioned,
                            is_local: false,
                            schema_names: vec!["category".to_owned()],
                        },
                    )],
                    vec![],
                ),
            ],
            edges: vec![StageEdge {
                source: NodeSpec {
                    stage: StageNumber(0),
                    node: LogicalNodeKey(1),
                },
                target: NodeSpec {
                    stage: StageNumber(1),
                    node: LogicalNodeKey(1),
                },
            }],
        }
    }

    #[test]
    fn each_stage_becomes_a_cluster() {
        let dot = stage_graph().display_dot().to_string();

        assert!(dot.starts_with("digraph stage_graph {\n"));
        assert!(dot.ends_with("}\n"));
        assert!(dot.contains("subgraph cluster_0 {"));
        assert!(dot.contains("subgraph cluster_1 {"));
        assert!(dot.contains("label=\"stage-0\""));
    }

    /// Node keys are only unique within a stage, so the ids have to be qualified or
    /// the two stages' nodes collapse into one.
    #[test]
    fn node_ids_are_scoped_to_their_stage() {
        let dot = stage_graph().display_dot().to_string();

        assert!(dot.contains("s0_n1 [label="));
        assert!(dot.contains("s1_n1 [label="));
    }

    /// `Edge` points from a node at its input, so the arrow has to be flipped to run
    /// source to sink like the data does.
    #[test]
    fn edges_follow_the_data() {
        let dot = stage_graph().display_dot().to_string();

        // Within a stage: the select feeds the shuffle write.
        assert!(dot.contains("s0_n2 -> s0_n1"));
        assert!(!dot.contains("s0_n1 -> s0_n2"));
        // Across stages: the shuffle write feeds the shuffle read.
        assert!(dot.contains("s0_n1 -> s1_n1"));
    }

    #[test]
    fn labels_carry_the_operator_and_its_subtitle() {
        let dot = stage_graph().display_dot().to_string();

        assert!(dot.contains(r#"s0_n2 [label="SELECT\lcategory"]"#));
        assert!(dot.contains("SHUFFLE READ (0)"));
    }

    /// Quotes inside an expression would otherwise end the label early and produce a
    /// document Graphviz cannot parse.
    #[test]
    fn quotes_in_labels_are_escaped() {
        let dot = stage_graph().display_dot().to_string();

        assert!(dot.contains(r#"partitioning: Hash col(\"category\")"#));
    }
}
