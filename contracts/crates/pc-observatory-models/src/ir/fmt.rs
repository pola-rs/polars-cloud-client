use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display, Write as _};

use crate::Edge;
use crate::fmt::{FmtStack, PrefixWrite, StackItem, display_trunc, iter_as_slice};
use crate::ir::IRVisualizationData;
use crate::ir::models::{AggKind, IRNodeProperties, PartitioningModel, Predicate};

impl IRVisualizationData {
    pub fn explain(&self) -> IRDisplay<'_> {
        let map: HashMap<u64, usize> = HashMap::from_iter(
            self.nodes
                .iter()
                .enumerate()
                .map(|(idx, node)| (node.id, idx)),
        );
        let mut inputs = vec![Vec::new(); self.nodes.len()];
        for Edge { source, target } in &self.edges {
            inputs[map[source]].push(map[target]);
        }
        IRDisplay { data: self, inputs }
    }
}

#[derive(Debug)]
pub struct IRDisplay<'a> {
    data: &'a IRVisualizationData,
    inputs: Vec<Vec<usize>>,
}

impl Display for IRDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n_roots = self.data.num_roots;
        let mut visited_caches: HashSet<usize> = HashSet::new();

        let mut stack = FmtStack::new(&self.inputs, n_roots);
        let mut f = PrefixWrite::new(f, 2);
        while let Some((item, level)) = stack.pop() {
            macro_rules! write_with_inputs {
                ($dst:expr, $($arg:tt),*) => {
                    stack.add([$(elem!($arg)),*].into_iter());
                    continue;
                };
                ($dst:expr, $($arg:tt),*,) => {
                    write_with_inputs!($dst, $($arg),*)
                };
            }
            macro_rules! elem {
                (($fmt:literal, $level:expr)) => {
                    (StackItem::String(format!($fmt).into()), $level)
                };
                ($fmt:literal) => {
                    elem!(($fmt, level))
                };
                (($exp:expr, $level:expr)) => {
                    (StackItem::Node($exp), $level)
                };
                ($node:expr) => {
                    (StackItem::Node($node), level)
                };
            }
            f.set_indent_level(level);
            let idx = match item {
                StackItem::Node(id) => id,
                StackItem::String(string) => {
                    f.write_str(&string)?;
                    continue;
                },
            };
            let node = &self.data.nodes[idx];
            match &node.properties {
                IRNodeProperties::Cache { .. } => {
                    write!(f, "CACHE[id: {idx}]")?;
                    if visited_caches.insert(idx) {
                        stack.add_sources(idx, level + 1);
                    }
                    continue;
                },
                IRNodeProperties::DataFrameScan { schema_names, .. } => {
                    write!(f, "DF {}", display_trunc(schema_names, 4))?;
                },
                IRNodeProperties::Distinct {
                    subset,
                    maintain_order,
                    keep_strategy,
                    ..
                } => {
                    write!(
                        f,
                        "UNIQUE[maintain_order: {maintain_order}, keep_strategy: {keep_strategy}]",
                    )?;
                    if let Some(cols) = subset {
                        write!(f, " BY {cols:?}")?;
                    }
                },
                IRNodeProperties::ExtContext { .. } => write!(f, "EXTERNAL_CONTEXT")?,
                IRNodeProperties::Filter { predicate } => {
                    write!(f, "FILTER {:?}\nFROM", predicate)?;
                },
                IRNodeProperties::Gather { null_on_oob } => {
                    write!(f, "GATHER[null_on_oob: {null_on_oob}]")?;
                },
                IRNodeProperties::GroupBy { keys, agg_kind, .. }
                | IRNodeProperties::DynamicGroupBy {
                    agg_kind,
                    group_by: keys,
                    ..
                }
                | IRNodeProperties::RollingGroupBy { keys, agg_kind, .. } => {
                    let maintain_order =
                        if let IRNodeProperties::GroupBy { maintain_order, .. } = node.properties {
                            maintain_order
                        } else {
                            true
                        };
                    writeln!(f, "AGGREGATE[maintain_order: {maintain_order}]")?;
                    let keys = expr_list(keys);
                    f.with_indent(level + 1, |f| {
                        match agg_kind {
                            AggKind::Aggs(items) => writeln!(f, "{} BY {keys}", expr_list(items))?,
                            AggKind::Apply => writeln!(f, "MAP_GROUPS BY {keys}")?,
                        };
                        write!(f, "FROM")
                    })?;
                },
                IRNodeProperties::HConcat { .. } => {
                    write!(f, "HCONCAT")?;
                    stack.add(
                        self.inputs[idx]
                            .iter()
                            .enumerate()
                            .flat_map(|(i, &idx)| {
                                [
                                    (StackItem::String(format!("PLAN {i}").into()), level + 1),
                                    (StackItem::Node(idx), level + 2),
                                ]
                            })
                            .chain(std::iter::once((
                                StackItem::String("END HCONCAT".into()),
                                level,
                            ))),
                    );
                    continue;
                },
                IRNodeProperties::HStack { exprs, .. } => {
                    write!(f, "WITH_COLUMNS:\n {}", expr_list(exprs))?;
                },
                IRNodeProperties::Invalid => write!(f, "INVALID")?,
                IRNodeProperties::Join {
                    how,
                    left_on,
                    right_on,
                    ..
                } => {
                    let left_on = expr_list(left_on);
                    let right_on = expr_list(right_on);
                    writeln!(f, "{how} JOIN:")?;
                    write_with_inputs!(
                        f,
                        "LEFT PLAN ON: {left_on}",
                        (self.inputs[idx][0], level + 1),
                        "RIGHT PLAN ON: {right_on}",
                        (self.inputs[idx][1], level + 1),
                        "END {how} JOIN"
                    );
                },
                IRNodeProperties::CrossJoin { predicate, .. } => {
                    write!(f, "NESTED LOOP JOIN")?;
                    if let Some(predicate) = predicate {
                        write!(f, "ON {predicate}:")?;
                    }
                    write_with_inputs!(
                        f,
                        "LEFT PLAN:",
                        (self.inputs[idx][0], level + 1),
                        "RIGHT PLAN:",
                        (self.inputs[idx][1], level + 1),
                        "END NESTED LOOP JOIN",
                    );
                },
                IRNodeProperties::MapFunction { function } => {
                    write!(f, "{function}")?;
                },
                IRNodeProperties::Scan {
                    scan_type,
                    num_sources,
                    first_source,
                    file_columns,
                    projection,
                    row_index_name,
                    row_index_offset,
                    pre_slice,
                    predicate,
                    ..
                } => {
                    fmt_scan(
                        &mut f,
                        scan_type,
                        Some((*num_sources, first_source.as_deref())),
                        file_columns.as_deref(),
                        projection.as_deref(),
                        row_index_name
                            .as_deref()
                            .map(|name| (name, *row_index_offset)),
                        pre_slice.as_ref(),
                        predicate.as_ref(),
                    )?;
                },
                IRNodeProperties::Select { exprs } => {
                    write!(f, "SELECT {}", expr_list(exprs))?;
                },
                IRNodeProperties::SimpleProjection { columns } => {
                    write!(f, "simple π [{columns:?}]")?;
                },
                IRNodeProperties::Sink { sink_type, .. }
                | IRNodeProperties::Sink2 { sink_type, .. } => {
                    write!(f, "SINK ({sink_type})")?;
                },
                IRNodeProperties::CallbackSink { .. } => {
                    write!(f, "SINK (Callback)")?;
                },
                IRNodeProperties::FlightSink { .. } => {
                    write!(f, "SINK (Flight)")?;
                },
                IRNodeProperties::SinkMultiple { .. } => {
                    write!(f, "SINK_MULTIPLE")?;
                },
                IRNodeProperties::Slice { offset, len } => {
                    write!(f, "SLICE[offset: {offset}, len: {len}]")?;
                },
                IRNodeProperties::Sort {
                    sort_columns,
                    slice,
                    maintain_order,
                    ..
                } => {
                    write!(f, "SORT BY ")?;

                    if slice.is_some()
                        || *maintain_order
                        || sort_columns.iter().any(|v| v.descending)
                        || sort_columns.iter().any(|v| v.nulls_last)
                    {
                        f.write_char('[')?;

                        let mut comma = false;
                        if let Some((o, l, dyn_pred)) = slice {
                            if let Some(dyn_pred) = &dyn_pred {
                                write!(f, "slice: ({o}, {l}, {dyn_pred:?})")?;
                            } else {
                                write!(f, "slice: ({o}, {l})")?;
                            }
                            comma = true;
                        }
                        if *maintain_order {
                            if comma {
                                f.write_str(", ")?;
                            }
                            f.write_str("maintain_order: true")?;
                            comma = true;
                        }
                        if sort_columns.iter().any(|v| v.descending) {
                            if comma {
                                f.write_str(", ")?;
                            }
                            write!(
                                f,
                                "descending: {}",
                                iter_as_slice(sort_columns.iter().map(|v| v.descending))
                            )?;
                            comma = true;
                        }

                        if sort_columns.iter().any(|v| v.nulls_last) {
                            if comma {
                                f.write_str(", ")?;
                            }
                            write!(
                                f,
                                "nulls_last: {}",
                                iter_as_slice(sort_columns.iter().map(|v| v.nulls_last))
                            )?;
                        }

                        f.write_str("] ")?;
                    }
                    write!(
                        f,
                        "{}",
                        iter_as_slice(sort_columns.iter().map(|col| &col.expr))
                    )?;
                },
                IRNodeProperties::Union {
                    maintain_order,
                    slice,
                    ..
                } => {
                    let name = fmt::from_fn(|f| {
                        if let Some(slice) = slice {
                            write!(
                                f,
                                "SLICED UNION[maintain_order: {maintain_order}]: {slice:?}"
                            )
                        } else {
                            write!(f, "UNION[maintain_order: {maintain_order}]")
                        }
                    });
                    write!(f, "{name}")?;
                    stack.add(
                        self.inputs[idx]
                            .iter()
                            .enumerate()
                            .flat_map(|(i, node)| {
                                [
                                    (StackItem::String(format!("PLAN {i}").into()), level + 1),
                                    (StackItem::Node(*node), level + 2),
                                ]
                            })
                            .chain(std::iter::once((
                                StackItem::String(format!("END {name}").into()),
                                level,
                            ))),
                    );
                    continue;
                },
                IRNodeProperties::AsOfJoin {
                    left_on, right_on, ..
                } => {
                    writeln!(f, "ASOF JOIN:")?;
                    write_with_inputs!(
                        f,
                        "LEFT PLAN ON: {left_on:?}",
                        (self.inputs[idx][0], level + 1),
                        "RIGHT PLAN ON: {right_on:?}",
                        (self.inputs[idx][1], level + 1),
                        "END ASOF JOIN"
                    );
                },
                IRNodeProperties::IEJoin {
                    left_on, right_on, ..
                } => {
                    writeln!(f, "IEJOIN:")?;
                    write_with_inputs!(
                        f,
                        "LEFT PLAN ON: {left_on:?}",
                        (self.inputs[idx][0], level + 1),
                        "RIGHT PLAN ON: {right_on:?}",
                        (self.inputs[idx][1], level + 1),
                        "END IEJOIN"
                    );
                },
                IRNodeProperties::MergeSorted {
                    keys,
                    maintain_order,
                } => {
                    writeln!(
                        f,
                        "MERGE SORTED[maintain_order: {maintain_order}] ON '{}':",
                        expr_list(keys)
                    )?;
                    write_with_inputs!(
                        f,
                        "LEFT PLAN:",
                        (self.inputs[idx][0], level + 1),
                        "RIGHT PLAN:",
                        (self.inputs[idx][1], level + 1),
                        "END MERGE_SORTED"
                    );
                },
                IRNodeProperties::PythonScan {
                    projection,
                    predicate,
                    schema_names,
                    ..
                } => fmt_scan(
                    &mut f,
                    "PYTHON",
                    None,
                    Some(schema_names),
                    projection.as_deref(),
                    None,
                    None,
                    predicate.as_ref(),
                )?,
                IRNodeProperties::PythonMultiScan { n_scans, .. } => fmt_scan(
                    &mut f,
                    "PYTHON MULTI",
                    Some((*n_scans, None)),
                    None,
                    None,
                    None,
                    None,
                    None,
                )?,
                IRNodeProperties::ShuffleRead {
                    shuffle_number,
                    partitioning,
                    is_local,
                    ..
                } => write!(
                    f,
                    "SHUFFLE READ ({shuffle_number}) [partitioning: {partitioning}, is_local: {is_local}]"
                )?,
                IRNodeProperties::ShuffleWrite {
                    shuffle_number,
                    partitioning,
                    ..
                } => {
                    write!(
                        f,
                        "SHUFFLE WRITE ({shuffle_number}) [partitioning: {partitioning}]"
                    )?;
                },
                IRNodeProperties::UnoptimizedDispatch { operation, .. } => {
                    write!(f, "UNOPTIMIZED DISPATCH TO {operation}")?;
                },
            }
            stack.add_sources(idx, level + 1);
        }
        Ok(())
    }
}

impl Display for PartitioningModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Partitioned => f.write_str("Partitioned"),
            Self::Local => f.write_str("Local"),
            Self::Single => f.write_str("Single"),
            Self::Broadcast => f.write_str("Broadcast"),
            Self::Hash { by } => write!(f, "Hash {by}"),
            Self::Range => f.write_str("Range"),
        }
    }
}

struct DisplayExprs<'a>(&'a [String]);

fn expr_list(exprs: &[String]) -> impl Display {
    DisplayExprs(exprs)
}

impl Display for DisplayExprs<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct DisplayStr<'a>(&'a str);

        impl std::fmt::Debug for DisplayStr<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.0)
            }
        }

        f.debug_list()
            .entries(self.0.iter().map(|expr| DisplayStr(expr.as_str())))
            .finish()
    }
}

#[expect(clippy::too_many_arguments)]
fn fmt_scan(
    f: &mut PrefixWrite<'_, fmt::Formatter<'_>>,
    scan_type: &str,
    sources: Option<(usize, Option<&str>)>,
    file_columns: Option<&[String]>,
    projection: Option<&[String]>,
    row_index: Option<(&str, Option<u64>)>,
    pre_slice: Option<&(i64, u64)>,
    predicate: Option<&Predicate>,
) -> Result<(), fmt::Error> {
    write!(f, "{scan_type} SCAN ")?;
    if let Some(sources) = sources {
        match sources {
            (0, _) => write!(f, "[]"),
            (1, Some(first_source)) => write!(f, "[{first_source}]"),
            (n, Some(first_source)) => {
                write!(f, "[{first_source:?}, ... {} other sources]", n - 1)
            },
            (n, None) => write!(f, "{n} sources"),
        }?;
    }
    writeln!(f)?;
    let total_cols = file_columns.as_ref().map_or(0, |v| v.len());
    match projection.as_ref() {
        Some(proj) => writeln!(f, "PROJECT {}/{total_cols} COLUMNS", proj.len())?,
        None => writeln!(f, "PROJECT */{total_cols} COLUMNS")?,
    }
    if let Some(predicate) = predicate {
        writeln!(f, "SELECTION: {predicate}")?;
    }
    if let Some(pre_slice) = pre_slice {
        writeln!(f, "SLICE: {pre_slice:?}")?;
    }
    if let Some((name, offset)) = row_index {
        write!(f, "ROW_INDEX: {}", name)?;
        if let Some(offset) = offset
            && offset != 0
        {
            write!(f, " (offset: {})", offset)?;
        }
    };
    Ok(())
}

impl Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        expr_list(&self.0).fmt(f)
    }
}
