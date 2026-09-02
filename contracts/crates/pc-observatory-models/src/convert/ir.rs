use std::collections::HashSet;

use polars_descriptions::{IrNodeDescription, IrPropsDescription};

use crate::Edge;
use crate::convert::{python_predicate, sink_from_dest, to_pred_skip, to_sort_columns};
use crate::ir::models::{AggKind, IRNodeProperties, Predicate};
use crate::ir::{IRNodeInfo, IRVisualizationData};

pub fn description_to_ir_visualization(nodes: Vec<IrNodeDescription>) -> IRVisualizationData {
    let mut edges = Vec::new();
    let mut referenced = HashSet::new();
    for node in &nodes {
        for input in &node.input_ids {
            edges.push(Edge::new(node.id, *input));
            referenced.insert(*input);
        }
    }
    let num_roots = nodes.len().saturating_sub(referenced.len());
    let nodes = nodes
        .into_iter()
        .map(|n| {
            let properties = to_ir_props(n.properties);
            let subtitle = super::subtitle::ir_subtitle(&properties);
            IRNodeInfo {
                id: n.id as u64,
                title: None,
                subtitle,
                properties,
            }
        })
        .collect();

    IRVisualizationData {
        title: String::new(),
        num_roots,
        nodes,
        edges,
    }
}

fn to_ir_props(props: IrPropsDescription) -> IRNodeProperties {
    match props {
        IrPropsDescription::Cache { id } => IRNodeProperties::Cache { id },
        IrPropsDescription::DataFrameScan {
            n_rows,
            schema_names,
        } => IRNodeProperties::DataFrameScan {
            n_rows,
            schema_names,
        },
        IrPropsDescription::Distinct {
            subset,
            maintain_order,
            keep_strategy,
            slice,
        } => IRNodeProperties::Distinct {
            subset,
            maintain_order,
            keep_strategy,
            slice,
        },
        IrPropsDescription::ExtContext {
            num_contexts,
            schema_names,
        } => IRNodeProperties::ExtContext {
            num_contexts,
            schema_names,
        },
        IrPropsDescription::Filter { predicate } => IRNodeProperties::Filter {
            predicate: Predicate(predicate),
        },
        IrPropsDescription::Gather { null_on_oob } => IRNodeProperties::Gather { null_on_oob },
        IrPropsDescription::GroupBy {
            keys,
            aggs,
            maintain_order,
            slice,
        } => IRNodeProperties::GroupBy {
            keys,
            agg_kind: AggKind::Aggs(aggs),
            maintain_order,
            slice,
        },
        IrPropsDescription::HConcat {
            num_inputs,
            schema_names,
            strict,
        } => IRNodeProperties::HConcat {
            num_inputs,
            schema_names,
            strict,
        },
        IrPropsDescription::HStack {
            exprs,
            should_broadcast,
        } => IRNodeProperties::HStack {
            exprs,
            should_broadcast,
        },
        IrPropsDescription::Invalid | IrPropsDescription::Other => IRNodeProperties::Invalid,
        IrPropsDescription::Join {
            how,
            left_on,
            right_on,
            nulls_equal,
            coalesce,
            maintain_order,
            validation,
            suffix,
            slice,
        } => IRNodeProperties::Join {
            how,
            left_on,
            right_on,
            nulls_equal,
            coalesce,
            maintain_order,
            validation,
            suffix,
            slice,
        },
        IrPropsDescription::CrossJoin {
            maintain_order,
            slice,
            predicate,
            suffix,
        } => IRNodeProperties::CrossJoin {
            maintain_order,
            slice,
            predicate: predicate.map(Predicate),
            suffix,
        },
        IrPropsDescription::MapFunction { function } => IRNodeProperties::MapFunction { function },
        IrPropsDescription::Scan {
            scan_type,
            num_sources,
            first_source,
            file_columns,
            projection,
            row_index_name,
            row_index_offset,
            pre_slice,
            predicate,
            predicate_file_skip_applied,
            has_table_statistics,
            include_file_paths,
            column_mapping_type,
            hive_columns,
        } => IRNodeProperties::Scan {
            scan_type,
            num_sources,
            first_source,
            file_columns,
            projection,
            row_index_name,
            row_index_offset,
            pre_slice,
            predicate: predicate.map(Predicate),
            predicate_file_skip_applied: predicate_file_skip_applied.map(to_pred_skip),
            has_table_statistics,
            include_file_paths,
            column_mapping_type,
            hive_columns,
        },
        IrPropsDescription::Select { exprs } => IRNodeProperties::Select { exprs },
        IrPropsDescription::SimpleProjection { columns } => {
            IRNodeProperties::SimpleProjection { columns }
        },
        IrPropsDescription::Sink { dest } => {
            let (sink_type, file_format, location) = sink_from_dest(dest);
            IRNodeProperties::Sink {
                sink_type,
                file_format,
                location,
            }
        },
        IrPropsDescription::SinkMultiple { num_inputs } => {
            IRNodeProperties::SinkMultiple { num_inputs }
        },
        IrPropsDescription::Slice { offset, len } => IRNodeProperties::Slice { offset, len },
        IrPropsDescription::Sort {
            sort_columns,
            slice,
            maintain_order,
            limit,
        } => IRNodeProperties::Sort {
            sort_columns: to_sort_columns(sort_columns),
            slice,
            maintain_order,
            limit,
        },
        IrPropsDescription::Union {
            num_inputs,
            maintain_order,
            slice,
        } => IRNodeProperties::Union {
            num_inputs,
            maintain_order,
            slice,
        },
        IrPropsDescription::AsOfJoin {
            left_on,
            right_on,
            left_by,
            right_by,
            strategy,
            tolerance,
            suffix,
            slice,
            coalesce,
            allow_eq,
            check_sortedness,
        } => IRNodeProperties::AsOfJoin {
            left_on,
            right_on,
            left_by,
            right_by,
            strategy,
            tolerance,
            suffix,
            slice,
            coalesce,
            allow_eq,
            check_sortedness,
        },
        IrPropsDescription::IEJoin {
            left_on,
            right_on,
            inequality_operators,
            suffix,
            slice,
        } => IRNodeProperties::IEJoin {
            left_on,
            right_on,
            inequality_operators,
            suffix,
            slice,
        },
        IrPropsDescription::DynamicGroupBy {
            index_column,
            aggs,
            every,
            period,
            offset,
            label,
            include_boundaries,
            closed_window,
            group_by,
            start_by,
        } => IRNodeProperties::DynamicGroupBy {
            index_column,
            agg_kind: AggKind::Aggs(aggs),
            every,
            period,
            offset,
            label,
            include_boundaries,
            closed_window,
            group_by,
            start_by,
        },
        IrPropsDescription::RollingGroupBy {
            keys,
            aggs,
            index_column,
            period,
            offset,
            closed_window,
            slice,
        } => IRNodeProperties::RollingGroupBy {
            keys,
            agg_kind: AggKind::Aggs(aggs),
            index_column,
            period,
            offset,
            closed_window,
            slice,
        },
        IrPropsDescription::MergeSorted {
            keys,
            maintain_order,
        } => IRNodeProperties::MergeSorted {
            keys,
            maintain_order,
        },
        IrPropsDescription::PythonScan {
            scan_source_type,
            n_rows,
            projection,
            predicate,
            schema_names,
            is_pure,
            validate_schema,
        } => IRNodeProperties::PythonScan {
            scan_source_type,
            n_rows,
            projection,
            predicate: python_predicate(predicate).map(|p| Predicate(vec![p])),
            schema_names,
            is_pure,
            validate_schema,
        },
        IrPropsDescription::UnoptimizedDispatch {
            num_inputs,
            operation,
        } => IRNodeProperties::UnoptimizedDispatch {
            num_inputs,
            operation,
        },
    }
}
