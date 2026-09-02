use std::collections::HashSet;
use std::num::NonZeroUsize;

use polars_descriptions::{PhysicalNodeDescription, PhysicalPropsDescription};
use uuid::Uuid;

use crate::Edge;
use crate::convert::{
    file_provider_str, ineq_str, python_predicate, to_pred_skip, to_sort_columns, warnings_for,
};
use crate::phys::models::PhysNodeProperties;
use crate::phys::{PhysNodeInfo, PhysicalPlanVisualizationData};

pub fn description_to_physical_visualization(
    nodes: Vec<PhysicalNodeDescription>,
) -> PhysicalPlanVisualizationData {
    let mut edges = Vec::new();
    let mut referenced = HashSet::new();
    for node in &nodes {
        for input in &node.input_ids {
            edges.push(Edge::new(node.id, *input));
            referenced.insert(*input);
        }
    }
    let num_roots = nodes.len().saturating_sub(referenced.len()) as u64;
    let nodes = nodes
        .into_iter()
        .map(|n| {
            let properties = to_phys_props(n.properties);
            let warnings = warnings_for(&properties);
            let subtitle = super::subtitle::phys_subtitle(&properties);
            PhysNodeInfo {
                id: n.id,
                type_id: properties.variant_name(),
                title: None,
                subtitle,
                properties,
                warnings,
            }
        })
        .collect();

    PhysicalPlanVisualizationData {
        title: String::new(),
        num_roots,
        nodes,
        edges,
    }
}

/// Split a shuffle storage path into a worker-invariant display path and the
/// shuffle id.
///
/// Returns `None` for paths that are not shuffle storage, e.g. a user sink
/// target or a plain file scan.
fn shuffle_path_parts(path: &str) -> Option<(String, u32)> {
    let (before, after) = path.split_once("/shuffle=")?;

    // Require a real query id rather than any `query=` text, so a user sink
    // target that happens to contain these segments is not mistaken for shuffle
    // storage.
    let query_id = before.rsplit_once("query=")?.1;
    Uuid::parse_str(query_id).ok()?;

    let number = after.split('/').next()?;
    let shuffle_id = number.parse::<u32>().ok()?;
    Some((format!("query={query_id}/shuffle={number}"), shuffle_id))
}

fn to_phys_props(props: PhysicalPropsDescription) -> PhysNodeProperties {
    match props {
        PhysicalPropsDescription::Default | PhysicalPropsDescription::Other => {
            PhysNodeProperties::Default
        },
        PhysicalPropsDescription::CallbackSink {
            maintain_order,
            chunk_size,
        } => PhysNodeProperties::CallbackSink {
            maintain_order,
            chunk_size: chunk_size.and_then(NonZeroUsize::new),
        },
        PhysicalPropsDescription::DynamicSlice => PhysNodeProperties::DynamicSlice,
        PhysicalPropsDescription::FileSink {
            target,
            file_format,
            maintain_order,
        } => {
            let (target, shuffle_id) = match shuffle_path_parts(&target) {
                Some((path, id)) => (path, Some(id)),
                None => (target, None),
            };
            PhysNodeProperties::FileSink {
                target,
                file_format,
                maintain_order,
                shuffle_id,
            }
        },
        PhysicalPropsDescription::Filter { predicate } => PhysNodeProperties::Filter { predicate },
        PhysicalPropsDescription::GatherEvery { n, offset } => {
            PhysNodeProperties::GatherEvery { n, offset }
        },
        PhysicalPropsDescription::GroupBy {
            num_inputs,
            key_per_input,
            aggs_per_input,
        } => PhysNodeProperties::GroupBy {
            num_inputs,
            key_per_input,
            aggs_per_input,
        },
        PhysicalPropsDescription::DynamicGroupBy {
            index_column,
            period,
            every,
            offset,
            start_by,
            label,
            include_boundaries,
            closed_window,
            aggs,
            slice,
        } => PhysNodeProperties::DynamicGroupBy {
            index_column,
            period,
            every,
            offset,
            start_by,
            label,
            include_boundaries,
            closed_window,
            aggs,
            slice,
        },
        PhysicalPropsDescription::RollingGroupBy {
            index_column,
            period,
            offset,
            closed_window,
            slice,
            aggs,
        } => PhysNodeProperties::RollingGroupBy {
            index_column,
            period,
            offset,
            closed_window,
            slice,
            aggs,
        },
        PhysicalPropsDescription::SortedGroupBy { key, aggs, slice } => {
            PhysNodeProperties::SortedGroupBy { key, aggs, slice }
        },
        PhysicalPropsDescription::InMemoryMap { format_str } => {
            PhysNodeProperties::InMemoryMap { format_str }
        },
        PhysicalPropsDescription::InMemorySink => PhysNodeProperties::InMemorySink,
        PhysicalPropsDescription::InMemorySource {
            n_rows,
            schema_names,
        } => PhysNodeProperties::InMemorySource {
            n_rows,
            schema_names,
        },
        PhysicalPropsDescription::InputIndependentSelect { selectors } => {
            PhysNodeProperties::InputIndependentSelect { selectors }
        },
        PhysicalPropsDescription::AsOfJoin {
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
        } => PhysNodeProperties::AsOfJoin {
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
        PhysicalPropsDescription::RangeJoin {
            left_on,
            right_on,
            suffix,
            slice,
            coalesce,
            descending,
        } => PhysNodeProperties::RangeJoin {
            left_on,
            right_on,
            suffix,
            slice,
            coalesce,
            descending,
        },
        PhysicalPropsDescription::CrossJoin {
            maintain_order,
            suffix,
        } => PhysNodeProperties::CrossJoin {
            maintain_order,
            suffix,
        },
        PhysicalPropsDescription::EquiJoin {
            how,
            left_on,
            right_on,
            nulls_equal,
            coalesce,
            maintain_order,
            validation,
            suffix,
        } => PhysNodeProperties::EquiJoin {
            how,
            left_on,
            right_on,
            nulls_equal,
            coalesce,
            maintain_order,
            validation,
            suffix,
        },
        PhysicalPropsDescription::MergeJoin {
            how,
            left_on,
            right_on,
            keys_row_encoded,
            descending,
            nulls_last,
            nulls_equal,
            coalesce,
            maintain_order,
            validation,
            suffix,
        } => PhysNodeProperties::MergeJoin {
            how,
            left_on,
            right_on,
            keys_row_encoded,
            descending,
            nulls_last,
            nulls_equal,
            coalesce,
            maintain_order,
            validation,
            suffix,
        },
        PhysicalPropsDescription::InMemoryJoin {
            how,
            left_on,
            right_on,
            nulls_equal,
            coalesce,
            maintain_order,
            validation,
            suffix,
            slice,
        } => PhysNodeProperties::InMemoryJoin {
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
        PhysicalPropsDescription::InMemoryAsOfJoin {
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
        } => PhysNodeProperties::InMemoryAsOfJoin {
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
        PhysicalPropsDescription::InMemoryIEJoin {
            left_on,
            right_on,
            inequality_operators,
            suffix,
            slice,
        } => PhysNodeProperties::InMemoryIEJoin {
            left_on,
            right_on,
            inequality_operators: inequality_operators.into_iter().map(ineq_str).collect(),
            suffix,
            slice,
        },
        PhysicalPropsDescription::Map => PhysNodeProperties::Map,
        PhysicalPropsDescription::MultiScan {
            scan_type,
            num_sources,
            first_source,
            projected_file_columns,
            row_index_name,
            row_index_offset,
            // Ignore pre_slice because this is different per-worker
            pre_slice: _,
            predicate,
            predicate_file_skip_applied,
            has_table_statistics,
            include_file_paths,
            deletion_files_type,
            hive_columns,
        } => {
            let (first_source, shuffle_id) =
                match first_source.as_deref().and_then(shuffle_path_parts) {
                    Some((path, id)) => (Some(path), Some(id)),
                    None => (first_source, None),
                };
            PhysNodeProperties::MultiScan {
                scan_type,
                num_sources,
                first_source,
                projected_file_columns,
                row_index_name,
                row_index_offset,
                predicate,
                predicate_file_skip_applied: predicate_file_skip_applied.map(to_pred_skip),
                has_table_statistics,
                include_file_paths,
                deletion_files_type,
                hive_columns,
                shuffle_id,
            }
        },
        PhysicalPropsDescription::Multiplexer => PhysNodeProperties::Multiplexer,
        PhysicalPropsDescription::NegativeSlice { offset, length } => {
            PhysNodeProperties::NegativeSlice { offset, length }
        },
        PhysicalPropsDescription::OrderedUnion { num_inputs } => {
            PhysNodeProperties::OrderedUnion { num_inputs }
        },
        PhysicalPropsDescription::UnorderedUnion { num_inputs } => {
            PhysNodeProperties::UnorderedUnion { num_inputs }
        },
        PhysicalPropsDescription::PartitionSink {
            base_path,
            file_path_provider,
            file_format,
            partition_strategy,
            partition_key_exprs,
            include_keys,
            maintain_order,
            max_rows_per_file,
            approximate_bytes_per_file,
        } => {
            let (base_path, shuffle_id) = match shuffle_path_parts(&base_path) {
                Some((path, id)) => (path, Some(id)),
                None => (base_path, None),
            };
            PhysNodeProperties::PartitionSink {
                base_path,
                file_path_provider: file_provider_str(file_path_provider),
                file_format,
                partition_strategy,
                partition_key_exprs,
                include_keys,
                maintain_order,
                max_rows_per_file,
                approximate_bytes_per_file,
                shuffle_id,
            }
        },
        PhysicalPropsDescription::PeakMin => PhysNodeProperties::PeakMin,
        PhysicalPropsDescription::PeakMax => PhysNodeProperties::PeakMax,
        PhysicalPropsDescription::Reduce { exprs } => PhysNodeProperties::Reduce { exprs },
        PhysicalPropsDescription::Repeat => PhysNodeProperties::Repeat,
        PhysicalPropsDescription::Rle => PhysNodeProperties::Rle,
        PhysicalPropsDescription::RleId => PhysNodeProperties::RleId,
        PhysicalPropsDescription::Select {
            selectors,
            extend_original,
        } => PhysNodeProperties::Select {
            selectors,
            extend_original,
        },
        PhysicalPropsDescription::Shift { has_fill } => PhysNodeProperties::Shift { has_fill },
        PhysicalPropsDescription::ForwardFill { limit } => {
            PhysNodeProperties::ForwardFill { limit }
        },
        PhysicalPropsDescription::BackwardFill { limit } => {
            PhysNodeProperties::BackwardFill { limit }
        },
        PhysicalPropsDescription::SimpleProjection { columns } => {
            PhysNodeProperties::SimpleProjection { columns }
        },
        PhysicalPropsDescription::SinkMultiple { num_sinks } => {
            PhysNodeProperties::SinkMultiple { num_sinks }
        },
        PhysicalPropsDescription::Sort {
            sort_columns,
            slice,
            multithreaded,
            maintain_order,
            limit,
        } => PhysNodeProperties::Sort {
            sort_columns: to_sort_columns(sort_columns),
            slice,
            multithreaded,
            maintain_order,
            limit,
        },
        PhysicalPropsDescription::SortedUnique { keys } => {
            PhysNodeProperties::SortedUnique { keys }
        },
        PhysicalPropsDescription::IsFirstDistinct { keys } => {
            PhysNodeProperties::IsFirstDistinct { keys }
        },
        PhysicalPropsDescription::Slice { offset, length } => {
            PhysNodeProperties::Slice { offset, length }
        },
        PhysicalPropsDescription::TopK {
            by_exprs,
            reverse,
            nulls_last,
            dyn_pred,
        } => PhysNodeProperties::TopK {
            by_exprs,
            reverse,
            nulls_last,
            dyn_pred,
        },
        PhysicalPropsDescription::WithRowIndex { name, offset } => {
            PhysNodeProperties::WithRowIndex { name, offset }
        },
        PhysicalPropsDescription::Zip {
            num_inputs,
            zip_behavior,
        } => PhysNodeProperties::Zip {
            num_inputs,
            zip_behavior,
        },
        PhysicalPropsDescription::CumAgg { kind } => PhysNodeProperties::CumAgg { kind },
        PhysicalPropsDescription::Ewm {
            variant,
            alpha,
            adjust,
            bias,
            min_periods,
            ignore_nulls,
        } => PhysNodeProperties::Ewm {
            variant,
            alpha,
            adjust,
            bias,
            min_periods,
            ignore_nulls,
        },
        PhysicalPropsDescription::SemiAntiJoin {
            left_on,
            right_on,
            nulls_equal,
            output_as_bool,
        } => PhysNodeProperties::SemiAntiJoin {
            left_on,
            right_on,
            nulls_equal,
            output_as_bool,
        },
        PhysicalPropsDescription::MergeSorted { maintain_order } => {
            PhysNodeProperties::MergeSorted { maintain_order }
        },
        PhysicalPropsDescription::PythonScan {
            scan_source_type,
            n_rows,
            projection,
            predicate,
            schema_names,
            is_pure,
            validate_schema,
        } => PhysNodeProperties::PythonScan {
            scan_source_type,
            n_rows,
            projection,
            predicate: python_predicate(predicate),
            schema_names,
            is_pure,
            validate_schema,
        },
        PhysicalPropsDescription::StrptimeInfer {
            format,
            strict,
            exact,
        } => PhysNodeProperties::StrptimeInfer {
            format,
            strict,
            exact,
        },
        PhysicalPropsDescription::Interpolate { method } => {
            PhysNodeProperties::Interpolate { method }
        },
        PhysicalPropsDescription::Gather { null_on_oob } => {
            PhysNodeProperties::Gather { null_on_oob }
        },
        PhysicalPropsDescription::ColumnarFunction { num_inputs, name } => {
            PhysNodeProperties::ColumnarFunction { num_inputs, name }
        },
        PhysicalPropsDescription::IsSorted {
            descending,
            nulls_last,
            output_name,
        } => PhysNodeProperties::IsSorted {
            descending,
            nulls_last,
            output_name,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUERY: &str = "01a01f13-6f1c-7ba3-bd88-4ac3f4d71617";

    #[test]
    fn shuffle_read_path_is_trimmed_to_the_worker_invariant_part() {
        // A reader's path carries the producer and consumer numbers.
        let path = format!("/tmp/wd/worker1/query={QUERY}/shuffle=6/3/2");
        assert_eq!(
            shuffle_path_parts(&path),
            Some((format!("query={QUERY}/shuffle=6"), 6))
        );
    }

    #[test]
    fn shuffle_write_base_path_is_trimmed_to_the_worker_invariant_part() {
        // A partition sink's base path carries the worker identifier.
        let path = format!("s3://bucket/worker_3/query={QUERY}/shuffle=0");
        assert_eq!(
            shuffle_path_parts(&path),
            Some((format!("query={QUERY}/shuffle=0"), 0))
        );
    }

    #[test]
    fn every_worker_reports_the_same_path_and_id() {
        let first = format!("/tmp/a/worker1/query={QUERY}/shuffle=2/0/1");
        let second = format!("/tmp/b/worker4/query={QUERY}/shuffle=2/3/1");
        assert_eq!(shuffle_path_parts(&first), shuffle_path_parts(&second));
    }

    #[test]
    fn non_shuffle_paths_are_not_shuffles() {
        // A user sink and a plain file scan must keep their real paths.
        assert_eq!(shuffle_path_parts("/tmp/out/1.parquet"), None);
        assert_eq!(shuffle_path_parts("s3://bucket/data/part-0.parquet"), None);
        // Only a real query id counts, so lookalike user paths are ignored --
        // including one shaped like a uuid but not made of hex digits.
        assert_eq!(shuffle_path_parts("/data/query=latest/shuffle=1"), None);
        assert_eq!(
            shuffle_path_parts("/data/query=zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz/shuffle=1"),
            None
        );
        // A shuffle number we cannot parse is not usable as an id.
        assert_eq!(
            shuffle_path_parts(&format!("/tmp/query={QUERY}/shuffle=abc")),
            None
        );
    }
}
