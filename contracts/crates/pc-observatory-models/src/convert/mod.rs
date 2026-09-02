mod ir;
mod physical;
mod subtitle;

pub use ir::description_to_ir_visualization;
pub use physical::description_to_physical_visualization;
use polars_descriptions::{
    FileProviderDescription, InequalityOperatorDescription, PredicateFileSkipDescription,
    PythonPredicateDescription, SinkDestDescription, SortColumnDescription,
};

use crate::SortColumn;
use crate::ir::models::PredicateFileSkip;
use crate::phys::models::PhysNodeProperties;
use crate::phys::warning::PhysNodeWarning;

pub(crate) fn to_sort_columns(by: Vec<SortColumnDescription>) -> Vec<SortColumn> {
    by.into_iter()
        .map(|c| SortColumn {
            expr: c.expr,
            descending: c.descending,
            nulls_last: c.nulls_last,
        })
        .collect()
}

pub(crate) fn to_pred_skip(p: PredicateFileSkipDescription) -> PredicateFileSkip {
    PredicateFileSkip {
        no_residual_predicate: p.no_residual_predicate,
        original_len: p.original_len,
    }
}

pub(crate) fn warnings_for(props: &PhysNodeProperties) -> Vec<PhysNodeWarning> {
    use PhysNodeProperties as P;
    let warning = match props {
        P::InMemoryMap { .. }
        | P::InMemoryJoin { .. }
        | P::InMemoryAsOfJoin { .. }
        | P::InMemoryIEJoin { .. } => Some(PhysNodeWarning::InMemoryFallback),
        P::InMemorySource { .. }
        | P::InputIndependentSelect { .. }
        | P::NegativeSlice { .. }
        | P::InMemorySink
        | P::Sort { .. }
        | P::GroupBy { .. }
        | P::EquiJoin { .. }
        | P::SemiAntiJoin { .. }
        | P::Multiplexer
        | P::MergeSorted { .. } => Some(PhysNodeWarning::MemoryIntensive),
        _ => None,
    };
    warning.into_iter().collect()
}

/// Interpret a raw sink destination into `(sink_type, file_format, location)`.
pub(crate) fn sink_from_dest(
    dest: SinkDestDescription,
) -> (String, Option<String>, Option<String>) {
    match dest {
        SinkDestDescription::Memory => ("Memory".to_string(), None, None),
        SinkDestDescription::Callback => ("Callback".to_string(), None, None),
        SinkDestDescription::File {
            file_format,
            target,
        } => ("File".to_string(), Some(file_format), Some(target)),
        SinkDestDescription::Partitioned {
            file_format,
            base_path,
        } => (
            "Partitioned".to_string(),
            Some(file_format),
            Some(base_path),
        ),
    }
}

pub(crate) fn file_provider_str(f: FileProviderDescription) -> String {
    match f {
        FileProviderDescription::Hive { extension } => extension,
        FileProviderDescription::Function => "Function".to_string(),
        FileProviderDescription::Iceberg => "Iceberg".to_string(),
    }
}

pub(crate) fn ineq_str(o: InequalityOperatorDescription) -> String {
    match o {
        InequalityOperatorDescription::Lt => "LessThan",
        InequalityOperatorDescription::LtEq => "LessThanOrEqualTo",
        InequalityOperatorDescription::Gt => "GreaterThan",
        InequalityOperatorDescription::GtEq => "GreaterThanOrEqualTo",
    }
    .to_string()
}

pub(crate) fn python_predicate(p: PythonPredicateDescription) -> Option<String> {
    match p {
        PythonPredicateDescription::None => None,
        PythonPredicateDescription::PyArrow {
            predicate,
            has_residual,
        } => Some(format!(
            "predicate: {predicate}, has_residual: {has_residual}"
        )),
        PythonPredicateDescription::Polars { predicate } => Some(predicate),
    }
}
