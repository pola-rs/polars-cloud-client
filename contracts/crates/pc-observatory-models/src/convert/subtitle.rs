use crate::ir::models::IRNodeProperties;
use crate::phys::models::PhysNodeProperties;
use crate::subtitle::{truncate_elements_lazy, truncate_subtitle};

pub(super) fn ir_subtitle(props: &IRNodeProperties) -> Option<String> {
    use IRNodeProperties as P;
    match props {
        P::SimpleProjection { columns } => truncate_join(columns),
        P::Select { exprs } => truncate_join(exprs),
        P::GroupBy { keys, .. } | P::RollingGroupBy { keys, .. } => truncate_join(keys),
        P::DynamicGroupBy { group_by, .. } => truncate_join(group_by),
        P::Sort { sort_columns, .. } => truncate_join(sort_columns.iter().map(|c| &c.expr)),
        P::Join {
            left_on, right_on, ..
        }
        | P::IEJoin {
            left_on, right_on, ..
        } => join_subtitle(left_on, right_on),
        P::Filter { predicate } => truncate_join(&predicate.0),
        P::Distinct {
            subset: Some(subset),
            ..
        } => truncate_join(subset),
        P::Scan {
            first_source: Some(source),
            ..
        } => truncate_subtitle(source),
        _ => None,
    }
}

pub(super) fn phys_subtitle(props: &PhysNodeProperties) -> Option<String> {
    use PhysNodeProperties as P;
    match props {
        P::SimpleProjection { columns } => truncate_join(columns),
        P::Select { selectors, .. } | P::InputIndependentSelect { selectors } => {
            truncate_join(selectors)
        },
        P::Reduce { exprs } => truncate_join(exprs),
        P::GroupBy { key_per_input, .. } => key_per_input.first().and_then(truncate_join),
        P::SortedGroupBy { key, .. } => Some(key.clone()),
        P::DynamicGroupBy { index_column, .. } | P::RollingGroupBy { index_column, .. } => {
            Some(index_column.clone())
        },
        P::Sort { sort_columns, .. } => truncate_join(sort_columns.iter().map(|c| &c.expr)),
        P::TopK { by_exprs, .. } => truncate_join(by_exprs),
        P::EquiJoin {
            left_on, right_on, ..
        }
        | P::InMemoryJoin {
            left_on, right_on, ..
        }
        | P::MergeJoin {
            left_on, right_on, ..
        }
        | P::SemiAntiJoin {
            left_on, right_on, ..
        } => join_subtitle(left_on, right_on),
        P::Filter { predicate } => truncate_subtitle(predicate),
        _ => None,
    }
}

fn truncate_join<S: AsRef<str>>(elems: impl IntoIterator<Item = S>) -> Option<String> {
    truncate_elements_lazy(elems.into_iter(), |s| s.as_ref().to_string())
}

fn join_subtitle(left: &[String], right: &[String]) -> Option<String> {
    truncate_elements_lazy(left.iter().zip(right.iter()), |(l, r)| {
        if l == r {
            l.clone()
        } else {
            format!("{} = {}", l, r)
        }
    })
}
