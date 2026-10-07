use serde::{Deserialize, Deserializer};

/// A plan read back from storage, whose JSON may predate the current model.
///
/// Decoding one never fails: a plan the model rejects is kept as the
/// `serde_json` error that rejected it, so callers reading a row that holds one
/// can still use the rest of it.
#[derive(Debug)]
pub struct StoredPlan<T>(pub Result<T, serde_json::Error>);

impl<T> StoredPlan<T>
where
    T: serde::de::DeserializeOwned,
{
    pub fn from_value(value: serde_json::Value) -> Self {
        Self(serde_json::from_value(value))
    }
}

impl<'de, T> Deserialize<'de> for StoredPlan<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Ok(Self(T::deserialize(value)))
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;
    use crate::ir::IRVisualizationData;

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct Plan {
        title: String,
        num_roots: usize,
    }

    fn decode<T: serde::de::DeserializeOwned>(json: &str) -> StoredPlan<T> {
        serde_json::from_str::<StoredPlan<T>>(json).unwrap()
    }

    #[test]
    fn decodes_a_matching_plan() {
        let json = r#"{"title": "q", "num_roots": 1}"#;
        assert_eq!(
            decode::<Plan>(json).0.ok(),
            Some(Plan {
                title: "q".to_string(),
                num_roots: 1,
            })
        );
    }

    #[test]
    fn reports_a_plan_with_a_missing_field_as_unsupported() {
        let error = decode::<Plan>(r#"{"title": "q"}"#).0.unwrap_err();
        assert!(
            error.to_string().contains("num_roots"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn reports_a_plan_with_a_retyped_field_as_unsupported() {
        let plan = decode::<Plan>(r#"{"title": "q", "num_roots": "one"}"#);
        assert!(plan.0.is_err());
    }

    #[test]
    fn reports_a_plan_of_an_unrelated_shape_as_unsupported() {
        let json = r#"{"nodes": [{"id": 0, "kind": "AncientScan"}]}"#;
        let plan = decode::<IRVisualizationData>(json);
        assert!(plan.0.is_err());
    }

    #[test]
    fn keeps_a_current_ir_plan() {
        let json = r#"{"title": "", "num_roots": 1, "nodes": [], "edges": []}"#;
        let plan = decode::<IRVisualizationData>(json);
        assert!(plan.0.is_ok());
    }

    #[test]
    fn still_fails_on_invalid_json() {
        assert!(serde_json::from_str::<StoredPlan<Plan>>("{").is_err());
    }
}
