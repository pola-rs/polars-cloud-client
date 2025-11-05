#[cfg(feature = "server")]
use std::sync::LazyLock;

#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "server")]
use regex::Regex;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::ToSchema;
use uuid::Uuid;

use crate::EntityOrdering;

#[cfg(feature = "server")]
static COLOR_HEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^#(?:[A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$").unwrap());

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct LabelSchema {
    /// Label name
    #[cfg_attr(feature = "server", garde(length(min = 1, max = 32)))]
    pub name: String,
    /// Label description
    #[cfg_attr(feature = "server", garde(length(max = 512)))]
    pub description: Option<String>,
    /// Label color (most likely a HEX value (eg. #0075ff))
    #[cfg_attr(feature = "server", garde(pattern(COLOR_HEX)))]
    pub color: String,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct LabelOutputSchema {
    pub id: Uuid,
    pub workspace_id: Uuid,
    /// Label name
    pub name: String,
    /// Label description
    pub description: Option<String>,
    /// Label color
    pub color: String,
}
impl EntityOrdering for LabelOutputSchema {
    fn order_fields() -> &'static [&'static str] {
        &["id", "name"]
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct LabelUpdateSchema {
    /// Label name
    #[cfg_attr(feature = "server", garde(length(min = 1, max = 32)))]
    pub name: Option<String>,
    /// Label description
    #[cfg_attr(feature = "server", garde(length(max = 512)))]
    pub description: Option<String>,
    /// Label color
    #[cfg_attr(feature = "server", garde(pattern(COLOR_HEX)))]
    pub color: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct LabelIdSchema {
    /// Label identifier
    #[cfg_attr(feature = "server", garde(skip))]
    pub label_id: Uuid,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct ComputeClusterLabelSchema {
    /// Unique identifier
    pub id: Uuid,
    /// Unique identifier of the compute cluster
    pub cluster_id: Uuid,
    /// Unique identifier of the label
    pub label_id: Uuid,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct QueryLabelSchema {
    /// Unique identifier
    pub id: Uuid,
    /// Unique identifier of the query
    pub query_id: Uuid,
    /// Unique identifier of the label
    pub label_id: Uuid,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct ManifestLabelSchema {
    /// Unique identifier
    pub id: Uuid,
    /// Unique identifier of the manifest
    pub manifest_id: Uuid,
    /// Unique identifier of the label
    pub label_id: Uuid,
}
