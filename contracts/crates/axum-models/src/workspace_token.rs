use chrono::{DateTime, Utc};
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::ToSchema;
use uuid::Uuid;

use crate::EntityOrdering;
#[cfg(feature = "server")]
use crate::common::validate_alphanumeric_name;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct WorkSpaceTokenBody {
    #[cfg_attr(
        feature = "server",
        garde(length(min = 1, max = 32), custom(validate_alphanumeric_name))
    )]
    pub name: String,
    #[cfg_attr(feature = "server", garde(length(max = 512)))]
    pub description: Option<String>,
}

#[derive(Serialize, Debug, Deserialize)]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[cfg_attr(feature = "pyo3", pyclass(get_all))]
pub struct WorkspaceAPIToken {
    pub id: Uuid,
    pub username: Uuid,
    pub api_secret: String,
    pub workspace_id: Uuid,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[cfg_attr(feature = "pyo3", pyclass(get_all))]
pub struct WorkspaceApiTokenWithNameSchema {
    /// Workspace token id
    pub id: Uuid,
    /// Workspace ID
    pub workspace_id: Uuid,
    /// Description given to token
    pub description: Option<String>,
    /// Creation date of token
    pub created_at: DateTime<Utc>,
    /// Name
    pub name: String,
}

impl EntityOrdering for WorkspaceApiTokenWithNameSchema {
    fn order_fields() -> &'static [&'static str] {
        &["created_at"]
    }
}
