use std::fmt::{Display, Formatter};

use chrono::{DateTime, Utc};
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[cfg(feature = "server")]
use crate::common::{validate_alphanumeric_name, validate_alphanumeric_name_opt};

#[cfg_attr(feature = "server", derive(IntoParams))]
pub struct WorkspaceId {
    /// Workspace identifier
    pub workspace_id: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct WorkSpaceArgs {
    #[cfg_attr(feature = "server", garde(skip))]
    pub organization_id: Uuid,
    #[cfg_attr(
        feature = "server",
        garde(length(min = 3, max = 32), custom(validate_alphanumeric_name))
    )]
    pub name: String,
}

#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct WorkspaceComputeInstanceTypeSchema {
    /// Instance type (m5.2xlarge)
    pub instance_type: String,
    /// Instance memory (in MIB)
    pub memory: u32,
    /// Instance vcpu amount
    pub vcpus: u32,
}

#[derive(Default, Debug, Deserialize)]
#[cfg_attr(feature = "server", derive(Validate, IntoParams))]
#[cfg_attr(feature="server",into_params(parameter_in = Query))]
pub struct WorkspaceQuery {
    #[cfg_attr(
        feature = "server",
        garde(length(min = 3, max = 32), custom(validate_alphanumeric_name_opt))
    )]
    pub name: Option<String>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub organization_id: Option<Uuid>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct WorkspaceDetails {
    #[cfg_attr(
        feature = "server",
        garde(length(min = 3, max = 32), custom(validate_alphanumeric_name_opt))
    )]
    pub name: Option<String>,
    #[cfg_attr(feature = "server", garde(length(max = 512)))]
    pub description: Option<String>,
    #[cfg_attr(feature = "server", garde(range(min = 10)))]
    pub idle_timeout_mins: Option<i32>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct ComputeTimeSchema {
    pub timestamp: DateTime<Utc>,
    // signed to be able to deserialize from postgres
    pub vcpu_hours: f64,
    // signed to be able to deserialize from postgres
    pub ram_mib_hours: f64,
    // signed to be able to deserialize from postgres
    pub storage_gb_hours: f64,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct WorkspaceWithUrlSchema {
    #[serde(flatten)]
    pub workspace: WorkspaceSchema,
    #[serde(rename = "url", alias = "full_url")]
    pub full_url: String,
    pub barebones_url: String,
}

#[cfg_attr(feature = "pyo3", pyclass(eq, eq_int))]
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub enum WorkspaceStateSchema {
    Uninitialized,
    Pending,
    Active,
    Failed,
    Deleted,
}

impl Display for WorkspaceStateSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}T")
    }
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct WorkspaceSchema {
    /// Workspace ID -> UUID v7
    pub id: Uuid,
    /// Organization ID
    pub organization_id: Uuid,
    /// Workspace Name
    pub name: String,
    /// Workspace Description
    pub description: String,
    /// User who owns the Workspace
    pub creator_id: Uuid,
    /// Status of the workspace
    pub status: WorkspaceStateSchema,
    /// Url to deployed resources for this workspace.
    /// For AWS this is a direct link to the cloudformation stack
    pub cloud_resources_url: Option<String>,
    /// The time a cluster can be idle before it will be automatically killed
    pub idle_timeout_mins: i32,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last update timestamp
    pub updated_at: DateTime<Utc>,
    /// Timestamp of the last update
    pub deleted_at: Option<DateTime<Utc>>,
}
