use deprecation_macro::deprecated_since_client;
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DefaultSortDirection, EntityOrdering};

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum WorkspaceRoleModel {
    Admin,
    Member,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
pub struct WorkspaceMemberRole {
    #[cfg_attr(feature = "server", garde(skip))]
    pub role: WorkspaceRoleModel,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct ListWorkspaceMembersQueryArgs {
    pub implicit_users: Option<bool>,
    pub service_accounts: Option<bool>,
    pub email: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[deprecated_since_client]
pub struct WorkspaceUserModel {
    pub id: Uuid,
    pub email: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    #[deprecated_since_client("0.9.1")] // (the serde_with)
    #[cfg_attr(feature = "server", schemars(with = "Option<String>"))]
    #[serde(with = "crate::string_empty_as_none")]
    pub avatar_url: Option<String>,
    pub role: WorkspaceRoleModel,
    pub implicit: bool,
    pub service_account: bool,
}

impl EntityOrdering for WorkspaceUserModel {
    fn order_fields() -> &'static [&'static str] {
        &["id", "first_name", "last_name"]
    }

    fn default_ordering() -> Option<(&'static str, DefaultSortDirection)> {
        Some(("id", DefaultSortDirection::Desc))
    }
}
