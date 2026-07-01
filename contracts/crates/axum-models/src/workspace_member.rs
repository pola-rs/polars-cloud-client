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
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct WorkspaceUserModel {
    pub id: Uuid,
    pub email: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub avatar_url: String,
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
