#[cfg(feature = "server")]
use garde::Validate;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::ToSchema;
use uuid::Uuid;

use crate::EntityOrdering;

#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum WorkspaceRoleSchema {
    Admin,
    Member,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct WorkspaceMemberRole {
    #[cfg_attr(feature = "server", garde(skip))]
    pub role: WorkspaceRoleSchema,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListMembersQueryParams {
    pub implicit_users: Option<bool>,
    pub service_accounts: Option<bool>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct WorkspaceUserSchema {
    pub id: Uuid,
    pub email: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub avatar_url: String,
    pub role: WorkspaceRoleSchema,
    pub implicit: bool,
    pub service_account: bool,
}

impl EntityOrdering for WorkspaceUserSchema {
    fn order_fields() -> &'static [&'static str] {
        &["id", "first_name", "last_name"]
    }
}
