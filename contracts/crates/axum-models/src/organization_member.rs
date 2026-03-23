#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::EntityOrdering;

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum OrganizationRoleModel {
    Admin,
    Member,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
pub struct OrganizationMemberRoleArgs {
    #[cfg_attr(feature = "server", garde(skip))]
    pub role: OrganizationRoleModel,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct OrganizationUserModel {
    pub id: Uuid,
    pub email: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub avatar_url: String,
    pub role: OrganizationRoleModel,
}
impl EntityOrdering for OrganizationUserModel {
    fn order_fields() -> &'static [&'static str] {
        &["id", "first_name", "last_name"]
    }
}
