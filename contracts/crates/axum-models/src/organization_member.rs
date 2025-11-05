#[cfg(feature = "server")]
use garde::Validate;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::ToSchema;
use uuid::Uuid;

use crate::EntityOrdering;

#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum OrganizationRoleSchema {
    Admin,
    Member,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct OrganizationMemberRole {
    #[cfg_attr(feature = "server", garde(skip))]
    pub role: OrganizationRoleSchema,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct OrganizationUserSchema {
    pub id: Uuid,
    pub email: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub avatar_url: String,
    pub role: OrganizationRoleSchema,
}
impl EntityOrdering for OrganizationUserSchema {
    fn order_fields() -> &'static [&'static str] {
        &["id", "first_name", "last_name"]
    }
}
