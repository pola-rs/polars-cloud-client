use chrono::{DateTime, Utc};
#[cfg(feature = "server")]
use garde::Validate;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::IntoParams;
#[cfg(feature = "server")]
use utoipa::ToSchema;
use uuid::Uuid;

use crate::EntityOrdering;

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct OrganizationInviteArgs {
    #[cfg_attr(feature = "server", garde(length(min = 1, max = 128)))]
    pub route: String,
    #[cfg_attr(feature = "server", garde(email))]
    pub email: String,
    #[cfg_attr(feature = "server", garde(skip))]
    pub send_email: bool,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct OrganizationInviteWithUrlSchema {
    #[serde(flatten)]
    pub invite: OrganizationInviteSchema,
    pub url: String,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct OrganizationInviteSchema {
    /// Invite id
    pub id: Uuid,
    /// The creator of the invite
    pub user_id: Uuid,
    /// Organization ID
    pub organization_id: Uuid,
    /// Name of the organization
    pub organization_name: String,
    /// Workspace IDs to immediately add the user to
    pub workspace_ids: Vec<Uuid>,
    /// Email of the person receiving the invite
    pub email: String,
    /// Email of the person creating the invite
    pub inviter_email: String,
    /// Time the invited was accepted
    pub accepted_at: Option<DateTime<Utc>>,
}

impl EntityOrdering for OrganizationInviteSchema {
    fn order_fields() -> &'static [&'static str] {
        &["id", "organization_name", "accepted_at"]
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct InviteArgs {
    #[cfg_attr(feature = "server", garde(length(min = 1, max = 128)))]
    pub route: String,
    #[cfg_attr(feature = "server", garde(email))]
    pub email: String,
    #[cfg_attr(feature = "server", garde(skip))]
    pub send_email: bool,
    #[cfg_attr(feature = "server", garde(skip))]
    pub workspace_ids: Vec<Uuid>,
}

#[derive(Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(IntoParams))]
#[cfg_attr(feature = "server",into_params(parameter_in = Query))]
pub struct RedeemInviteParams {
    pub id: Uuid,
    pub key: String,
}
