use chrono::{DateTime, Utc};
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DefaultSortDirection, EntityOrdering};

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
pub struct OrganizationInviteArgs {
    #[cfg_attr(feature = "server", garde(length(min = 1, max = 128)))]
    pub route: String,
    #[cfg_attr(feature = "server", garde(email))]
    pub email: String,
    #[cfg_attr(feature = "server", garde(skip))]
    pub send_email: bool,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct OrganizationInviteWithUrlModel {
    #[serde(flatten)]
    pub invite: OrganizationInviteModel,
    pub url: String,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct OrganizationInviteModel {
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

impl EntityOrdering for OrganizationInviteModel {
    fn order_fields() -> &'static [&'static str] {
        &["id", "organization_name", "accepted_at"]
    }

    fn default_ordering() -> Option<(&'static str, DefaultSortDirection)> {
        Some(("id", DefaultSortDirection::Desc))
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
#[serde(deny_unknown_fields)]
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
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct RedeemInviteArgs {
    pub id: Uuid,
    pub key: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct GetOrganizationInvitesArgs {
    pub workspace_id: Option<Uuid>,
}
