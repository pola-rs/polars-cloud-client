use chrono::{DateTime, Utc};
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DefaultSortDirection, EntityOrdering};

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
    /// Time after which the invite can no longer be redeemed
    pub expires_at: DateTime<Utc>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct OrganizationInviteWorkspaceModel {
    pub id: Uuid,
    pub name: String,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct OrganizationInviteDetailsModel {
    #[serde(flatten)]
    pub invite: OrganizationInviteModel,
    /// Workspaces the invite grants access to, in invite order
    pub workspaces: Vec<OrganizationInviteWorkspaceModel>,
}

impl EntityOrdering for OrganizationInviteModel {
    fn order_fields() -> &'static [&'static str] {
        &["id", "organization_name", "accepted_at", "expires_at"]
    }

    fn default_ordering() -> Option<(&'static str, DefaultSortDirection)> {
        Some(("id", DefaultSortDirection::Desc))
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct InviteArgs {
    #[cfg_attr(feature = "server", garde(email))]
    pub email: String,
    #[cfg_attr(feature = "server", garde(skip))]
    pub send_email: bool,
    #[cfg_attr(feature = "server", garde(skip))]
    pub workspace_ids: Vec<Uuid>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RedeemInviteArgs {
    #[cfg_attr(feature = "server", garde(skip))]
    pub id: Uuid,
    #[cfg_attr(feature = "server", garde(skip))]
    pub key: String,
    /// Workspace of the invite to make the user's default workspace. Leave empty to keep the
    /// current default.
    #[serde(default)]
    #[cfg_attr(feature = "server", garde(skip))]
    pub default_workspace_id: Option<Uuid>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(
    feature = "server",
    derive(Validate, JsonSchema),
    garde(allow_unvalidated)
)]
pub struct GetOrganizationInvitesArgs {
    pub workspace_id: Option<Uuid>,
}
