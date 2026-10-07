use chrono::{DateTime, Utc};
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "server")]
const MIN_SHARE_LIFETIME: chrono::TimeDelta = chrono::TimeDelta::hours(1);

/// A share link of a query, as members see it.
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct QueryShareModel {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub query_id: Uuid,
    /// The member who created the link
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// `null` means the link never expires
    pub expires_at: Option<DateTime<Utc>>,
    /// The credential, as 64 hex characters. It appears here and in the `Shared-Resource-Token` header only.
    #[cfg_attr(feature = "server", schemars(regex(pattern = r"^[0-9a-f]{64}$")))]
    pub token: String,
    /// The shareable page URL
    pub url: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateQueryShareArgs {
    /// `null` or omitted means the link never expires. Otherwise it must be in the future.
    #[cfg_attr(feature = "server", garde(custom(validate_expires_at)))]
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

#[cfg(feature = "server")]
fn validate_expires_at(expires_at: &Option<DateTime<Utc>>, _ctx: &()) -> garde::Result {
    if let Some(expires_at) = expires_at
        && *expires_at < Utc::now() + MIN_SHARE_LIFETIME
    {
        return Err(garde::Error::new(
            "Expiry must be at least one hour in the future.",
        ));
    }
    Ok(())
}
