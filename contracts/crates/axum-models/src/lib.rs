mod aws;
mod common;
mod compute;
mod error_response;
mod label;
mod manifest;
mod notification;
mod on_prem;
mod organization;
mod organization_billing;
mod organization_invite;
mod organization_member;
pub mod paginate;
pub(crate) mod query;
mod termination;
mod user;
pub mod workspace;
mod workspace_cluster_defaults;
mod workspace_member;
mod workspace_onboarding;
mod workspace_token;

pub use aws::*;
pub use common::*;
pub use compute::*;
pub use error_response::*;
pub use label::*;
pub use manifest::*;
pub use notification::*;
pub use on_prem::*;
pub use organization::*;
pub use organization_billing::*;
pub use organization_invite::*;
pub use organization_member::*;
pub use paginate::*;
pub use query::*;
use serde::Deserialize;
use serde::de::IntoDeserializer;
pub use termination::*;
pub use user::*;
pub use version_number::VersionNumber;
pub use workspace::*;
pub use workspace_cluster_defaults::*;
pub use workspace_member::*;
pub use workspace_onboarding::*;
pub use workspace_token::*;

/// De/Serialize an [`Option`]`<String>`, transforming the empty string to/from [`None`].
///
/// An empty string (or `null`) is deserialized as [`None`]; [`None`] serializes as the empty string.
mod string_empty_as_none {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Option::<String>::deserialize(deserializer)?.filter(|s| !s.is_empty()))
    }

    pub fn serialize<S>(option: &Option<String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(option.as_deref().unwrap_or(""))
    }
}

pub fn csv_vec_opt<'de, D, T>(deserializer: D) -> Result<Option<Vec<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    if let Some(s) = Option::<String>::deserialize(deserializer)? {
        Ok(Some(
            s.split(',')
                .map(|item| T::deserialize(item.into_deserializer()))
                .collect::<Result<Vec<_>, _>>()?,
        ))
    } else {
        Ok(None)
    }
}
