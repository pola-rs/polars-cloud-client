use chrono::{DateTime, Utc};
use deprecation_macro::deprecated_since_client;
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
#[cfg(feature = "server")]
use regex::Regex;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub enum OrganizationTierModel {
    FreeTier,
    PayAsYouGo,
}

/// Superseded by [`OrganizationTierModel`]. Kept only so that clients up to 0.10.x, which
/// deserialize [`OrganizationModel`] strictly, can still parse our responses. None of them
/// read the value.
#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[deprecated_since_client("0.11.0")]
pub enum OrganizationSubscriptionStateModel {
    PreTrial,
    Trial,
    TrialExpired,
    Subscribing,
    Subscribed,
    Unsubscribed,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Deserialize, Serialize, Debug)]
#[deprecated_since_client]
pub struct OrganizationModel {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    #[deprecated_since_client("0.9.1")] // (the serde_with)
    #[cfg_attr(feature = "server", schemars(with = "Option<String>"))]
    #[serde(with = "crate::string_empty_as_none")]
    pub avatar_url: Option<String>,
    pub creator_id: Uuid,
    #[deprecated_since_client("0.11.0")]
    pub subscription_state: OrganizationSubscriptionStateModel,
    #[deprecated_since_client("0.11.0")]
    pub trial_started_at: Option<DateTime<Utc>>,
    #[deprecated_since_client("0.11.0")]
    pub trial_expires_at: Option<DateTime<Utc>>,
    pub tier: OrganizationTierModel,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[cfg(feature = "server")]
fn validate_organization_name_opt(name: &Option<String>, ctx: &()) -> garde::Result {
    if let Some(name) = name {
        validate_organization_name(name, ctx)
    } else {
        Ok(())
    }
}

#[cfg(feature = "server")]
fn validate_organization_name(name: &str, _ctx: &()) -> garde::Result {
    if name != name.trim() {
        return Err(garde::Error::new(
            "The organization name cannot have whitespace characters at the start or end",
        ));
    }

    let valid_alphabet = Regex::new(r"^[\p{Alphabetic}\p{M}\d .'\-&()]+$").unwrap();
    let starts_with_a_character = Regex::new(r"^[\p{Alphabetic}\d].*$").unwrap();

    if !valid_alphabet.is_match(name) {
        return Err(garde::Error::new(
            "Only letters, numbers, spaces, and - _ ' & ( ) are allowed",
        ));
    }

    if !starts_with_a_character.is_match(name) {
        return Err(garde::Error::new("Must start with a letter or digit"));
    }

    Ok(())
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
pub struct OrganizationCreateArgs {
    /// Organization name
    #[cfg_attr(
        feature = "server",
        garde(length(min = 1, max = 32), custom(validate_organization_name))
    )]
    pub name: String,
}

#[derive(Default, Debug, Deserialize)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
pub struct OrganizationQueryArgs {
    // Todo! what limits do we want on the name
    #[cfg_attr(
        feature = "server",
        garde(length(min = 1, max = 32), custom(validate_organization_name_opt))
    )]
    pub name: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
pub struct OrganizationDetailsArgs {
    #[cfg_attr(
        feature = "server",
        garde(length(min = 1, max = 32), custom(validate_organization_name_opt))
    )]
    pub name: Option<String>,
    #[cfg_attr(feature = "server", garde(length(max = 512)))]
    pub description: Option<String>,
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use serde::Deserialize;
    use uuid::Uuid;

    #[cfg(feature = "server")]
    use crate::organization::validate_organization_name;
    use crate::organization::{
        OrganizationModel, OrganizationSubscriptionStateModel, OrganizationTierModel,
    };

    /// `OrganizationModel` as clients up to 0.10.x declare it. Those clients derive
    /// `Deserialize` without field defaults, so every field here -- including the pre-tier
    /// ones we no longer use ourselves -- has to stay on the wire or they cannot parse an
    /// organization at all.
    #[derive(Deserialize)]
    #[expect(dead_code)]
    struct OrganizationModelUpTo0_10 {
        id: Uuid,
        name: String,
        description: String,
        avatar_url: String,
        creator_id: Uuid,
        subscription_state: OrganizationSubscriptionStateModel,
        trial_started_at: Option<DateTime<Utc>>,
        trial_expires_at: Option<DateTime<Utc>>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        deleted_at: Option<DateTime<Utc>>,
    }

    #[test]
    fn test_organization_model_parses_on_older_clients() {
        let model = OrganizationModel {
            id: Uuid::now_v7(),
            name: "Acme".to_string(),
            description: "An organization".to_string(),
            avatar_url: None,
            creator_id: Uuid::now_v7(),
            subscription_state: OrganizationSubscriptionStateModel::Trial,
            trial_started_at: None,
            trial_expires_at: None,
            tier: OrganizationTierModel::FreeTier,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            deleted_at: None,
        };

        let json = serde_json::to_string(&model).unwrap();
        serde_json::from_str::<OrganizationModelUpTo0_10>(&json)
            .expect("older clients must still be able to deserialize an organization");
    }

    #[cfg(feature = "server")]
    #[test]
    fn test_validate_organization_name() {
        assert!(validate_organization_name("test", &()).is_ok());
        assert!(validate_organization_name("Johnson & Johnson", &()).is_ok());
        assert!(validate_organization_name("üñîçødê", &()).is_ok());
        assert!(validate_organization_name("ソニー株式会社", &()).is_ok());
        assert!(validate_organization_name("X", &()).is_ok());
        assert!(validate_organization_name("x -.()&'", &()).is_ok());

        assert!(validate_organization_name("", &()).is_err());
        assert!(validate_organization_name("   ", &()).is_err());
        assert!(validate_organization_name(" a ", &()).is_err());
        assert!(validate_organization_name("_a", &()).is_err());
        assert!(validate_organization_name("one\ntwo", &()).is_err());
        assert!(validate_organization_name("a💩", &()).is_err());
        assert!(validate_organization_name("--willem--", &()).is_err());
        assert!(validate_organization_name("!$%", &()).is_err());
        assert!(validate_organization_name("ABC\u{202e}DEF", &()).is_err());
    }
}
