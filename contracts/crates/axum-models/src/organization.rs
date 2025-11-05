use chrono::{DateTime, Utc};
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
#[cfg(feature = "server")]
use regex::Regex;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub enum OrganizationSubscriptionStateSchema {
    PreTrial,
    Trial,
    TrialExpired,
    Subscribing,
    Subscribed,
    Unsubscribed,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct OrganizationSchema {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub avatar_url: String,
    pub creator_id: Uuid,
    pub subscription_state: OrganizationSubscriptionStateSchema,
    pub trial_started_at: Option<DateTime<Utc>>,
    pub trial_expires_at: Option<DateTime<Utc>>,
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
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct OrganizationCreateSchema {
    /// Organization name
    #[cfg_attr(
        feature = "server",
        garde(length(min = 1, max = 32), custom(validate_organization_name))
    )]
    pub name: String,
}

#[derive(Default, Debug, Deserialize)]
#[cfg_attr(feature = "server", derive(Validate, IntoParams))]
#[cfg_attr(feature="server",into_params(parameter_in = Query))]
pub struct OrganizationQuery {
    // Todo! what limits do we want on the name
    #[cfg_attr(
        feature = "server",
        garde(length(min = 1, max = 32), custom(validate_organization_name_opt))
    )]
    pub name: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct OrganizationDetails {
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
    #[cfg(feature = "server")]
    use crate::organization::validate_organization_name;

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
