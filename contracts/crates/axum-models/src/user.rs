#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[cfg_attr(feature = "pyo3", pyclass(get_all))]
pub struct UserSchema {
    pub id: Uuid,
    pub email: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub avatar_url: String,
    pub default_workspace_id: Option<Uuid>,
    pub newsletter_updates: bool,
    pub personal_emails: bool,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct UserBodyArgs {
    #[cfg_attr(feature = "server", garde(length(min = 1, max = 32)))]
    pub first_name: Option<String>,
    #[cfg_attr(feature = "server", garde(length(min = 1, max = 32)))]
    pub last_name: Option<String>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub default_workspace_id: Option<Uuid>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub newsletter_updates: Option<bool>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub personal_emails: Option<bool>,
}
