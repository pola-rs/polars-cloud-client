use deprecation_macro::deprecated_since_client;
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object, get_all))]
#[deprecated_since_client]
pub struct UserModel {
    pub id: Uuid,
    pub email: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    #[deprecated_since_client("0.9.1")] // (the serde_with)
    #[cfg_attr(feature = "server", schemars(with = "Option<String>"))]
    #[serde(with = "crate::string_empty_as_none")]
    pub avatar_url: Option<String>,
    pub default_workspace_id: Option<Uuid>,
    pub newsletter_updates: bool,
    pub personal_emails: bool,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
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
