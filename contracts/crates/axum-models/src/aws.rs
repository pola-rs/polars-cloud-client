#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Deserialize, Debug)]
#[cfg_attr(
    feature = "server",
    derive(ToSchema, Validate),
    garde(allow_unvalidated)
)]
pub struct WorkspaceCallbackArgs {
    pub stack_name: String,
    pub workspace_id: Uuid,
    pub user_id: Uuid,
    pub encrypted_external_id: String,
    pub user_initiated_action_role: String,
    pub unattended_role_arn: String,
    pub worker_role_arn: Option<String>,
    pub worker_role_profile_arn: String,
    pub subnet_ids: Vec<String>,
    pub proxy_security_group: String,
    pub direct_security_group: String,
    pub region: String,
}

#[derive(Deserialize, Debug)]
#[cfg_attr(
    feature = "server",
    derive(ToSchema, Validate),
    garde(allow_unvalidated)
)]
pub struct AWSWorkspaceDeleteCallbackArgs {
    pub workspace_id: Uuid,
    pub user_id: Uuid,
    pub encrypted_external_id: String,
}

#[derive(Deserialize, Debug)]
#[cfg_attr(
    feature = "server",
    derive(ToSchema, Validate),
    garde(allow_unvalidated)
)]
pub struct AWSWorkspaceStartCallbackArgs {
    pub workspace_id: Uuid,
    pub user_id: Uuid,
    pub stack_url: Option<String>,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Deserialize, Serialize, Debug)]
pub struct DeleteWorkspaceSchema {
    pub stack_name: String,
    pub url: String,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Deserialize, Serialize, Debug)]
pub struct WorkspaceSetupUrlSchema {
    #[serde(rename = "setup_url", alias = "full_setup_url")]
    pub full_setup_url: String,
    pub barebones_setup_url: String,
    #[serde(rename = "template_url", alias = "full_template_url")]
    pub full_template_url: String,
    pub barebones_template_url: String,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Deserialize, Serialize, Debug)]
pub struct WorkspaceAWSSettingsOutputSchema {
    pub worker_role_arn: Option<String>,
    pub region: String,
    pub workspace_id: Uuid,
    pub account_id: String,
}
