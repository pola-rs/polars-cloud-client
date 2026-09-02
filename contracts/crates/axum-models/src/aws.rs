#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Up to and including this version stamp the lambda callback does not carry the CloudFormation
// template version; we default to 1.0.0 (same default as the database)
fn default_cfn_template_version() -> String {
    "1.0.0".to_string()
}

#[derive(Deserialize, Debug)]
#[cfg_attr(
    feature = "server",
    derive(JsonSchema, Validate),
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
    #[serde(default = "default_cfn_template_version")]
    pub cfn_template_version: String,
}

#[derive(Deserialize, Debug)]
#[cfg_attr(
    feature = "server",
    derive(JsonSchema, Validate),
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
    derive(JsonSchema, Validate),
    garde(allow_unvalidated)
)]
pub struct AWSWorkspaceStartCallbackArgs {
    pub workspace_id: Uuid,
    pub user_id: Uuid,
    pub stack_url: Option<String>,
}

#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Deserialize, Serialize, Debug)]
pub struct DeleteWorkspaceModel {
    pub stack_name: String,
    pub url: String,
}

#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Deserialize, Serialize, Debug)]
pub struct WorkspaceSetupUrlModel {
    #[serde(rename = "setup_url", alias = "full_setup_url")]
    pub full_setup_url: String,
    pub barebones_setup_url: String,
    #[serde(rename = "template_url", alias = "full_template_url")]
    pub full_template_url: String,
    pub barebones_template_url: String,
}

#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Deserialize, Serialize, Debug)]
pub struct WorkspaceAwsStackModel {
    pub workspace_id: Uuid,
    pub console_url: String,
}

#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Deserialize, Serialize, Debug)]
pub struct WorkspaceAWSSettingsOutputModel {
    pub worker_role_arn: Option<String>,
    pub region: String,
    pub workspace_id: Uuid,
    pub account_id: String,
    pub cfn_template_version: String,
    pub latest_cfn_template_version: String,
}

/// Where a workspace's AWS connection is in its lifecycle.
#[cfg_attr(feature = "pyo3", pyclass(from_py_object, eq, eq_int))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum AwsConnectionStatusModel {
    /// The CloudFormation stack is deploying and has not reported its settings yet.
    Pending,
    /// The stack deployed and the workspace can run AWS clusters.
    Completed,
    /// The stack rolled back before it ever reported its settings.
    Failed,
    /// The stack was torn down and AWS is no longer connected.
    Deleted,
}

/// A workspace's AWS connection as one resource. The fields are filled in as the
/// CloudFormation stack progresses, so everything except the status is optional.
#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Deserialize, Serialize, Debug)]
pub struct WorkspaceAwsConnectionModel {
    pub workspace_id: Uuid,
    pub status: AwsConnectionStatusModel,
    /// Link to the CloudFormation stack, available from the moment it starts deploying.
    pub console_url: Option<String>,
    pub stack_name: Option<String>,
    pub region: Option<String>,
    pub account_id: Option<String>,
    pub worker_role_arn: Option<String>,
    pub cfn_template_version: Option<String>,
    pub latest_cfn_template_version: String,
}

/// Fields are exposed to Python one by one, as a caller needs them.
#[cfg_attr(feature = "pyo3", pyo3::pymethods)]
#[cfg(feature = "pyo3")]
impl WorkspaceAwsConnectionModel {
    #[getter]
    fn status(&self) -> AwsConnectionStatusModel {
        self.status.clone()
    }

    #[getter]
    fn console_url(&self) -> Option<&str> {
        self.console_url.as_deref()
    }
}
