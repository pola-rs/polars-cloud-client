#![allow(clippy::result_large_err)]

use polars_axum_models::{WorkSpaceTokenBody, WorkspaceAPIToken, WorkspaceApiTokenWithNameSchema};
use polars_backend_client::client::ApiClient;
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::client::{CLIENT_GLOBAL, WrappedAPIClient};
use crate::entry::EnterRustExt;
use crate::error::ApiError;

#[pymethods]
impl WrappedAPIClient {
    pub fn get_service_accounts(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<Vec<WorkspaceApiTokenWithNameSchema>, ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call(|client: &ApiClient| client.get_workspace_tokens(workspace_id))
        })
    }

    pub fn create_service_account(
        &self,
        py: Python,
        workspace_id: Uuid,
        name: String,
        description: Option<String>,
    ) -> Result<WorkspaceAPIToken, ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call(move |client: &ApiClient| {
                let body = WorkSpaceTokenBody { name, description };
                client.create_workspace_token(workspace_id, body)
            })
        })
    }

    pub fn delete_service_account(
        &self,
        py: Python,
        workspace_id: Uuid,
        user_id: Uuid,
    ) -> Result<(), ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call(move |client: &ApiClient| {
                client.delete_workspace_token(workspace_id, user_id)
            })
        })
    }
}
