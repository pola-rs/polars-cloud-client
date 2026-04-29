#![allow(clippy::result_large_err)]

use client_core::{ApiError, RUNTIME};
use polars_axum_models::{
    WorkSpaceTokenBodyArgs, WorkspaceAPITokenModel, WorkspaceApiTokenWithNameModel,
};
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::entry::EnterRustExt;
use crate::wrapped_client::WrappedAPIClient;

#[pymethods]
impl WrappedAPIClient {
    pub fn get_service_accounts(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<Vec<WorkspaceApiTokenWithNameModel>, ApiError> {
        py.enter_rust(|| RUNTIME.block_on(self.client.get_workspace_tokens(workspace_id))?)
    }

    pub fn create_service_account(
        &self,
        py: Python,
        workspace_id: Uuid,
        name: String,
        description: Option<String>,
    ) -> Result<WorkspaceAPITokenModel, ApiError> {
        py.enter_rust(|| {
            let body = WorkSpaceTokenBodyArgs { name, description };
            RUNTIME.block_on(self.client.create_workspace_token(workspace_id, body))?
        })
    }

    pub fn delete_service_account(
        &self,
        py: Python,
        workspace_id: Uuid,
        user_id: Uuid,
    ) -> Result<(), ApiError> {
        py.enter_rust(|| {
            RUNTIME.block_on(self.client.delete_workspace_token(workspace_id, user_id))?
        })
    }
}
