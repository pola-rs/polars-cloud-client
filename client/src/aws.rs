#![allow(clippy::result_large_err)]

use polars_axum_models::{
    DeleteWorkspaceSchema, WorkSpaceArgs, WorkspaceSetupUrlSchema, WorkspaceWithUrlSchema,
};
use polars_backend_client::client::ApiClient;
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::client::{CLIENT_GLOBAL, WrappedAPIClient};
use crate::entry::EnterRustExt;
use crate::error::ApiError;

#[pymethods]
impl WrappedAPIClient {
    #[pyo3(signature=(name, organization_id))]
    pub fn create_workspace(
        &self,
        py: Python,
        name: String,
        organization_id: Uuid,
    ) -> Result<WorkspaceWithUrlSchema, ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call(|client: &ApiClient| {
                let params = WorkSpaceArgs {
                    name,
                    organization_id,
                };
                client.create_workspace(params)
            })
        })
    }

    #[pyo3(signature=(workspace_id))]
    pub fn get_workspace_setup_url(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<WorkspaceSetupUrlSchema, ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call(|client: &ApiClient| client.get_workspace_setup_url(workspace_id))
        })
    }

    #[pyo3(signature=(workspace_id))]
    pub fn delete_workspace(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<Option<DeleteWorkspaceSchema>, ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call(|client: &ApiClient| client.delete_workspace(workspace_id))
        })
    }
}
