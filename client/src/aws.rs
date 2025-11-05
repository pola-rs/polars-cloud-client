#![allow(clippy::result_large_err)]

use polars_axum_models::{
    DeleteWorkspaceSchema, WorkSpaceArgs, WorkspaceSetupUrlSchema, WorkspaceWithUrlSchema,
};
use polars_backend_client::client::ApiClient;
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::client::WrappedAPIClient;
use crate::error::ApiError;

#[pymethods]
impl WrappedAPIClient {
    #[pyo3(signature=(name, organization_id))]
    pub fn create_workspace(
        &mut self,
        py: Python,
        name: String,
        organization_id: Uuid,
    ) -> Result<WorkspaceWithUrlSchema, ApiError> {
        self.call(py, |client: &ApiClient| {
            let params = WorkSpaceArgs {
                name,
                organization_id,
            };
            client.create_workspace(params)
        })
    }

    #[pyo3(signature=(workspace_id))]
    pub fn get_workspace_setup_url(
        &mut self,
        py: Python<'_>,
        workspace_id: Uuid,
    ) -> Result<WorkspaceSetupUrlSchema, ApiError> {
        self.call(py, |client: &ApiClient| {
            client.get_workspace_setup_url(workspace_id)
        })
    }

    #[pyo3(signature=(workspace_id))]
    pub fn delete_workspace(
        &mut self,
        py: Python<'_>,
        workspace_id: Uuid,
    ) -> Result<Option<DeleteWorkspaceSchema>, ApiError> {
        self.call(py, |client: &ApiClient| {
            client.delete_workspace(workspace_id)
        })
    }
}
