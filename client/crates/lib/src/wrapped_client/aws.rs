#![allow(clippy::result_large_err)]

use client_core::{ApiError, CTRL_PLN_CLIENT_GLOBAL};
use polars_axum_models::{
    DeleteWorkspaceModel, WorkSpaceArgs, WorkspaceSetupUrlModel, WorkspaceWithUrlModel,
};
use polars_backend_client::client::ApiClient;
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::entry::EnterRustExt;
use crate::wrapped_client::WrappedAPIClient;

#[pymethods]
impl WrappedAPIClient {
    #[pyo3(signature=(name, organization_id))]
    pub fn create_workspace(
        &self,
        py: Python,
        name: String,
        organization_id: Uuid,
    ) -> Result<WorkspaceWithUrlModel, ApiError> {
        py.enter_rust(|| {
            CTRL_PLN_CLIENT_GLOBAL.call(|client: &ApiClient| {
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
    ) -> Result<WorkspaceSetupUrlModel, ApiError> {
        py.enter_rust(|| {
            CTRL_PLN_CLIENT_GLOBAL
                .call(|client: &ApiClient| client.get_workspace_setup_url(workspace_id))
        })
    }

    #[pyo3(signature=(workspace_id))]
    pub fn delete_workspace(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<Option<DeleteWorkspaceModel>, ApiError> {
        py.enter_rust(|| {
            CTRL_PLN_CLIENT_GLOBAL.call(|client: &ApiClient| client.delete_workspace(workspace_id))
        })
    }
}
