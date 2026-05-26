#![allow(clippy::result_large_err)]

use client_core::{ApiError, RUNTIME};
use polars_axum_models::{WorkSpaceArgs, WorkspaceModel};
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::entry::EnterRustExt;
use crate::wrapped_client::WrappedAPIClient;

#[pymethods]
impl WrappedAPIClient {
    #[pyo3(signature=(name, organization_id))]
    pub fn create_on_prem_workspace(
        &self,
        py: Python,
        name: String,
        organization_id: Uuid,
    ) -> Result<WorkspaceModel, ApiError> {
        py.enter_rust(|| {
            let params = WorkSpaceArgs {
                name,
                organization_id,
            };
            RUNTIME.block_on(self.client.create_on_prem_workspace(params))?
        })
    }

    #[pyo3(signature=(workspace_id))]
    pub fn delete_on_prem_workspace(&self, py: Python, workspace_id: Uuid) -> Result<(), ApiError> {
        py.enter_rust(|| RUNTIME.block_on(self.client.delete_on_prem_workspace(workspace_id))?)
    }
}
