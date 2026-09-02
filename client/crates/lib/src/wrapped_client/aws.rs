#![allow(clippy::result_large_err)]

use client_core::{ApiError, RUNTIME, workspace_aws};
use polars_axum_models::{
    DeleteWorkspaceModel, WorkSpaceArgs, WorkspaceAwsConnectionModel, WorkspaceAwsStackModel,
    WorkspaceSetupUrlModel, WorkspaceWithUrlModel,
};
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::entry::EnterRustExt;
use crate::wrapped_client::WrappedAPIClient;

#[pymethods]
impl WrappedAPIClient {
    #[pyo3(signature=(name, organization_id))]
    pub fn create_aws_workspace(
        &self,
        py: Python,
        name: String,
        organization_id: Uuid,
    ) -> Result<WorkspaceWithUrlModel, ApiError> {
        py.enter_rust(|| {
            let params = WorkSpaceArgs {
                name,
                organization_id,
            };
            RUNTIME.block_on(self.client.create_aws_workspace(params))?
        })
    }

    #[pyo3(signature=(workspace_id))]
    pub fn get_workspace_setup_url(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<WorkspaceSetupUrlModel, ApiError> {
        py.enter_rust(|| RUNTIME.block_on(self.client.get_aws_workspace_setup_url(workspace_id))?)
    }

    #[pyo3(signature=(workspace_id))]
    pub fn get_workspace_stack(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<WorkspaceAwsStackModel, ApiError> {
        py.enter_rust(|| RUNTIME.block_on(self.client.get_aws_workspace_stack(workspace_id))?)
    }

    #[pyo3(signature=(workspace_id))]
    pub fn get_workspace_aws_connection(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<Option<WorkspaceAwsConnectionModel>, ApiError> {
        py.enter_rust(|| RUNTIME.block_on(workspace_aws::connection(&self.client, workspace_id))?)
    }

    #[pyo3(signature=(workspace_id))]
    pub fn delete_workspace_aws_connection(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<DeleteWorkspaceModel, ApiError> {
        py.enter_rust(|| {
            RUNTIME.block_on(self.client.delete_workspace_aws_connection(workspace_id))?
        })
    }
}
