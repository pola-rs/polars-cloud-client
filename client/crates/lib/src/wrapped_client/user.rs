#![allow(clippy::result_large_err)]

use client_core::{ApiError, RUNTIME};
use polars_axum_models::UserModel;
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::entry::EnterRustExt;
use crate::wrapped_client::WrappedAPIClient;

#[pymethods]
impl WrappedAPIClient {
    pub fn get_user(&self, py: Python) -> Result<UserModel, ApiError> {
        py.enter_rust(|| RUNTIME.block_on(self.client.get_logged_in_user())?)
    }

    pub fn set_user_default_workspace(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<(), ApiError> {
        py.enter_rust(|| RUNTIME.block_on(self.client.set_default_workspace(workspace_id))?)
    }
}
