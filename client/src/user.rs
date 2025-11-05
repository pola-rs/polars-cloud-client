#![allow(clippy::result_large_err)]

use polars_axum_models::UserSchema;
use polars_backend_client::client::ApiClient;
use pyo3::{Python, pymethods};

use crate::client::WrappedAPIClient;
use crate::error::ApiError;

#[pymethods]
impl WrappedAPIClient {
    pub fn get_user(&mut self, py: Python<'_>) -> Result<UserSchema, ApiError> {
        self.call(py, |client: &ApiClient| client.get_logged_in_user())
    }
}
