#![allow(clippy::result_large_err)]

use polars_axum_models::UserSchema;
use polars_backend_client::client::ApiClient;
use pyo3::{Python, pymethods};

use crate::client::{CLIENT_GLOBAL, WrappedAPIClient};
use crate::entry::EnterRustExt;
use crate::error::ApiError;

#[pymethods]
impl WrappedAPIClient {
    pub fn get_user(&self, py: Python) -> Result<UserSchema, ApiError> {
        py.enter_rust(|| CLIENT_GLOBAL.call(|client: &ApiClient| client.get_logged_in_user()))
    }
}
