#![allow(clippy::result_large_err)]

use client_core::{ApiError, CTRL_PLN_CLIENT_GLOBAL};
use polars_axum_models::UserModel;
use polars_backend_client::client::ApiClient;
use pyo3::{Python, pymethods};

use crate::entry::EnterRustExt;
use crate::wrapped_client::WrappedAPIClient;

#[pymethods]
impl WrappedAPIClient {
    pub fn get_user(&self, py: Python) -> Result<UserModel, ApiError> {
        py.enter_rust(|| {
            CTRL_PLN_CLIENT_GLOBAL.call(|client: &ApiClient| client.get_logged_in_user())
        })
    }
}
