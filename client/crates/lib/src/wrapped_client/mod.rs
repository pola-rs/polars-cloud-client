use client_core::{ApiError, CTRL_PLN_CLIENT_GLOBAL, RUNTIME};
use polars_backend_client::client::ApiClient;
use pyo3::{Python, pyclass, pymethods};

use crate::entry::EnterRustExt;

pub mod aws;
pub mod compute;
pub mod organization;
pub mod query;
pub mod query_control_grpc;
pub mod service_account;
pub mod user;
pub mod workspace;

#[pyclass(from_py_object, name = "ApiClient")]
#[derive(Clone, Default)]
pub struct WrappedAPIClient {}

#[pymethods]
impl WrappedAPIClient {
    #[new]
    fn new() -> Self {
        Default::default()
    }

    fn login(&self, py: Python) -> Result<(), ApiError> {
        py.enter_rust(|| RUNTIME.block_on(CTRL_PLN_CLIENT_GLOBAL.login())?)
    }

    fn clear_authentication(&self, py: Python) {
        let _ = py.enter_rust_ok(|| CTRL_PLN_CLIENT_GLOBAL.clear_authentication());
    }

    fn get_auth_header(&self, py: Python) -> Result<String, ApiError> {
        let out = py.enter_rust(|| {
            CTRL_PLN_CLIENT_GLOBAL
                .call(|_api_client: &ApiClient| async { Ok(()) })
                .map(|_| {
                    CTRL_PLN_CLIENT_GLOBAL
                        .rest()
                        .auth_header
                        .read()
                        .unwrap()
                        .clone()
                })
        })?;
        Ok(out)
    }

    #[pyo3(signature = (client_id=None, client_secret=None, interactive=true))]
    fn authenticate(
        &self,
        py: Python,
        client_id: Option<String>,
        client_secret: Option<String>,
        interactive: bool,
    ) -> Result<(), ApiError> {
        py.enter_rust(|| {
            RUNTIME.block_on(CTRL_PLN_CLIENT_GLOBAL.authenticate(
                client_id,
                client_secret,
                interactive,
            ))?
        })
    }
}
