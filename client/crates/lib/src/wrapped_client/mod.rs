#![allow(clippy::result_large_err)]

use std::sync::Arc;

use client_core::{ApiError, ControlPlaneClient, RUNTIME};
use pyo3::{Python, pyclass, pymethods};

use crate::CTRL_PLN_CLIENT_GLOBAL;
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
#[derive(Clone)]
pub struct WrappedAPIClient {
    pub(crate) client: Arc<dyn ControlPlaneClient>,
}

impl Default for WrappedAPIClient {
    fn default() -> Self {
        Self {
            client: CTRL_PLN_CLIENT_GLOBAL.clone(),
        }
    }
}

#[pymethods]
impl WrappedAPIClient {
    #[new]
    fn new() -> Self {
        Default::default()
    }

    fn login(&self, py: Python) -> Result<(), ApiError> {
        py.enter_rust(|| RUNTIME.block_on(self.client.login())?)
    }

    fn clear_authentication(&self, py: Python) {
        let _ = py.enter_rust_ok(|| self.client.clear_authentication());
    }

    fn get_auth_header(&self, py: Python) -> Result<String, ApiError> {
        py.enter_rust(|| RUNTIME.block_on(self.client.get_auth_header())?)
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
            RUNTIME.block_on(
                self.client
                    .authenticate(client_id, client_secret, interactive),
            )?
        })
    }
}
