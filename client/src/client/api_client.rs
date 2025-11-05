#![allow(clippy::result_large_err)]

use polars_axum_models::Paginated;
use polars_backend_client::client::ApiClient;
use polars_backend_client::error::ApiError as ClientApiError;
use protos_common::tonic::{Request, Status};
use pyo3::exceptions::PyValueError;
use pyo3::{Python, pyclass, pymethods};

use crate::VERSIONS;
use crate::client::grpc::{ControlPlaneGRPCClient, get_control_plane_client};
use crate::client::login::login_new;
use crate::client::{AuthError, AuthMethod, AuthToken};
use crate::constants::{API_ADDR, RUNTIME};
use crate::error::ApiError;

#[pyclass(name = "ApiClient")]
pub struct WrappedAPIClient {
    rest: ApiClient,
    grpc: ControlPlaneGRPCClient,
    auth_token: Option<AuthToken>,
}

impl Default for WrappedAPIClient {
    fn default() -> Self {
        let versions = VERSIONS.get().unwrap().clone().unwrap();
        let rest =
            ApiClient::new_with_versions("PLACEHOLDER".to_string(), API_ADDR.clone(), versions.1);
        let grpc = get_control_plane_client();
        WrappedAPIClient {
            rest,
            grpc,
            auth_token: None,
        }
    }
}

impl WrappedAPIClient {
    async fn set_or_refresh_auth(&mut self) -> Result<(), AuthError> {
        let connection_pool = self.rest.client.clone();
        if let Some(token) = self.auth_token.as_mut() {
            token.refresh(connection_pool).await?;
        } else {
            self.auth_token = Some(AuthToken::new(connection_pool).await?)
        }
        let auth_header = self.auth_token.as_ref().unwrap().to_auth_header();
        self.rest.set_auth_header(auth_header);
        Ok(())
    }

    fn get_auth_method(&self) -> Option<AuthMethod> {
        self.auth_token.as_ref().map(|t| t.method())
    }

    pub(crate) fn call<'a, T: Send, F, F2>(
        &'a mut self,
        py: Python<'a>,
        f: F,
    ) -> Result<T, ApiError>
    where
        F: FnOnce(&'a ApiClient) -> F2,
        F2: Future<Output = Result<T, ClientApiError>> + Send + 'a,
    {
        RUNTIME.block_on(py, self.set_or_refresh_auth())??;
        RUNTIME
            .block_on(py, f(&self.rest))?
            .map_err(|e| ApiError::from_with_auth_method(e, self.get_auth_method()))
    }

    pub(crate) fn call_grpc<'a, T: Send, U, F, F2>(
        &'a mut self,
        py: Python<'a>,
        f: F,
        mut request: Request<U>,
    ) -> Result<T, ApiError>
    where
        F: FnOnce(ControlPlaneGRPCClient, Request<U>) -> F2,
        F2: Future<Output = Result<T, Status>> + Send + 'a,
    {
        RUNTIME.block_on(py, self.set_or_refresh_auth())??;
        request
            .metadata_mut()
            .insert("authorization", self.rest.auth_header.parse().unwrap());
        RUNTIME
            .block_on(py, f(self.grpc.clone(), request))?
            .map_err(ApiError::from)
    }

    pub(crate) fn call_paginated<'a, T: Send, F, F2>(
        &'a mut self,
        py: Python<'a>,
        f: F,
    ) -> Result<Vec<T>, ApiError>
    where
        F: Fn(&'a ApiClient, i64) -> F2,
        F2: Future<Output = Result<Paginated<T>, ClientApiError>> + Send + 'a,
    {
        RUNTIME.block_on(py, self.set_or_refresh_auth())??;
        let mut results = Vec::with_capacity(25);

        for page in 1..10 {
            let mut paginated_response = RUNTIME
                .block_on(py, f(&self.rest, page))?
                .map_err(|e| ApiError::from_with_auth_method(e, self.get_auth_method()))?;

            results.append(&mut paginated_response.result);

            if page >= paginated_response.pagination.total_pages {
                break;
            }
        }
        Ok(results)
    }
}

#[pymethods]
impl WrappedAPIClient {
    #[new]
    fn new() -> Self {
        Default::default()
    }

    fn login(&mut self, py: Python<'_>) -> Result<(), ApiError> {
        let token = RUNTIME.block_on(py, login_new(self.rest.client.clone()))??;
        self.auth_token = Some(token);
        Ok(())
    }

    fn clear_authentication(&mut self) {
        self.auth_token = None
    }

    fn get_auth_header(&mut self, py: Python<'_>) -> Result<String, ApiError> {
        self.call(py, |_api_client: &ApiClient| async { Ok(()) })?;
        Ok(self.rest.clone().auth_header)
    }

    #[pyo3(signature = (client_id=None, client_secret=None, interactive=true))]
    fn authenticate(
        &mut self,
        py: Python<'_>,
        client_id: Option<String>,
        client_secret: Option<String>,
        interactive: bool,
    ) -> Result<(), ApiError> {
        match (client_id.clone(), client_secret) {
            (Some(client_id), Some(client_secret)) => {
                let client_clone = self.rest.client.clone();
                let token = RUNTIME.block_on(py, async move {
                    AuthToken::from_service_account(client_id, client_secret, client_clone).await
                })??;
                self.auth_token = Some(token);
            },
            (Some(_), None) | (None, Some(_)) => {
                return Err(PyValueError::new_err(
                    "Client Id and Secret must either both be set or none at all.",
                )
                .into());
            },
            _ => (),
        };

        match self.call(py, |client: &ApiClient| client.get_logged_in_user()) {
            Ok(_) => Ok(()),
            Err(e) => {
                if !interactive || client_id.is_some() {
                    Err(e)
                } else {
                    self.login(py)
                }
            },
        }
    }
}
