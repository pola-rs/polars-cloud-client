#![allow(clippy::result_large_err)]

use std::sync::RwLock;

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
use crate::entry::EnterRustExt;
use crate::error::ApiError;

pub struct AutoRefreshApiClient {
    rest: ApiClient,
    grpc: ControlPlaneGRPCClient,
    auth_token: RwLock<Option<AuthToken>>,
}

impl Default for AutoRefreshApiClient {
    fn default() -> Self {
        let versions = VERSIONS.get().unwrap().clone().unwrap();
        let rest =
            ApiClient::new_with_versions("PLACEHOLDER".to_string(), API_ADDR.clone(), versions.1);
        let grpc = get_control_plane_client();
        AutoRefreshApiClient {
            rest,
            grpc,
            auth_token: Default::default(),
        }
    }
}

impl AutoRefreshApiClient {
    pub fn rest(&self) -> &ApiClient {
        &self.rest
    }

    async fn set_or_refresh_auth(&self) -> Result<(), AuthError> {
        let connection_pool = self.rest.client.clone();

        let auth_token = self.auth_token.read().unwrap().clone();

        let auth_token = if let Some(token) = auth_token {
            if let Some(new_token) = token.refresh(connection_pool).await? {
                *self.auth_token.write().unwrap() = Some(new_token.clone());
                new_token
            } else {
                token
            }
        } else {
            AuthToken::new(connection_pool).await?
        };
        let auth_header = auth_token.to_auth_header();

        self.rest.set_auth_header(auth_header);
        Ok(())
    }

    fn login(&self) -> Result<(), ApiError> {
        let token = RUNTIME.block_on(login_new(self.rest.client.clone()))??;
        *self.auth_token.write().unwrap() = Some(token);
        Ok(())
    }

    fn clear_authentication(&self) {
        *self.auth_token.write().unwrap() = None
    }

    fn get_auth_method(&self) -> Option<AuthMethod> {
        self.auth_token.read().unwrap().as_ref().map(|t| t.method())
    }

    pub(crate) fn call<'a, T: Send, F, F2>(&'a self, f: F) -> Result<T, ApiError>
    where
        F: FnOnce(&'a ApiClient) -> F2,
        F2: Future<Output = Result<T, ClientApiError>> + Send + 'a,
    {
        RUNTIME.block_on(self.set_or_refresh_auth())??;
        RUNTIME
            .block_on(f(&self.rest))?
            .map_err(|e| ApiError::from_with_auth_method(e, self.get_auth_method()))
    }

    pub(crate) fn call_grpc<'a, T: Send, U, F, F2>(
        &'a self,
        f: F,
        mut request: Request<U>,
    ) -> Result<T, ApiError>
    where
        F: FnOnce(ControlPlaneGRPCClient, Request<U>) -> F2,
        F2: Future<Output = Result<T, Status>> + Send + 'a,
    {
        RUNTIME.block_on(self.set_or_refresh_auth())??;
        request.metadata_mut().insert(
            "authorization",
            self.rest.auth_header.read().unwrap().parse().unwrap(),
        );
        RUNTIME
            .block_on(f(self.grpc.clone(), request))?
            .map_err(ApiError::from)
    }

    pub(crate) fn call_paginated<'a, T: Send, F, F2>(&'a self, f: F) -> Result<Vec<T>, ApiError>
    where
        F: Fn(&'a ApiClient, i64) -> F2,
        F2: Future<Output = Result<Paginated<T>, ClientApiError>> + Send + 'a,
    {
        RUNTIME.block_on(self.set_or_refresh_auth())??;
        let mut results = Vec::with_capacity(25);

        for page in 1..10 {
            let mut paginated_response = RUNTIME
                .block_on(f(&self.rest, page))?
                .map_err(|e| ApiError::from_with_auth_method(e, self.get_auth_method()))?;

            results.append(&mut paginated_response.result);

            if page >= paginated_response.pagination.total_pages {
                break;
            }
        }
        Ok(results)
    }

    fn authenticate(
        &self,
        client_id: Option<String>,
        client_secret: Option<String>,
        interactive: bool,
    ) -> Result<(), ApiError> {
        match (client_id.clone(), client_secret) {
            (Some(client_id), Some(client_secret)) => {
                let client_clone = self.rest.client.clone();
                let token = RUNTIME.block_on(async move {
                    AuthToken::from_service_account(client_id, client_secret, client_clone).await
                })??;
                *self.auth_token.write().unwrap() = Some(token);
            },
            (Some(_), None) | (None, Some(_)) => {
                return Err(PyValueError::new_err(
                    "Client Id and Secret must either both be set or none at all.",
                )
                .into());
            },
            _ => (),
        };

        match self.call(|client: &ApiClient| client.get_logged_in_user()) {
            Ok(_) => Ok(()),
            Err(e) => {
                if !interactive || client_id.is_some() {
                    Err(e)
                } else {
                    self.login()
                }
            },
        }
    }
}

pub static CLIENT_GLOBAL: std::sync::LazyLock<AutoRefreshApiClient> =
    std::sync::LazyLock::new(Default::default);

#[pyclass(name = "ApiClient")]
#[derive(Clone, Default)]
pub struct WrappedAPIClient {}

#[pymethods]
impl WrappedAPIClient {
    #[new]
    fn new() -> Self {
        Default::default()
    }

    fn login(&self, py: Python) -> Result<(), ApiError> {
        py.enter_rust(|| CLIENT_GLOBAL.login())
    }

    fn clear_authentication(&self, py: Python) {
        let _ = py.enter_rust_ok(|| CLIENT_GLOBAL.clear_authentication());
    }

    fn get_auth_header(&self, py: Python) -> Result<String, ApiError> {
        let out = py.enter_rust(|| {
            CLIENT_GLOBAL
                .call(|_api_client: &ApiClient| async { Ok(()) })
                .map(|_| CLIENT_GLOBAL.rest.auth_header.read().unwrap().clone())
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
        py.enter_rust(|| CLIENT_GLOBAL.authenticate(client_id, client_secret, interactive))
    }
}
