#![allow(clippy::result_large_err)]

use std::path::PathBuf;
use std::sync::RwLock;

use polars_axum_models::Paginated;
use polars_backend_client::client::ApiClient;
use polars_backend_client::error::ApiError as ClientApiError;
use protos_common::tonic::{Request, Status};
use pyo3::exceptions::PyValueError;

use crate::constants::{API_ADDR, RUNTIME};
use crate::error::ApiError;
use crate::grpc::{ControlPlaneGRPCClient, get_control_plane_client};
use crate::{AuthError, AuthMethod, AuthToken, VERSIONS, login_new};

pub struct AutoRefreshApiControlPlaneClient {
    rest: ApiClient,
    grpc: ControlPlaneGRPCClient,
    auth_token: RwLock<Option<AuthToken>>,
    override_token: RwLock<Option<String>>,
    override_token_path: RwLock<Option<PathBuf>>,
}

impl Default for AutoRefreshApiControlPlaneClient {
    fn default() -> Self {
        let versions = VERSIONS.get().unwrap().clone().unwrap();
        let rest =
            ApiClient::new_with_versions("PLACEHOLDER".to_string(), API_ADDR.clone(), versions.1);
        let grpc = get_control_plane_client();
        AutoRefreshApiControlPlaneClient {
            rest,
            grpc,
            auth_token: Default::default(),
            override_token: RwLock::new(None),
            override_token_path: RwLock::new(None),
        }
    }
}

impl AutoRefreshApiControlPlaneClient {
    pub fn rest(&self) -> &ApiClient {
        &self.rest
    }

    pub fn set_token_override(&self, token: String) {
        *self.override_token.write().unwrap() = Some(token);
    }

    pub fn set_token_path_override(&self, path: PathBuf) {
        *self.override_token_path.write().unwrap() = Some(path);
    }

    async fn set_or_refresh_auth(&self) -> Result<(), AuthError> {
        let connection_pool = self.rest.client.clone();

        let auth_token = self.auth_token.read().unwrap().clone();

        let auth_token = if let Some(token) = auth_token {
            if let Some(new_token) = token.check_and_refresh(connection_pool).await? {
                *self.auth_token.write().unwrap() = Some(new_token.clone());
                new_token
            } else {
                token
            }
        } else {
            let token = self.override_token.read().unwrap().clone();
            let token_path = self.override_token_path.read().unwrap().clone();
            if let Some(token) = token {
                AuthToken::new_with_token(token)?
            } else {
                AuthToken::new_from_env_or_disk(token_path, connection_pool).await?
            }
        };
        let auth_header = auth_token.to_auth_header();

        self.rest.set_auth_header(auth_header);
        Ok(())
    }

    pub async fn login(&self) -> Result<(), ApiError> {
        let token = login_new(self.rest.client.clone()).await?;
        *self.auth_token.write().unwrap() = Some(token);
        Ok(())
    }

    pub fn clear_authentication(&self) {
        *self.auth_token.write().unwrap() = None
    }

    fn get_auth_method(&self) -> Option<AuthMethod> {
        self.auth_token.read().unwrap().as_ref().map(|t| t.method())
    }

    pub async fn call_async<'a, T: Send, F, F2>(&'a self, f: F) -> Result<T, ApiError>
    where
        F: FnOnce(&'a ApiClient) -> F2 + Send,
        F2: Future<Output = Result<T, ClientApiError>> + Send + 'a,
    {
        self.set_or_refresh_auth().await?;
        f(&self.rest)
            .await
            .map_err(|e| ApiError::from_with_auth_method(e, self.get_auth_method()))
    }

    pub fn call<'a, T: Send, F, F2>(&'a self, f: F) -> Result<T, ApiError>
    where
        F: FnOnce(&'a ApiClient) -> F2 + Send,
        F2: Future<Output = Result<T, ClientApiError>> + Send + 'a,
    {
        RUNTIME.block_on(self.call_async(f))?
    }

    pub async fn call_grpc_async<'a, T: Send, U, F, F2>(
        &'a self,
        f: F,
        mut request: Request<U>,
    ) -> Result<T, ApiError>
    where
        F: FnOnce(ControlPlaneGRPCClient, Request<U>) -> F2 + Send,
        F2: Future<Output = Result<T, Status>> + Send + 'a,
    {
        self.set_or_refresh_auth().await?;
        request.metadata_mut().insert(
            "authorization",
            self.rest.auth_header.read().unwrap().parse().unwrap(),
        );
        f(self.grpc.clone(), request).await.map_err(ApiError::from)
    }

    pub fn call_grpc<'a, T: Send, U, F, F2>(
        &'a self,
        f: F,
        request: Request<U>,
    ) -> Result<T, ApiError>
    where
        F: FnOnce(ControlPlaneGRPCClient, Request<U>) -> F2 + Send,
        F2: Future<Output = Result<T, Status>> + Send + 'a,
        U: Send,
    {
        RUNTIME.block_on(self.call_grpc_async(f, request))?
    }

    pub async fn call_paginated_async<'a, T: Send, F, F2>(
        &'a self,
        f: F,
    ) -> Result<Vec<T>, ApiError>
    where
        F: Fn(&'a ApiClient, i64) -> F2 + Send,
        F2: Future<Output = Result<Paginated<T>, ClientApiError>> + Send + 'a,
    {
        self.set_or_refresh_auth().await?;
        let mut results = Vec::with_capacity(25);

        for page in 1..10 {
            let mut paginated_response = f(&self.rest, page)
                .await
                .map_err(|e| ApiError::from_with_auth_method(e, self.get_auth_method()))?;

            results.append(&mut paginated_response.result);

            if page >= paginated_response.pagination.total_pages {
                break;
            }
        }
        Ok(results)
    }

    pub fn call_paginated<'a, T: Send, F, F2>(&'a self, f: F) -> Result<Vec<T>, ApiError>
    where
        F: Fn(&'a ApiClient, i64) -> F2 + Send,
        F2: Future<Output = Result<Paginated<T>, ClientApiError>> + Send + 'a,
    {
        RUNTIME.block_on(self.call_paginated_async(f))?
    }

    pub async fn authenticate(
        &self,
        client_id: Option<String>,
        client_secret: Option<String>,
        interactive: bool,
    ) -> Result<(), ApiError> {
        match (client_id.clone(), client_secret) {
            (Some(client_id), Some(client_secret)) => {
                let client_clone = self.rest.client.clone();
                let token =
                    AuthToken::from_service_account(client_id, client_secret, client_clone).await?;
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

        match self
            .call_async(|client: &ApiClient| client.get_logged_in_user())
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                if !interactive || client_id.is_some() {
                    Err(e)
                } else {
                    self.login().await
                }
            },
        }
    }
}

pub static CTRL_PLN_CLIENT_GLOBAL: std::sync::LazyLock<AutoRefreshApiControlPlaneClient> =
    std::sync::LazyLock::new(|| {
        // `connect_lazy()` in tonic calls `tokio::task::spawn` internally so we need to enter the runtime.
        let _guard = RUNTIME.0.enter();
        AutoRefreshApiControlPlaneClient::default()
    });
