#![allow(clippy::result_large_err)]

use std::path::PathBuf;
use std::sync::RwLock;

use async_trait::async_trait;
use polars_axum_models::*;
use polars_backend_client::client::ApiClient;
use polars_backend_client::error::ApiError as ClientApiError;
use protos_common::tonic::{Request, Status};
use pyo3::exceptions::PyValueError;
use uuid::Uuid;

use crate::client_trait::ControlPlaneClient;
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

    async fn call<'a, T: Send, F, F2>(&'a self, f: F) -> Result<T, ApiError>
    where
        F: FnOnce(&'a ApiClient) -> F2 + Send,
        F2: Future<Output = Result<T, ClientApiError>> + Send + 'a,
    {
        self.set_or_refresh_auth().await?;
        f(&self.rest)
            .await
            .map_err(|e| ApiError::from_with_auth_method(e, self.get_auth_method()))
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

    async fn call_paginated<'a, T: Send, F, F2>(&'a self, f: F) -> Result<Vec<T>, ApiError>
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
            .call(|client: &ApiClient| client.get_logged_in_user())
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

#[async_trait]
impl ControlPlaneClient for AutoRefreshApiControlPlaneClient {
    async fn login(&self) -> Result<(), ApiError> {
        AutoRefreshApiControlPlaneClient::login(self).await
    }

    async fn authenticate(
        &self,
        client_id: Option<String>,
        client_secret: Option<String>,
        interactive: bool,
    ) -> Result<(), ApiError> {
        AutoRefreshApiControlPlaneClient::authenticate(self, client_id, client_secret, interactive)
            .await
    }

    fn clear_authentication(&self) {
        AutoRefreshApiControlPlaneClient::clear_authentication(self)
    }

    async fn get_auth_header(&self) -> Result<String, ApiError> {
        self.call(|_client: &ApiClient| async { Ok(()) }).await?;
        Ok(self.rest.auth_header.read().unwrap().clone())
    }

    async fn get_organization(&self, organization_id: Uuid) -> Result<OrganizationModel, ApiError> {
        self.call(|client| client.get_organization(organization_id))
            .await
    }

    async fn create_organization(
        &self,
        params: OrganizationCreateArgs,
    ) -> Result<OrganizationModel, ApiError> {
        self.call(move |client| client.create_organization(params))
            .await
    }

    async fn delete_organization(&self, organization_id: Uuid) -> Result<(), ApiError> {
        self.call(move |client| client.delete_organization(organization_id))
            .await
    }

    async fn get_organizations(
        &self,
        name: Option<String>,
    ) -> Result<Vec<OrganizationModel>, ApiError> {
        self.call_paginated(|client, page| {
            let pagination = Pagination {
                page,
                limit: 1000,
                offset: 0,
            };
            let query = OrganizationQueryArgs { name: name.clone() };
            client.get_organizations(pagination, query)
        })
        .await
    }

    async fn get_workspace(&self, workspace_id: Uuid) -> Result<WorkspaceModel, ApiError> {
        self.call(|client| client.get_workspace(workspace_id)).await
    }

    async fn get_workspaces(
        &self,
        name: Option<String>,
        organization_id: Option<Uuid>,
    ) -> Result<Vec<WorkspaceModel>, ApiError> {
        self.call_paginated(|client, page| {
            let pagination = Pagination {
                page,
                limit: 1000,
                offset: 0,
            };
            let query = WorkspaceQueryArgs {
                name: name.clone(),
                organization_id,
            };
            client.get_workspaces(query, pagination)
        })
        .await
    }

    async fn get_cluster_defaults(
        &self,
        workspace_id: Uuid,
    ) -> Result<Option<WorkspaceClusterDefaultsModel>, ApiError> {
        self.call(|client| client.get_cluster_defaults(workspace_id))
            .await
    }

    async fn set_cluster_defaults(
        &self,
        workspace_id: Uuid,
        params: &WorkspaceClusterDefaultsModel,
    ) -> Result<(), ApiError> {
        self.call(|client| client.set_cluster_defaults(workspace_id, params))
            .await
    }

    async fn create_aws_workspace(
        &self,
        params: WorkSpaceArgs,
    ) -> Result<WorkspaceWithUrlModel, ApiError> {
        self.call(|client| client.create_aws_workspace(params))
            .await
    }

    async fn get_aws_workspace_setup_url(
        &self,
        workspace_id: Uuid,
    ) -> Result<WorkspaceSetupUrlModel, ApiError> {
        self.call(|client| client.get_aws_workspace_setup_url(workspace_id))
            .await
    }

    async fn delete_aws_workspace(
        &self,
        workspace_id: Uuid,
    ) -> Result<Option<DeleteWorkspaceModel>, ApiError> {
        self.call(|client| client.delete_aws_workspace(workspace_id))
            .await
    }

    // --- Compute ---

    async fn find_compute_cluster_manifest(
        &self,
        workspace_id: Uuid,
        params: ManifestQueryArgs,
    ) -> Result<ManifestModel, ApiError> {
        self.call(|client| client.find_compute_cluster_manifest(workspace_id, params))
            .await
    }

    async fn get_compute_cluster(
        &self,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<ComputeModel, ApiError> {
        self.call(|client| client.get_compute_cluster(workspace_id, compute_id))
            .await
    }

    async fn stop_compute_cluster(
        &self,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<(), ApiError> {
        self.call(|client| client.stop_compute_cluster(workspace_id, compute_id))
            .await
    }

    async fn get_public_server_info(
        &self,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<ComputeClusterPublicInfoModel, ApiError> {
        self.call(|client| client.get_public_server_info(workspace_id, compute_id))
            .await
    }

    async fn get_compute_cluster_token(
        &self,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<ComputeTokenModel, ApiError> {
        self.call(|client| client.get_compute_cluster_token(workspace_id, compute_id))
            .await
    }

    async fn get_compute_cluster_nodes(
        &self,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<Vec<ComputeClusterNodeInfoModel>, ApiError> {
        self.call_paginated(|client, page| {
            let pagination = Pagination {
                page,
                limit: 1000,
                offset: 0,
            };
            client.get_compute_cluster_nodes(workspace_id, compute_id, pagination)
        })
        .await
    }

    async fn register_compute_cluster_manifest(
        &self,
        workspace_id: Uuid,
        params: RegisterComputeClusterManifestArgs,
    ) -> Result<ManifestModel, ApiError> {
        self.call(|client| client.register_compute_cluster_manifest(workspace_id, params))
            .await
    }

    async fn unregister_compute_cluster_manifest(
        &self,
        workspace_id: Uuid,
        name: String,
    ) -> Result<(), ApiError> {
        self.call(move |client| client.unregister_compute_cluster_manifest(workspace_id, name))
            .await
    }

    async fn start_compute_cluster_manifest(
        &self,
        workspace_id: Uuid,
        params: StartComputeClusterManifestArgs,
    ) -> Result<ComputeModel, ApiError> {
        self.call(|client| client.start_compute_cluster_manifest(workspace_id, params))
            .await
    }

    async fn start_compute_cluster(
        &self,
        workspace_id: Uuid,
        params: StartComputeClusterArgs,
    ) -> Result<ComputeModel, ApiError> {
        self.call(|client| client.start_compute_cluster(workspace_id, params))
            .await
    }

    async fn get_compute_clusters(
        &self,
        workspace_id: Uuid,
        filters: GetClusterFilterArgs,
    ) -> Result<Vec<ComputeModel>, ApiError> {
        self.call_paginated(|client, page| {
            let pagination = Pagination {
                page,
                limit: 1000,
                offset: 0,
            };
            let filters = GetClusterFilterArgs {
                status: filters.status.clone(),
                current_user_only: filters.current_user_only,
            };
            client.get_compute_clusters(workspace_id, filters, pagination)
        })
        .await
    }

    async fn get_query(
        &self,
        workspace_id: Uuid,
        query_id: Uuid,
    ) -> Result<QueryWithStateTimingAndResultModel, ApiError> {
        self.call(|client| client.get_query(workspace_id, query_id))
            .await
    }

    async fn cancel_query(&self, workspace_id: Uuid, query_id: Uuid) -> Result<(), ApiError> {
        self.call(|client| client.cancel_query(workspace_id, query_id))
            .await
    }

    async fn get_queries(
        &self,
        workspace_id: Uuid,
        filters: GetQueryArgs,
    ) -> Result<Vec<QueryWithStateTimingModel>, ApiError> {
        self.call_paginated(|client, page| {
            let pagination = Pagination {
                page,
                limit: 1000,
                offset: 0,
            };
            let filters = GetQueryArgs {
                cluster_id: filters.cluster_id,
                user_id: filters.user_id,
            };
            client.get_queries(workspace_id, filters, pagination)
        })
        .await
    }

    async fn get_logged_in_user(&self) -> Result<UserModel, ApiError> {
        self.call(|client| client.get_logged_in_user()).await
    }

    async fn get_workspace_tokens(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<WorkspaceApiTokenWithNameModel>, ApiError> {
        self.call(|client| client.get_workspace_tokens(workspace_id))
            .await
    }

    async fn create_workspace_token(
        &self,
        workspace_id: Uuid,
        params: WorkSpaceTokenBodyArgs,
    ) -> Result<WorkspaceAPITokenModel, ApiError> {
        self.call(move |client| client.create_workspace_token(workspace_id, params))
            .await
    }

    async fn delete_workspace_token(
        &self,
        workspace_id: Uuid,
        token_id: Uuid,
    ) -> Result<(), ApiError> {
        self.call(move |client| client.delete_workspace_token(workspace_id, token_id))
            .await
    }
}
