#![allow(clippy::result_large_err)]

use std::sync::Arc;

use async_trait::async_trait;
#[cfg(any(test, feature = "test-utils"))]
use mockall::automock;
use polars_axum_models::{
    ComputeClusterNodeInfoModel, ComputeClusterPublicInfoModel, ComputeModel, ComputeTokenModel,
    DeleteWorkspaceModel, GetClusterFilterArgs, GetQueryArgs, ManifestModel, ManifestQueryArgs,
    OrganizationCreateArgs, OrganizationModel, QueryWithStateTimingAndResultModel,
    QueryWithStateTimingModel, RegisterComputeClusterManifestArgs, StartComputeClusterArgs,
    StartComputeClusterManifestArgs, UserModel, WorkSpaceArgs, WorkSpaceTokenBodyArgs,
    WorkspaceAPITokenModel, WorkspaceApiTokenWithNameModel, WorkspaceClusterDefaultsModel,
    WorkspaceModel, WorkspaceSetupUrlModel, WorkspaceWithUrlModel,
};
use uuid::Uuid;

use crate::error::ApiError;

pub type Client = Arc<dyn ControlPlaneClient>;

#[cfg_attr(any(test, feature = "test-utils"), automock)]
#[async_trait]
pub trait ControlPlaneClient: Send + Sync {
    // --- Auth ---
    async fn login(&self) -> Result<(), ApiError>;
    async fn authenticate(
        &self,
        client_id: Option<String>,
        client_secret: Option<String>,
        interactive: bool,
    ) -> Result<(), ApiError>;
    fn clear_authentication(&self);
    async fn get_auth_header(&self) -> Result<String, ApiError>;

    // --- Organization ---
    async fn get_organization(&self, organization_id: Uuid) -> Result<OrganizationModel, ApiError>;
    async fn create_organization(
        &self,
        params: OrganizationCreateArgs,
    ) -> Result<OrganizationModel, ApiError>;
    async fn delete_organization(&self, organization_id: Uuid) -> Result<(), ApiError>;
    async fn get_organizations(
        &self,
        name: Option<String>,
    ) -> Result<Vec<OrganizationModel>, ApiError>;

    // --- Workspace ---
    async fn get_workspace(&self, workspace_id: Uuid) -> Result<WorkspaceModel, ApiError>;
    async fn get_workspaces(
        &self,
        name: Option<String>,
        organization_id: Option<Uuid>,
    ) -> Result<Vec<WorkspaceModel>, ApiError>;
    async fn get_cluster_defaults(
        &self,
        workspace_id: Uuid,
    ) -> Result<Option<WorkspaceClusterDefaultsModel>, ApiError>;
    async fn set_cluster_defaults(
        &self,
        workspace_id: Uuid,
        params: &WorkspaceClusterDefaultsModel,
    ) -> Result<(), ApiError>;

    // --- Workspace (AWS) ---
    async fn create_aws_workspace(
        &self,
        params: WorkSpaceArgs,
    ) -> Result<WorkspaceWithUrlModel, ApiError>;
    async fn get_aws_workspace_setup_url(
        &self,
        workspace_id: Uuid,
    ) -> Result<WorkspaceSetupUrlModel, ApiError>;
    async fn delete_aws_workspace(
        &self,
        workspace_id: Uuid,
    ) -> Result<Option<DeleteWorkspaceModel>, ApiError>;

    // --- Workspace (On-Prem) ---
    async fn create_on_prem_workspace(
        &self,
        params: WorkSpaceArgs,
    ) -> Result<WorkspaceModel, ApiError>;
    async fn delete_on_prem_workspace(&self, workspace_id: Uuid) -> Result<(), ApiError>;

    // --- Compute ---
    async fn find_compute_cluster_manifest(
        &self,
        workspace_id: Uuid,
        params: ManifestQueryArgs,
    ) -> Result<ManifestModel, ApiError>;
    async fn get_compute_cluster(
        &self,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<ComputeModel, ApiError>;
    async fn stop_compute_cluster(
        &self,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<(), ApiError>;
    async fn get_public_server_info(
        &self,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<ComputeClusterPublicInfoModel, ApiError>;
    async fn get_compute_cluster_token(
        &self,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<ComputeTokenModel, ApiError>;
    async fn get_compute_cluster_nodes(
        &self,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<Vec<ComputeClusterNodeInfoModel>, ApiError>;
    async fn register_compute_cluster_manifest(
        &self,
        workspace_id: Uuid,
        params: RegisterComputeClusterManifestArgs,
    ) -> Result<ManifestModel, ApiError>;
    async fn unregister_compute_cluster_manifest(
        &self,
        workspace_id: Uuid,
        name: String,
    ) -> Result<(), ApiError>;
    async fn start_compute_cluster_manifest(
        &self,
        workspace_id: Uuid,
        params: StartComputeClusterManifestArgs,
    ) -> Result<ComputeModel, ApiError>;
    async fn start_compute_cluster(
        &self,
        workspace_id: Uuid,
        params: StartComputeClusterArgs,
    ) -> Result<ComputeModel, ApiError>;
    async fn get_compute_clusters(
        &self,
        workspace_id: Uuid,
        filters: GetClusterFilterArgs,
    ) -> Result<Vec<ComputeModel>, ApiError>;

    // --- Query ---
    async fn get_query(
        &self,
        workspace_id: Uuid,
        query_id: Uuid,
    ) -> Result<QueryWithStateTimingAndResultModel, ApiError>;
    async fn cancel_query(&self, workspace_id: Uuid, query_id: Uuid) -> Result<(), ApiError>;
    async fn get_queries(
        &self,
        workspace_id: Uuid,
        filters: GetQueryArgs,
    ) -> Result<Vec<QueryWithStateTimingModel>, ApiError>;

    // --- User ---
    async fn get_logged_in_user(&self) -> Result<UserModel, ApiError>;

    // --- Service Accounts / Workspace Tokens ---
    async fn get_workspace_tokens(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<WorkspaceApiTokenWithNameModel>, ApiError>;
    async fn create_workspace_token(
        &self,
        workspace_id: Uuid,
        params: WorkSpaceTokenBodyArgs,
    ) -> Result<WorkspaceAPITokenModel, ApiError>;
    async fn delete_workspace_token(
        &self,
        workspace_id: Uuid,
        token_id: Uuid,
    ) -> Result<(), ApiError>;
}
