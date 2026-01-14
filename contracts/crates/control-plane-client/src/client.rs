use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use chrono::SecondsFormat;
use http::header::AUTHORIZATION;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use polars_axum_models::*;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
use reqwest::redirect;
use reqwest_middleware::ClientBuilder;
use uuid::Uuid;

use crate::builder::ApiRequestBuilder;
use crate::error::*;
use crate::middleware::RetryTransientMiddleware;

#[cfg_attr(feature = "pyo3", pyclass)]
#[derive(Clone)]
pub struct ApiClient {
    pub client: reqwest_middleware::ClientWithMiddleware,
    pub address: String,
    pub auth_header: Arc<RwLock<String>>,
}

#[derive(Clone)]
pub struct Versions {
    pub polars: HeaderValue,
    pub polars_cloud: HeaderValue,
}

impl Versions {
    pub fn new(polars_version: HeaderValue, polars_cloud_version: HeaderValue) -> Self {
        Self {
            polars: polars_version,
            polars_cloud: polars_cloud_version,
        }
    }
}

pub struct ApiClientBuilder {
    builder: reqwest::ClientBuilder,
    retry_middleware: Option<RetryTransientMiddleware>,
}

impl Default for ApiClientBuilder {
    fn default() -> Self {
        Self {
            builder: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(30))
                .user_agent(user_agent(None))
                .http2_keep_alive_timeout(Duration::from_secs(15)),
            retry_middleware: None,
        }
    }
}

pub fn user_agent(versions: Option<&Versions>) -> HeaderValue {
    let mut user_agent = BytesMut::from("polars-cloud");
    if let Some(versions) = versions {
        user_agent.put_u8(b'/');
        user_agent.extend_from_slice(versions.polars_cloud.as_bytes());
    }
    HeaderValue::from_maybe_shared(user_agent.freeze()).unwrap()
}

impl ApiClientBuilder {
    pub fn with_redirect_policy(mut self, policy: redirect::Policy) -> Self {
        self.builder = self.builder.redirect(policy);
        self
    }

    pub fn with_versions(mut self, versions: Versions) -> Self {
        let mut headers = HeaderMap::new();
        let user_agent = user_agent(Some(&versions));
        self.builder = self.builder.user_agent(user_agent);
        headers.insert(
            HeaderName::from_static("x-client-version"),
            versions.polars_cloud,
        );
        headers.insert(HeaderName::from_static("x-polars-version"), versions.polars);
        self.builder = self.builder.default_headers(headers);
        self
    }

    pub fn with_retries(mut self) -> Self {
        self.retry_middleware = Some(RetryTransientMiddleware {
            max_retries: 4,
            wait_period: Duration::from_secs(1),
        });
        self
    }

    pub fn build(self, auth_header: String, address: String) -> ApiClient {
        let mut client_builder = ClientBuilder::new(self.builder.build().unwrap());
        if let Some(retry) = self.retry_middleware {
            client_builder = client_builder.with(retry);
        }

        ApiClient {
            client: client_builder.build(),
            address,
            auth_header: Arc::new(RwLock::new(auth_header)),
        }
    }
}

impl ApiClient {
    pub fn builder() -> ApiClientBuilder {
        ApiClientBuilder::default()
    }

    pub fn new(auth_header: String, address: String) -> Self {
        ApiClientBuilder::default()
            .with_retries()
            .build(auth_header, address)
    }

    pub fn new_with_versions(auth_header: String, address: String, versions: Versions) -> Self {
        ApiClientBuilder::default()
            .with_retries()
            .with_versions(versions)
            .build(auth_header, address)
    }

    pub fn new_without_retries(auth_header: String, address: String) -> Self {
        ApiClientBuilder::default().build(auth_header, address)
    }

    pub fn with_bearer_token(&self, bearer_token: String) -> Self {
        Self {
            client: self.client.clone(),
            address: self.address.clone(),
            auth_header: Arc::new(RwLock::new(format!("Bearer {bearer_token}"))),
        }
    }

    pub fn set_auth_header(&self, auth_header: String) {
        *self.auth_header.write().unwrap() = auth_header;
    }

    pub fn get(&self, endpoint: &str) -> ApiRequestBuilder<'_> {
        ApiRequestBuilder::new(
            &self.client,
            http::Method::GET,
            format!("{}{endpoint}", self.address),
            self.auth_header.read().unwrap().clone(),
        )
    }

    pub fn post(&self, endpoint: &str) -> ApiRequestBuilder<'_> {
        ApiRequestBuilder::new(
            &self.client,
            http::Method::POST,
            format!("{}{endpoint}", self.address),
            self.auth_header.read().unwrap().clone(),
        )
    }

    fn put(&self, endpoint: &str) -> ApiRequestBuilder<'_> {
        ApiRequestBuilder::new(
            &self.client,
            http::Method::PUT,
            format!("{}{endpoint}", self.address),
            self.auth_header.read().unwrap().clone(),
        )
    }

    fn delete(&self, endpoint: &str) -> ApiRequestBuilder<'_> {
        ApiRequestBuilder::new(
            &self.client,
            http::Method::DELETE,
            format!("{}{endpoint}", self.address),
            self.auth_header.read().unwrap().clone(),
        )
    }

    fn patch(&self, endpoint: &str) -> ApiRequestBuilder<'_> {
        ApiRequestBuilder::new(
            &self.client,
            http::Method::PATCH,
            format!("{}{endpoint}", self.address),
            self.auth_header.read().unwrap().clone(),
        )
    }

    pub async fn delete_workspace(
        &self,
        workspace_id: Uuid,
    ) -> Result<Option<DeleteWorkspaceSchema>> {
        let response = self
            .delete(&format!("/api/v1/workspace/aws/{workspace_id}"))
            .await?
            .response()?;

        if response.status() == StatusCode::NO_CONTENT {
            Ok(None)
        } else {
            Ok(Some(response.json().await?))
        }
    }

    pub async fn create_workspace(&self, params: WorkSpaceArgs) -> Result<WorkspaceWithUrlSchema> {
        self.post("/api/v1/workspace/aws")
            .json(params)
            .await?
            .json()
            .await
    }

    pub async fn get_workspace_setup_url(
        &self,
        workspace_id: Uuid,
    ) -> Result<WorkspaceSetupUrlSchema> {
        self.get(&format!("/api/v1/workspace/aws/{workspace_id}/setup-url"))
            .await?
            .json()
            .await
    }

    pub async fn get_available_instance_types(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<WorkspaceComputeInstanceTypeSchema>> {
        self.get(&format!(
            "/api/v1/workspace/aws/{workspace_id}/instance-types"
        ))
        .await?
        .json()
        .await
    }

    pub async fn find_compute_cluster_manifest(
        &self,
        workspace_id: Uuid,
        params: ManifestQuery,
    ) -> Result<ManifestSchema> {
        self.get(&format!("/api/v1/workspace/{workspace_id}/manifest/find"))
            .parameter("name", params.name.clone())
            .await?
            .json()
            .await
    }

    pub async fn delete_compute_cluster_manifests(
        &self,
        workspace_id: Uuid,
        manifest_id: Uuid,
    ) -> Result<()> {
        self.delete(&format!(
            "/api/v1/workspace/{workspace_id}/manifest/{manifest_id}"
        ))
        .await?
        .empty()
        .await
    }

    pub async fn get_compute_cluster_manifests(
        &self,
        workspace_id: Uuid,
        pagination: Pagination,
    ) -> Result<Paginated<ManifestSchema>> {
        self.get(&format!("/api/v1/workspace/{workspace_id}/manifest"))
            .pagination(&pagination)
            .await?
            .json()
            .await
    }

    pub async fn patch_compute_cluster_manifest(
        &self,
        workspace_id: Uuid,
        manifest_id: Uuid,
        params: PatchManifestArgs,
    ) -> Result<ManifestSchema> {
        self.patch(&format!(
            "/api/v1/workspace/{workspace_id}/manifest/{manifest_id}"
        ))
        .json(params)
        .await?
        .json()
        .await
    }

    pub async fn get_compute_clusters(
        &self,
        workspace_id: Uuid,
        filters: GetClusterFilterParams,
        pagination: Pagination,
    ) -> Result<Paginated<ComputeSchema>> {
        self.get(&format!("/api/v1/workspace/{workspace_id}/compute"))
            .parameter_vec_opt("status", filters.status)
            .pagination(&pagination)
            .await?
            .json()
            .await
    }

    pub async fn get_compute_cluster(
        &self,
        workspace_id: Uuid,
        cluster_id: Uuid,
    ) -> Result<ComputeSchema> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/compute/{cluster_id}"
        ))
        .await?
        .json()
        .await
    }

    pub async fn register_compute_cluster_manifest(
        &self,
        workspace_id: Uuid,
        params: RegisterComputeClusterArgs,
    ) -> Result<ManifestSchema> {
        self.post(&format!("/api/v1/workspace/{workspace_id}/manifest"))
            .json(params)
            .await?
            .json()
            .await
    }

    pub async fn unregister_compute_cluster_manifest(
        &self,
        workspace_id: Uuid,
        name: String,
    ) -> Result<()> {
        self.delete(&format!("/api/v1/workspace/{workspace_id}/manifest"))
            .parameter("name", name)
            .await?
            .empty()
            .await
    }

    pub async fn start_compute_cluster_manifest(
        &self,
        workspace_id: Uuid,
        params: StartComputeClusterManifestArgs,
    ) -> Result<ComputeSchema> {
        self.post(&format!("/api/v1/workspace/{workspace_id}/manifest/start"))
            .json(params)
            .await?
            .json()
            .await
    }

    pub async fn start_compute_cluster(
        &self,
        workspace_id: Uuid,
        params: StartComputeClusterArgs,
    ) -> Result<ComputeSchema> {
        self.post(&format!("/api/v1/workspace/{workspace_id}/compute/start"))
            .json(params)
            .await?
            .json()
            .await
    }

    pub async fn get_compute_cluster_token(
        &self,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<ComputeTokenSchema> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/compute/{compute_id}/token"
        ))
        .await?
        .json()
        .await
    }

    pub async fn stop_compute_cluster(&self, workspace_id: Uuid, cluster_id: Uuid) -> Result<()> {
        self.post(&format!(
            "/api/v1/workspace/{workspace_id}/compute/{cluster_id}/stop"
        ))
        .await?
        .empty()
        .await
    }

    pub async fn get_cluster_logs(
        &self,
        workspace_id: Uuid,
        cluster_id: Uuid,
    ) -> Result<TokenPaginated<Vec<AwsLogEventSchema>>> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/compute/{cluster_id}/logs"
        ))
        .await?
        .json()
        .await
    }

    pub async fn get_public_server_info(
        &self,
        workspace_id: Uuid,
        cluster_id: Uuid,
    ) -> Result<ComputeClusterPublicInfoSchema> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/compute/{cluster_id}/public_info"
        ))
        .await?
        .json()
        .await
    }

    pub async fn get_cluster_metrics(
        &self,
        workspace_id: Uuid,
        cluster_id: Uuid,
    ) -> Result<TokenPaginated<AwsMetricsSchema>> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/compute/{cluster_id}/metrics"
        ))
        .await?
        .json()
        .await
    }

    pub async fn add_compute_label(
        &self,
        workspace_id: Uuid,
        cluster_id: Uuid,
        label_id: Uuid,
    ) -> Result<()> {
        self.post(&format!(
            "/api/v1/workspace/{workspace_id}/compute/{cluster_id}/label"
        ))
        .json(&LabelIdSchema { label_id })
        .await?
        .empty()
        .await
    }

    pub async fn get_compute_labels(
        &self,
        workspace_id: Uuid,
        cluster_id: Uuid,
    ) -> Result<Vec<LabelOutputSchema>> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/compute/{cluster_id}/label"
        ))
        .await?
        .json()
        .await
    }

    pub async fn delete_compute_label(
        &self,
        workspace_id: Uuid,
        cluster_id: Uuid,
        label_id: Uuid,
    ) -> Result<()> {
        self.delete(&format!(
            "/api/v1/workspace/{workspace_id}/compute/{cluster_id}/label/{label_id}"
        ))
        .await?
        .empty()
        .await
    }

    pub async fn create_label(
        &self,
        workspace_id: Uuid,
        params: &LabelSchema,
    ) -> Result<LabelOutputSchema> {
        self.post(&format!("/api/v1/workspace/{workspace_id}/label"))
            .json(params)
            .await?
            .json()
            .await
    }

    pub async fn get_label(&self, workspace_id: Uuid, label_id: Uuid) -> Result<LabelOutputSchema> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/label/{label_id}"
        ))
        .await?
        .json()
        .await
    }

    pub async fn delete_label(&self, workspace_id: Uuid, label_id: Uuid) -> Result<()> {
        self.delete(&format!(
            "/api/v1/workspace/{workspace_id}/label/{label_id}"
        ))
        .await?
        .empty()
        .await
    }

    pub async fn update_label(
        &self,
        workspace_id: Uuid,
        label_id: Uuid,
        params: &LabelUpdateSchema,
    ) -> Result<()> {
        self.patch(&format!(
            "/api/v1/workspace/{workspace_id}/label/{label_id}"
        ))
        .json(params)
        .await?
        .empty()
        .await
    }

    pub async fn create_organization(
        &self,
        params: OrganizationCreateSchema,
    ) -> Result<OrganizationSchema> {
        self.post("/api/v1/organization")
            .json(params)
            .await?
            .json()
            .await
    }

    pub async fn get_organizations(
        &self,
        pagination: Pagination,
        filters: OrganizationQuery,
    ) -> Result<Paginated<OrganizationSchema>> {
        self.get("/api/v1/organization")
            .pagination(&pagination)
            .parameter_opt("name", filters.name)
            .await?
            .json()
            .await
    }

    pub async fn get_organization(&self, organization_id: Uuid) -> Result<OrganizationSchema> {
        self.get(&format!("/api/v1/organization/{organization_id}"))
            .await?
            .json()
            .await
    }

    pub async fn put_organization_avatar(&self, organization_id: Uuid, image: Bytes) -> Result<()> {
        let auth_header = self.auth_header.read().unwrap().clone();
        self.client
            .put(format!(
                "{}/api/v1/organization/{organization_id}/avatar",
                self.address
            ))
            .header(AUTHORIZATION, auth_header)
            .body(image)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    pub async fn delete_organization_avatar(&self, organization_id: Uuid) -> Result<()> {
        let auth_header = self.auth_header.read().unwrap().clone();
        self.client
            .delete(format!(
                "{}/api/v1/organization/{organization_id}/avatar",
                self.address
            ))
            .header(AUTHORIZATION, auth_header)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    pub async fn patch_organization_details(
        &self,
        organization_id: Uuid,
        params: &OrganizationDetails,
    ) -> Result<()> {
        self.patch(&format!("/api/v1/organization/{organization_id}"))
            .json(params)
            .await?
            .empty()
            .await
    }

    pub async fn delete_organization(&self, organization_id: Uuid) -> Result<()> {
        self.delete(&format!("/api/v1/organization/{organization_id}"))
            .await?
            .empty()
            .await
    }

    pub async fn post_organization_billing_details(
        &self,
        organization_id: Uuid,
        params: &BillingSubscribeSchema,
    ) -> Result<()> {
        self.post(&format!("/api/v1/organization/{organization_id}/billing"))
            .json(params)
            .await?
            .empty()
            .await
    }

    pub async fn get_organization_billing_details(
        &self,
        organization_id: Uuid,
    ) -> Result<OrganizationBillingDetailsSchema> {
        self.get(&format!("/api/v1/organization/{organization_id}/billing"))
            .await?
            .json()
            .await
    }

    pub async fn get_organization_billing_histogram(
        &self,
        organization_id: Uuid,
        window: &MetricWindow,
    ) -> Result<Vec<BillingHistogramSchema>> {
        let TimeWindow { start, end } = window.window;

        self.get(&format!(
            "/api/v1/organization/{organization_id}/billing/histogram"
        ))
        .parameter("start", start.to_rfc3339_opts(SecondsFormat::Millis, true))
        .parameter("end", end.to_rfc3339_opts(SecondsFormat::Millis, true))
        .parameter("interval", window.interval.num_seconds())
        .await?
        .json()
        .await
    }

    pub async fn create_organization_invite(
        &self,
        organization_id: Uuid,
        params: &InviteArgs,
    ) -> Result<OrganizationInviteWithUrlSchema> {
        self.post(&format!("/api/v1/organization/{organization_id}/invite"))
            .json(params)
            .await?
            .json()
            .await
    }

    pub async fn get_organization_invites(
        &self,
        pagination: &Pagination,
        organization_id: Uuid,
    ) -> Result<Paginated<OrganizationInviteSchema>> {
        self.get(&format!("/api/v1/organization/{organization_id}/invite"))
            .pagination(pagination)
            .await?
            .json()
            .await
    }

    pub async fn get_organization_invite(
        &self,
        organization_id: Uuid,
        invite_id: Uuid,
    ) -> Result<OrganizationInviteSchema> {
        self.get(&format!(
            "/api/v1/organization/{organization_id}/invite/{invite_id}"
        ))
        .await?
        .json()
        .await
    }

    pub async fn delete_organization_invite(
        &self,
        organization_id: Uuid,
        invite_id: Uuid,
    ) -> Result<()> {
        self.delete(&format!(
            "/api/v1/organization/{organization_id}/invite/{invite_id}"
        ))
        .await?
        .empty()
        .await
    }

    pub async fn redeem_organization_invite(&self, uri: &str) -> Result<()> {
        self.get(uri).await?.empty().await
    }

    pub async fn get_organization_members(
        &self,
        organization_id: Uuid,
        pagination: &Pagination,
    ) -> Result<Paginated<OrganizationUserSchema>> {
        self.get(&format!("/api/v1/organization/{organization_id}/member"))
            .pagination(pagination)
            .await?
            .json()
            .await
    }

    pub async fn get_organization_member(
        &self,
        organization_id: Uuid,
        user_id: Uuid,
    ) -> Result<OrganizationUserSchema> {
        self.get(&format!(
            "/api/v1/organization/{organization_id}/member/{user_id}"
        ))
        .await?
        .json()
        .await
    }

    pub async fn patch_organization_member_role(
        &self,
        organization_id: Uuid,
        user_id: Uuid,
        role: OrganizationRoleSchema,
    ) -> Result<()> {
        self.patch(&format!(
            "/api/v1/organization/{organization_id}/member/{user_id}/role"
        ))
        .json(OrganizationMemberRole { role })
        .await?
        .empty()
        .await
    }

    pub async fn remove_organization_member(
        &self,
        organization_id: Uuid,
        user_id: Uuid,
    ) -> Result<()> {
        self.delete(&format!(
            "/api/v1/organization/{organization_id}/member/{user_id}"
        ))
        .await?
        .empty()
        .await
    }

    pub async fn get_queries(
        &self,
        workspace_id: Uuid,
        filters: QueryParamsFilter,
        pagination: Pagination,
    ) -> Result<Paginated<QueryWithStateTimingSchema>> {
        self.get(&format!("/api/v1/workspace/{workspace_id}/query"))
            .pagination(&pagination)
            .parameter("order_direction", "asc")
            .parameter_opt("cluster_id", filters.cluster_id)
            .parameter_opt("user_id", filters.user_id)
            .await?
            .json()
            .await
    }

    pub async fn get_query(
        &self,
        workspace_id: Uuid,
        query_id: Uuid,
    ) -> Result<QueryWithStateTimingAndResultSchema> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/query/{query_id}"
        ))
        .await?
        .json()
        .await
    }

    pub async fn get_query_plans(
        &self,
        workspace_id: Uuid,
        query_id: Uuid,
    ) -> Result<QueryPlansSchema> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/query/{query_id}/plans"
        ))
        .await?
        .json()
        .await
    }

    pub async fn get_query_count(
        &self,
        workspace_id: Uuid,
        filters: &QueryCountParams,
        window: &MetricWindow,
    ) -> Result<Vec<QueryCountSchema>> {
        let TimeWindow { start, end } = window.window;
        // A custom limit as otherwise we get the default pagination which is 25
        let limit = (end - start).num_days() + 1;

        Ok(self
            .get(&format!("/api/v1/workspace/{workspace_id}/query/counts"))
            .parameter("start", start.to_rfc3339_opts(SecondsFormat::Millis, true))
            .parameter("end", end.to_rfc3339_opts(SecondsFormat::Millis, true))
            .parameter("interval", window.interval.num_seconds())
            .parameter("limit", limit)
            .parameter_opt("cluster_id", filters.cluster_id)
            .await?
            .json::<Paginated<QueryCountSchema>>()
            .await?
            .result)
    }

    pub async fn cancel_query(&self, workspace_id: Uuid, query_id: Uuid) -> Result<()> {
        self.post(&format!(
            "/api/v1/workspace/{workspace_id}/query/{query_id}/cancel"
        ))
        .await?
        .empty()
        .await
    }

    pub async fn add_query_label(
        &self,
        workspace_id: Uuid,
        query_id: Uuid,
        label_id: Uuid,
    ) -> Result<()> {
        self.post(&format!(
            "/api/v1/workspace/{workspace_id}/query/{query_id}/label"
        ))
        .json(&LabelIdSchema { label_id })
        .await?
        .empty()
        .await
    }

    pub async fn get_query_labels(
        &self,
        workspace_id: Uuid,
        query_id: Uuid,
    ) -> Result<Vec<LabelOutputSchema>> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/query/{query_id}/label"
        ))
        .await?
        .json()
        .await
    }

    pub async fn delete_query_label(
        &self,
        workspace_id: Uuid,
        query_id: Uuid,
        label_id: Uuid,
    ) -> Result<()> {
        self.delete(&format!(
            "/api/v1/workspace/{workspace_id}/query/{query_id}/label/{label_id}"
        ))
        .await?
        .empty()
        .await
    }

    pub async fn add_manifest_label(
        &self,
        workspace_id: Uuid,
        manifest_id: Uuid,
        label_id: Uuid,
    ) -> Result<()> {
        self.post(&format!(
            "/api/v1/workspace/{workspace_id}/manifest/{manifest_id}/label"
        ))
        .json(&LabelIdSchema { label_id })
        .await?
        .empty()
        .await
    }

    pub async fn get_manifest_labels(
        &self,
        workspace_id: Uuid,
        manifest_id: Uuid,
    ) -> Result<Vec<LabelOutputSchema>> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/manifest/{manifest_id}/label"
        ))
        .await?
        .json()
        .await
    }

    pub async fn delete_manifest_label(
        &self,
        workspace_id: Uuid,
        manifest_id: Uuid,
        label_id: Uuid,
    ) -> Result<()> {
        self.delete(&format!(
            "/api/v1/workspace/{workspace_id}/manifest/{manifest_id}/label/{label_id}"
        ))
        .await?
        .empty()
        .await
    }

    pub async fn put_user_avatar(&self, image: Bytes) -> Result<()> {
        let auth_header = self.auth_header.read().unwrap().clone();
        self.client
            .put(format!("{}/api/v1/user/avatar", self.address))
            .header(AUTHORIZATION, auth_header)
            .body(image)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    pub async fn delete_user_avatar(&self) -> Result<()> {
        let auth_header = self.auth_header.read().unwrap().clone();
        self.client
            .delete(format!("{}/api/v1/user/avatar", self.address))
            .header(AUTHORIZATION, auth_header)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    pub async fn patch_user(&self, params: &UserBodyArgs) -> Result<()> {
        self.patch("/api/v1/user").json(params).await?.empty().await
    }

    pub async fn get_notifications(&self) -> Result<Paginated<NotificationSchema>> {
        self.get("/api/v1/notifications").await?.json().await
    }

    pub async fn patch_notification(
        &self,
        notification_id: Uuid,
        params: NotificationDetail,
    ) -> Result<()> {
        self.patch(&format!("/api/v1/user/notifications/{notification_id}"))
            .json(&params)
            .await?
            .empty()
            .await
    }

    pub async fn delete_notification(&self, notification_id: Uuid) -> Result<()> {
        self.delete(&format!("/api/v1/user/notifications/{notification_id}"))
            .await?
            .empty()
            .await
    }

    pub async fn get_logged_in_user(&self) -> Result<UserSchema> {
        self.get("/api/v1/user/me").await?.json().await
    }

    pub async fn get_workspaces(
        &self,
        filters: WorkspaceQuery,
        pagination: Pagination,
    ) -> Result<Paginated<WorkspaceSchema>> {
        self.get("/api/v1/workspace")
            .pagination(&pagination)
            .parameter_opt("name", filters.name.clone())
            .parameter_opt("organization_id", filters.organization_id)
            .await?
            .json()
            .await
    }

    pub async fn get_workspace(&self, workspace_id: Uuid) -> Result<WorkspaceSchema> {
        self.get(&format!("/api/v1/workspace/{workspace_id}"))
            .await?
            .json()
            .await
    }

    pub async fn patch_workspace_details(
        &self,
        workspace_id: Uuid,
        params: &WorkspaceDetails,
    ) -> Result<()> {
        self.patch(&format!("/api/v1/workspace/{workspace_id}"))
            .json(params)
            .await?
            .empty()
            .await
    }

    pub async fn get_workspace_compute_time(
        &self,
        workspace_id: Uuid,
        params: MetricWindow,
    ) -> Result<Vec<ComputeTimeSchema>> {
        let TimeWindow { start, end } = params.window;
        let interval = params.interval.num_seconds();
        // A custom limit as otherwise we get the default pagination which is 25
        let limit = (end - start).num_days() + 1;

        Ok(self
            .get(&format!("/api/v1/workspace/{workspace_id}/compute-time"))
            .parameter("start", start.to_rfc3339_opts(SecondsFormat::Millis, true))
            .parameter("end", end.to_rfc3339_opts(SecondsFormat::Millis, true))
            .parameter("interval", interval)
            .parameter("limit", limit)
            .await?
            .json::<Paginated<ComputeTimeSchema>>()
            .await?
            .result)
    }

    pub async fn get_cluster_defaults(
        &self,
        workspace_id: Uuid,
    ) -> Result<Option<WorkspaceClusterDefaultsSchema>> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/cluster-defaults"
        ))
        .await?
        .json()
        .await
    }

    pub async fn set_cluster_defaults(
        &self,
        workspace_id: Uuid,
        params: &WorkspaceClusterDefaultsSchema,
    ) -> Result<()> {
        self.put(&format!(
            "/api/v1/workspace/{workspace_id}/cluster-defaults"
        ))
        .json(params)
        .await?
        .empty()
        .await
    }

    pub async fn delete_cluster_defaults(&self, workspace_id: Uuid) -> Result<()> {
        self.delete(&format!(
            "/api/v1/workspace/{workspace_id}/cluster-defaults"
        ))
        .await?
        .empty()
        .await
    }

    pub async fn get_compute_cluster_nodes(
        &self,
        workspace_id: Uuid,
        compute_id: Uuid,
        pagination: Pagination,
    ) -> Result<Paginated<ComputeClusterNodeInfoSchema>> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/compute/{compute_id}/node"
        ))
        .pagination(&pagination)
        .await?
        .json()
        .await
    }

    pub async fn add_workspace_member(
        &self,
        workspace_id: Uuid,
        user_id: Uuid,
        params: &WorkspaceMemberRole,
    ) -> Result<()> {
        self.put(&format!(
            "/api/v1/workspace/{workspace_id}/member/{user_id}"
        ))
        .json(params)
        .await?
        .empty()
        .await
    }

    pub async fn get_workspace_members(
        &self,
        workspace_id: Uuid,
        implicit_users: Option<bool>,
        service_accounts: Option<bool>,
    ) -> Result<Vec<WorkspaceUserSchema>> {
        self.get(&format!("/api/v1/workspace/{workspace_id}/member"))
            .parameter_opt("implicit_users", implicit_users)
            .parameter_opt("service_accounts", service_accounts)
            .await?
            .json()
            .await
    }

    pub async fn get_workspace_member(
        &self,
        workspace_id: Uuid,
        user_id: Uuid,
    ) -> Result<WorkspaceUserSchema> {
        self.get(&format!(
            "/api/v1/workspace/{workspace_id}/member/{user_id}"
        ))
        .await?
        .json()
        .await
    }

    pub async fn patch_workspace_member_role(
        &self,
        workspace_id: Uuid,
        user_id: Uuid,
        role: WorkspaceRoleSchema,
    ) -> Result<()> {
        self.patch(&format!(
            "/api/v1/workspace/{workspace_id}/member/{user_id}/role"
        ))
        .json(&WorkspaceMemberRole { role })
        .await?
        .empty()
        .await
    }

    pub async fn remove_workspace_member(&self, workspace_id: Uuid, user_id: Uuid) -> Result<()> {
        self.delete(&format!(
            "/api/v1/workspace/{workspace_id}/member/{user_id}"
        ))
        .await?
        .empty()
        .await
    }

    pub async fn get_workspace_tokens(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<WorkspaceApiTokenWithNameSchema>> {
        self.get(&format!("/api/v1/workspace/{workspace_id}/token"))
            .await?
            .json()
            .await
    }

    pub async fn create_workspace_token(
        &self,
        workspace_id: Uuid,
        params: WorkSpaceTokenBody,
    ) -> Result<WorkspaceAPIToken> {
        self.post(&format!("/api/v1/workspace/{workspace_id}/token"))
            .json(params)
            .await?
            .json()
            .await
    }

    pub async fn delete_workspace_token(&self, workspace_id: Uuid, token_id: Uuid) -> Result<()> {
        self.delete(&format!(
            "/api/v1/workspace/{workspace_id}/token/{token_id}"
        ))
        .await?
        .empty()
        .await
    }
}
