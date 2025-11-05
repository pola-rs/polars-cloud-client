use bytes::Bytes;
use protos_common::QueryIdentifier;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, async_trait};

mod proto {
    pub use self::query_profile_service_client::QueryProfileServiceClient;
    pub use self::query_profile_service_server::QueryProfileService;
    pub use crate::proto::polars_cloud::compute_plane::observatory::v1::*;
}
pub use proto::QueryProfileServiceClient;
pub use proto::query_profile_service_server::QueryProfileServiceServer;

#[derive(Clone)]
pub struct QueryProfileClient {
    inner: proto::QueryProfileServiceClient<Channel>,
}

impl QueryProfileClient {
    pub async fn new(channel: Channel) -> Result<Self, tonic::transport::Error> {
        Ok(Self {
            inner: proto::QueryProfileServiceClient::new(channel),
        })
    }

    pub async fn get_query_profile(
        &mut self,
        request: GetQueryProfileRequest,
    ) -> Result<Response<Option<QueryProfile>>, Status> {
        self.inner
            .get_query_profile(Request::new(request.into()))
            .await
            .map(|resp| resp.map(|outer| outer.profile.map(QueryProfile::from)))
    }
}

#[async_trait]
/// This is the trait downstream crates need to implement.
pub trait AdaptedQueryProfileService {
    async fn get_query_profile(
        &self,
        request: Request<GetQueryProfileRequest>,
    ) -> Result<Response<Option<QueryProfile>>, Status>;
}

#[async_trait]
impl<T: AdaptedQueryProfileService + Send + Sync + 'static> proto::QueryProfileService for T {
    async fn get_query_profile(
        &self,
        request: Request<proto::GetQueryProfileRequest>,
    ) -> Result<Response<proto::GetQueryProfileResponse>, Status> {
        self.get_query_profile(request.map(GetQueryProfileRequest::from))
            .await
            .map(|resp| {
                resp.map(|q| proto::GetQueryProfileResponse {
                    profile: q.map(Into::into),
                })
            })
    }
}

#[derive(Clone)]
pub struct GetQueryProfileRequest {
    pub query_id: QueryIdentifier,
    pub tag: Option<Bytes>,
}

impl From<proto::GetQueryProfileRequest> for GetQueryProfileRequest {
    fn from(value: proto::GetQueryProfileRequest) -> Self {
        GetQueryProfileRequest {
            query_id: value.identifier.unwrap().into(),
            tag: value.tag,
        }
    }
}

impl From<GetQueryProfileRequest> for proto::GetQueryProfileRequest {
    fn from(value: GetQueryProfileRequest) -> Self {
        proto::GetQueryProfileRequest {
            identifier: Some(value.query_id.into()),
            tag: value.tag,
        }
    }
}

impl From<proto::GetQueryProfileResponse> for Option<QueryProfile> {
    fn from(proto::GetQueryProfileResponse { profile }: proto::GetQueryProfileResponse) -> Self {
        profile.map(Into::into)
    }
}

pub struct QueryProfile {
    pub tag: Bytes,
    pub total_stages: Option<u32>,
    pub phys_plan_explain: Option<String>,
    pub phys_plan_dot: Option<String>,
    pub data: Bytes,
}

impl From<proto::QueryProfile> for QueryProfile {
    fn from(value: proto::QueryProfile) -> Self {
        Self {
            tag: value.tag,
            total_stages: value.total_stages,
            phys_plan_explain: value.phys_plan_explain,
            phys_plan_dot: value.phys_plan_dot,
            data: value.data,
        }
    }
}

impl From<QueryProfile> for proto::QueryProfile {
    fn from(value: QueryProfile) -> Self {
        proto::QueryProfile {
            tag: value.tag,
            total_stages: value.total_stages,
            phys_plan_explain: value.phys_plan_explain,
            phys_plan_dot: value.phys_plan_dot,
            data: value.data,
        }
    }
}
