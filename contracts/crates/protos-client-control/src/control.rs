use bytes::Bytes;
use protos_common::{ComputeIdentifier, QueryIdentifier, QueryResult, map_trait};
use tonic::{Request, Response, Status};

mod proto {
    pub use self::client_service_client::ClientServiceClient;
    pub use self::client_service_server::{ClientService, ClientServiceServer};
    pub use crate::proto::polars_cloud::control_plane::client::v1::*;
}

pub use proto::SubmitQueryRequest as SubmitQueryRequestProto;
pub type ClientServiceServer<T> = proto::ClientServiceServer<T>;
pub type ClientServiceClient<T> = proto::ClientServiceClient<T>;

/// This is the trait downstream crates need to implement.
#[trait_variant::make(Send)]
pub trait ClientService {
    type Error: Into<Status>;
    async fn submit_query(
        &self,
        request: Request<server::SubmitQueryRequest>,
    ) -> Result<Response<QueryIdentifier>, Self::Error>;

    async fn get_query_result(
        &self,
        request: Request<QueryIdentifier>,
    ) -> Result<Response<QueryResult>, Self::Error>;
}

map_trait! {
    impl ClientService for proto::ClientService {
        submit_query(proto::SubmitQueryRequest) -> proto::SubmitQueryResponse;
        get_query_result(proto::GetQueryResultRequest) -> proto::GetQueryResultResponse;
    }
}

impl From<QueryResult> for proto::GetQueryResultResponse {
    fn from(value: QueryResult) -> Self {
        Self {
            result: Some(value.into()),
        }
    }
}

impl From<proto::GetQueryResultResponse> for QueryResult {
    fn from(proto::GetQueryResultResponse { result }: proto::GetQueryResultResponse) -> Self {
        result.unwrap().into()
    }
}

impl From<QueryIdentifier> for proto::SubmitQueryResponse {
    fn from(value: QueryIdentifier) -> Self {
        Self {
            query_id: Some(value.into()),
        }
    }
}

impl From<proto::SubmitQueryResponse> for QueryIdentifier {
    fn from(proto::SubmitQueryResponse { query_id }: proto::SubmitQueryResponse) -> Self {
        query_id.unwrap().into()
    }
}

impl From<proto::GetQueryResultRequest> for QueryIdentifier {
    fn from(value: proto::GetQueryResultRequest) -> Self {
        value.query_id.unwrap().query_id.parse().unwrap()
    }
}
impl From<QueryIdentifier> for proto::GetQueryResultRequest {
    fn from(value: QueryIdentifier) -> Self {
        Self {
            query_id: Some(value.into()),
        }
    }
}

pub mod server {
    use protos_common::QueryInfo;

    use super::*;
    #[derive(Clone)]
    pub struct SubmitQueryRequest {
        pub compute_id: ComputeIdentifier,
        pub settings: Bytes,
        pub plan: Bytes,
        pub query_info: QueryInfo,
    }

    impl From<proto::SubmitQueryRequest> for SubmitQueryRequest {
        fn from(value: proto::SubmitQueryRequest) -> Self {
            Self {
                compute_id: value.compute_id.unwrap().compute_id.parse().unwrap(),
                settings: value.settings,
                plan: value.plan,
                query_info: value.query_info.map(Into::into).unwrap_or_default(),
            }
        }
    }
}

#[allow(clippy::module_inception)]
pub mod client {
    use protos_client_compute::client::QuerySettings;
    use protos_common::QueryInfo;

    use super::*;

    #[derive(Clone)]
    pub struct SubmitQueryRequest {
        pub compute_id: ComputeIdentifier,
        pub settings: QuerySettings,
        pub plan: Bytes,
        pub query_info: QueryInfo,
    }

    impl From<SubmitQueryRequest> for proto::SubmitQueryRequest {
        fn from(value: SubmitQueryRequest) -> Self {
            Self {
                compute_id: Some(value.compute_id.into()),
                settings: value.settings.encode(),
                plan: value.plan,
                query_info: Some(value.query_info.into()),
            }
        }
    }
}

impl From<ComputeIdentifier> for proto::ComputeId {
    fn from(value: ComputeIdentifier) -> Self {
        Self {
            compute_id: value.into_string(),
        }
    }
}
