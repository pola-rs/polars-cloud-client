use std::collections::BTreeMap;

use bytes::Bytes;
use prost::Message;
use prost_types::FieldMask;
use protos_common::{QueryIdentifier, QueryPlans, QueryResult, map_trait};
use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status};

use crate::client::proto::QueryStageStatistics;

mod proto {
    pub use self::client_service_server::ClientService;
    pub use self::query_settings::*;
    pub use crate::proto::polars_cloud::compute_plane::client::v1::*;
}

pub use proto::client_service_client::ClientServiceClient;
pub use proto::client_service_server::ClientServiceServer;
pub use proto::{ClientService as ClientServiceProto, StageStatistics};
pub type QueryStatus = proto::QueryStatus;

#[trait_variant::make(Send)]
pub trait ClientService {
    type Error: Into<Status>;

    async fn submit_query(
        &self,
        request: Request<server::SubmitQueryRequest>,
    ) -> Result<Response<QueryIdentifier>, Self::Error>;

    async fn cancel_query(
        &self,
        request: Request<QueryIdentifier>,
    ) -> Result<Response<()>, Self::Error>;

    async fn get_query_status(
        &self,
        request: Request<QueryIdentifier>,
    ) -> Result<Response<QueryStatus>, Self::Error>;

    async fn get_query_result(
        &self,
        request: Request<QueryIdentifier>,
    ) -> Result<Response<GetQueryResultResponse>, Self::Error>;

    async fn get_query_plans(
        &self,
        request: Request<GetQueryPlansRequest>,
    ) -> Result<Response<QueryPlans>, Self::Error>;
}

map_trait! {
    impl ClientService for proto::ClientService {
        submit_query(proto::SubmitQueryRequest) -> proto::SubmitQueryResponse;
        get_query_result(proto::GetQueryResultRequest) -> proto::GetQueryResultResponse;
        get_query_status(proto::GetQueryStatusRequest) -> proto::GetQueryStatusResponse;
        get_query_plans(proto::GetQueryPlansRequest) -> proto::GetQueryPlansResponse;
        cancel_query(proto::CancelQueryRequest) -> proto::CancelQueryResponse;
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

#[allow(clippy::module_inception)]
pub mod client {
    use protos_common::QueryInfo;

    use super::*;

    pub struct SubmitQueryRequest {
        pub query_settings: QuerySettings,
        pub plan: Bytes,
        pub query_info: QueryInfo,
    }

    impl From<SubmitQueryRequest> for proto::SubmitQueryRequest {
        fn from(value: SubmitQueryRequest) -> Self {
            Self {
                settings: Some(value.query_settings.into()),
                plan: value.plan,
                query_info: value.query_info.encode(),
            }
        }
    }
}

pub mod server {
    use bytes::Bytes;

    use super::*;

    impl From<proto::SubmitQueryRequest> for SubmitQueryRequest {
        fn from(value: proto::SubmitQueryRequest) -> Self {
            Self {
                query_settings: value.settings.unwrap().into(),
                plan: value.plan,
                query_info: value.query_info,
            }
        }
    }

    pub struct SubmitQueryRequest {
        pub query_settings: QuerySettings,
        pub plan: Bytes,
        pub query_info: Bytes,
    }
}

impl From<QuerySettings> for proto::QuerySettings {
    fn from(value: QuerySettings) -> Self {
        Self {
            engine: proto::Engine::from(value.engine).into(),
            preferred_graph_format: proto::GraphFormat::from(value.preferred_graph_format).into(),
            n_retries: value.n_retries,
            query_type: Some(value.query_type.into()),
        }
    }
}

impl From<proto::QuerySettings> for QuerySettings {
    fn from(value: proto::QuerySettings) -> Self {
        Self {
            engine: value.engine().into(),
            preferred_graph_format: value.preferred_graph_format().into(),
            n_retries: value.n_retries,
            query_type: value.query_type.unwrap().into(),
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShuffleCompression {
    #[default]
    Auto,
    LZ4,
    ZSTD,
    Uncompressed,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShuffleFormat {
    #[default]
    Auto,
    Ipc,
    Parquet,
}

impl From<ShuffleFormat> for proto::ShuffleFormat {
    fn from(value: ShuffleFormat) -> Self {
        match value {
            ShuffleFormat::Auto => proto::ShuffleFormat::Auto,
            ShuffleFormat::Ipc => proto::ShuffleFormat::Ipc,
            ShuffleFormat::Parquet => proto::ShuffleFormat::Parquet,
        }
    }
}

impl From<proto::ShuffleFormat> for ShuffleFormat {
    fn from(value: proto::ShuffleFormat) -> Self {
        match value {
            proto::ShuffleFormat::Auto | proto::ShuffleFormat::Unspecified => ShuffleFormat::Auto,
            proto::ShuffleFormat::Ipc => ShuffleFormat::Ipc,
            proto::ShuffleFormat::Parquet => ShuffleFormat::Parquet,
        }
    }
}

impl From<ShuffleCompression> for proto::ShuffleCompression {
    fn from(value: ShuffleCompression) -> Self {
        match value {
            ShuffleCompression::Auto => proto::ShuffleCompression::Auto,
            ShuffleCompression::LZ4 => proto::ShuffleCompression::Lz4,
            ShuffleCompression::ZSTD => proto::ShuffleCompression::Zstd,
            ShuffleCompression::Uncompressed => proto::ShuffleCompression::Uncompressed,
        }
    }
}

impl From<proto::ShuffleCompression> for ShuffleCompression {
    fn from(value: proto::ShuffleCompression) -> Self {
        match value {
            proto::ShuffleCompression::Auto | proto::ShuffleCompression::Unspecified => {
                ShuffleCompression::Auto
            },
            proto::ShuffleCompression::Lz4 => ShuffleCompression::LZ4,
            proto::ShuffleCompression::Zstd => ShuffleCompression::ZSTD,
            proto::ShuffleCompression::Uncompressed => ShuffleCompression::Uncompressed,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct ShuffleOpts {
    pub format: ShuffleFormat,
    pub compression: ShuffleCompression,
    pub compression_level: Option<i32>,
}

impl From<ShuffleOpts> for proto::ShuffleOpts {
    fn from(value: ShuffleOpts) -> Self {
        Self {
            format: proto::ShuffleFormat::from(value.format).into(),
            compression: proto::ShuffleCompression::from(value.compression).into(),
            compression_level: value.compression_level,
        }
    }
}

impl From<proto::ShuffleOpts> for ShuffleOpts {
    fn from(value: proto::ShuffleOpts) -> Self {
        Self {
            format: value.format().into(),
            compression: value.compression().into(),
            compression_level: value.compression_level,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub enum QueryType {
    #[default]
    Single,
    Distributed {
        shuffle_opts: ShuffleOpts,
        pre_aggregation: bool,
        sort_partitioned: bool,
        cost_based_planner: bool,
        equi_join_broadcast_limit: u64,
        partitions_per_worker: Option<u32>,
    },
}

impl From<QueryType> for proto::QueryType {
    fn from(value: QueryType) -> Self {
        match value {
            QueryType::Single => proto::QueryType::Single(proto::SingleOpts {}),
            QueryType::Distributed {
                shuffle_opts,
                pre_aggregation,
                sort_partitioned,
                cost_based_planner,
                equi_join_broadcast_limit,
                partitions_per_worker,
            } => proto::QueryType::Distributed(proto::DistributedOpts {
                shuffle_opts: proto::ShuffleOpts::from(shuffle_opts).into(),
                allow_pre_aggregation: pre_aggregation,
                allow_partitioned_sort: sort_partitioned,
                allow_equi_join_broadcast_limit: equi_join_broadcast_limit,
                cost_based_planner,
                partitions_per_worker,
            }),
        }
    }
}

impl From<proto::QueryType> for QueryType {
    fn from(value: proto::QueryType) -> Self {
        match value {
            proto::QueryType::Single(proto::SingleOpts {}) => Self::Single,
            proto::QueryType::Distributed(opts) => {
                let proto::DistributedOpts {
                    allow_pre_aggregation,
                    allow_partitioned_sort,
                    allow_equi_join_broadcast_limit,
                    cost_based_planner,
                    shuffle_opts,
                    partitions_per_worker,
                } = opts;
                Self::Distributed {
                    shuffle_opts: shuffle_opts.unwrap_or_default().into(),
                    pre_aggregation: allow_pre_aggregation,
                    sort_partitioned: allow_partitioned_sort,
                    cost_based_planner,
                    equi_join_broadcast_limit: allow_equi_join_broadcast_limit,
                    partitions_per_worker,
                }
            },
        }
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub enum Engine {
    #[default]
    Auto,
    Streaming,
    InMemory,
    Gpu,
}

impl From<Engine> for proto::Engine {
    fn from(value: Engine) -> Self {
        match value {
            Engine::Auto => Self::Auto,
            Engine::Streaming => Self::Streaming,
            Engine::InMemory => Self::InMemory,
            Engine::Gpu => Self::Gpu,
        }
    }
}

impl From<proto::Engine> for Engine {
    fn from(value: proto::Engine) -> Self {
        match value {
            proto::Engine::Unspecified | proto::Engine::Auto => Self::Auto,
            proto::Engine::Streaming => Self::Streaming,
            proto::Engine::InMemory => Self::InMemory,
            proto::Engine::Gpu => Self::Gpu,
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphFormat {
    #[default]
    Auto,
    Dot,
    Explain,
}

impl From<GraphFormat> for proto::GraphFormat {
    fn from(value: GraphFormat) -> Self {
        match value {
            GraphFormat::Auto => Self::Auto,
            GraphFormat::Dot => Self::Dot,
            GraphFormat::Explain => Self::Explain,
        }
    }
}

impl From<proto::GraphFormat> for GraphFormat {
    fn from(value: proto::GraphFormat) -> Self {
        match value {
            proto::GraphFormat::Unspecified | proto::GraphFormat::Auto => Self::Auto,
            proto::GraphFormat::Dot => Self::Dot,
            proto::GraphFormat::Explain => Self::Explain,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct QuerySettings {
    pub engine: Engine,
    pub preferred_graph_format: GraphFormat,
    pub n_retries: u32,
    pub query_type: QueryType,
}

impl QuerySettings {
    pub fn encode(self) -> Bytes {
        proto::QuerySettings::from(self).encode_to_vec().into()
    }

    pub fn decode(bytes: Bytes) -> Self {
        proto::QuerySettings::decode(bytes).unwrap().into()
    }
}

impl From<QueryIdentifier> for proto::CancelQueryRequest {
    fn from(value: QueryIdentifier) -> Self {
        Self {
            query_id: Some(value.into()),
        }
    }
}

impl From<proto::CancelQueryRequest> for QueryIdentifier {
    fn from(proto::CancelQueryRequest { query_id }: proto::CancelQueryRequest) -> Self {
        query_id.unwrap().into()
    }
}

impl From<()> for proto::CancelQueryResponse {
    fn from(_: ()) -> Self {
        proto::CancelQueryResponse {}
    }
}

impl From<proto::CancelQueryResponse> for () {
    fn from(proto::CancelQueryResponse {}: proto::CancelQueryResponse) -> Self {}
}

impl From<proto::GetQueryStatusRequest> for QueryIdentifier {
    fn from(proto::GetQueryStatusRequest { query_id }: proto::GetQueryStatusRequest) -> Self {
        query_id.unwrap().into()
    }
}

impl From<QueryIdentifier> for proto::GetQueryStatusRequest {
    fn from(value: QueryIdentifier) -> Self {
        Self {
            query_id: Some(value.into()),
        }
    }
}

impl From<proto::GetQueryStatusResponse> for QueryStatus {
    fn from(value: proto::GetQueryStatusResponse) -> Self {
        let status = value.status();
        // Make sure compilation fails if fields are added
        let proto::GetQueryStatusResponse { status: _ } = value;
        status
    }
}

impl From<QueryStatus> for proto::GetQueryStatusResponse {
    fn from(value: QueryStatus) -> Self {
        Self {
            status: value.into(),
        }
    }
}

#[derive(Debug)]
pub struct GetQueryResultResponse {
    pub result: QueryResult,
    pub compute_info: ComputeQueryInfo,
}

#[derive(Debug)]
pub struct ComputeQueryInfo {
    pub head: Option<Result<Bytes, String>>,
    pub stage_statistics: Option<BTreeMap<u32, StageStatistics>>,
}

impl From<proto::GetQueryResultResponse> for GetQueryResultResponse {
    fn from(
        proto::GetQueryResultResponse {
            query_result,
            compute_info,
        }: proto::GetQueryResultResponse,
    ) -> Self {
        Self {
            result: query_result.unwrap().into(),
            compute_info: compute_info.unwrap().into(),
        }
    }
}

impl From<GetQueryResultResponse> for proto::GetQueryResultResponse {
    fn from(
        GetQueryResultResponse {
            result,
            compute_info,
        }: GetQueryResultResponse,
    ) -> Self {
        Self {
            query_result: Some(result.into()),
            compute_info: Some(compute_info.into()),
        }
    }
}

impl From<proto::ComputeQueryInfo> for ComputeQueryInfo {
    fn from(
        proto::ComputeQueryInfo {
            head,
            stage_statistics,
        }: proto::ComputeQueryInfo,
    ) -> Self {
        Self {
            head: head.map(|head| match head {
                proto::compute_query_info::Head::Data(bytes) => Ok(bytes),
                proto::compute_query_info::Head::Error(e) => Err(e),
            }),
            stage_statistics: stage_statistics
                .map(|proto::QueryStageStatistics { stage_statistics }| stage_statistics),
        }
    }
}

impl From<ComputeQueryInfo> for proto::ComputeQueryInfo {
    fn from(
        ComputeQueryInfo {
            head,
            stage_statistics,
        }: ComputeQueryInfo,
    ) -> Self {
        Self {
            stage_statistics: Some(QueryStageStatistics {
                stage_statistics: stage_statistics.unwrap_or_default(),
            }),
            head: head.map(|head| match head {
                Ok(head) => proto::compute_query_info::Head::Data(head),
                Err(e) => proto::compute_query_info::Head::Error(e),
            }),
        }
    }
}

impl From<proto::GetQueryResultRequest> for QueryIdentifier {
    fn from(proto::GetQueryResultRequest { query_id }: proto::GetQueryResultRequest) -> Self {
        query_id.unwrap().into()
    }
}

impl From<QueryIdentifier> for proto::GetQueryResultRequest {
    fn from(value: QueryIdentifier) -> Self {
        Self {
            query_id: Some(value.into()),
        }
    }
}

#[derive(Debug)]
pub struct GetQueryPlansRequest {
    pub query_id: QueryIdentifier,
    pub plan_selection: Option<PlanSelection>,
}

#[derive(Clone, Copy, Debug)]
pub struct PlanSelection {
    pub ir: bool,
    pub phys: bool,
}

impl Default for PlanSelection {
    fn default() -> Self {
        Self {
            ir: true,
            phys: true,
        }
    }
}

impl TryFrom<FieldMask> for PlanSelection {
    type Error = Status;

    fn try_from(value: FieldMask) -> Result<Self, Self::Error> {
        let mut selection = PlanSelection::default();
        for field in value.paths {
            match field.as_str() {
                "plans.ir" => selection.ir = true,
                "plans.phys" => selection.phys = true,
                other => {
                    return Err(Status::invalid_argument(format!(
                        "Non-existent field {other} in `fields`"
                    )));
                },
            }
        }
        Ok(selection)
    }
}

impl From<PlanSelection> for FieldMask {
    fn from(value: PlanSelection) -> Self {
        let mut paths = Vec::with_capacity(4);
        if value.ir {
            paths.push("plans.ir".into());
        }
        if value.phys {
            paths.push("plans.phys".into());
        }
        FieldMask { paths }
    }
}

impl TryFrom<proto::GetQueryPlansRequest> for GetQueryPlansRequest {
    type Error = Status;
    fn try_from(
        proto::GetQueryPlansRequest { query_id, fields }: proto::GetQueryPlansRequest,
    ) -> Result<Self, Self::Error> {
        Ok(GetQueryPlansRequest {
            query_id: query_id.unwrap().into(),
            plan_selection: fields.map(TryInto::try_into).transpose()?,
        })
    }
}

impl From<GetQueryPlansRequest> for proto::GetQueryPlansRequest {
    fn from(value: GetQueryPlansRequest) -> Self {
        Self {
            query_id: Some(value.query_id.into()),
            fields: value.plan_selection.map(Into::into),
        }
    }
}

impl From<proto::GetQueryPlansResponse> for QueryPlans {
    fn from(proto::GetQueryPlansResponse { plans }: proto::GetQueryPlansResponse) -> Self {
        plans.unwrap()
    }
}

impl From<QueryPlans> for proto::GetQueryPlansResponse {
    fn from(value: QueryPlans) -> Self {
        Self { plans: Some(value) }
    }
}
