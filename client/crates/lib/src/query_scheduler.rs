#![allow(clippy::result_large_err)]

use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use client_core::constants::SERVICE_NAME;
use client_core::error::{ApiResult, ObservatoryTransportError, SchedulerTransportError};
use client_core::{ApiError, ClientConfig, ControlPlaneClient, RUNTIME};
use pc_observatory_models::QueryDetailModel;
use pc_observatory_models::stages::StageGraphVisualizationData;
use polars_axum_models::QueryStatusCodeModel;
use polars_backend_client::client::user_agent;
use protos_client_compute::client::client::SubmitQueryRequest;
use protos_client_compute::client::{
    ClientQueryPlans, ClientServiceClient, GetComputeVersionsRequest, GetQueryPlansRequest,
    GetQueryResultResponse, PlanSelection, QueryStatus,
};
use protos_client_compute::proto::polars_cloud::compute_plane::client::v1::{
    self as proto, GetQueryStatusRequest,
};
use protos_client_compute::tonic::body::Body;
use protos_client_compute::tonic::codegen::http;
use protos_common::tonic::metadata::{MetadataKey, MetadataValue};
use protos_common::tonic::transport::{Certificate, Channel, ClientTlsConfig, Uri};
use protos_common::tonic::{self, Code, Request};
use protos_common::{
    ComputeVersions, MAX_MESSAGE_LENGTH_UNLIMITED, PlanFormat, QueryIdentifier, QueryInfo,
};
use pyo3::exceptions::{PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use reqwest_otel::TracingMiddleware;
use tokio_rustls::rustls;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::{DigitallySignedStruct, SignatureScheme};
use tonic::Status;
use tower::ServiceBuilder;
use tower::retry::backoff::ExponentialBackoffMaker;
use tower::util::BoxCloneSyncService;
use tower::util::rng::HasherRng;
use tower_grpc_retry::{HttpRetry, Params, RetryUnavailable};
use tower_http::auth::{AsyncAuthorizeRequest, AsyncRequireAuthorizationLayer};
use tower_http::set_header::HeaderMetadata;
use tower_http::set_header::request::SetMultipleRequestHeadersLayer;
use tracing::Instrument;
use utils::{Backoff, Exponential, retry};
use uuid::Uuid;

use crate::entry::EnterRustExt;
use crate::flight::{Flight, FlightResult};
use crate::query_settings::{PyLineageContext, PyQuerySettings};
use crate::serde_types::{
    DomainName, ParsedHeaders, ParsedUri, QueryDetailPy, QueryInfoPy, query_result_to_py,
    query_status_to_code,
};
use crate::{CTRL_PLN_CLIENT_GLOBAL, VERSIONS};

type SchedulerChannel =
    BoxCloneSyncService<http::Request<Body>, http::Response<Body>, tonic::transport::Error>;

type SchedulerGRPCClient = ClientServiceClient<
    BoxCloneSyncService<http::Request<Body>, http::Response<Body>, SchedulerTransportError>,
>;

const CLIENT_VERSION_HEADER_NAME: HeaderName = HeaderName::from_static("x-client-version");
const POLARS_VERSION_HEADER_NAME: HeaderName = HeaderName::from_static("x-polars-version");
const GRPC_RETRY_CONNECTION_DEADLINE: Duration = Duration::from_secs(60);

const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(20);

/// Retries a request that failed with `Unavailable`, which is how an unreachable scheduler
/// shows up. The backoff adds up to about a minute, like [`GRPC_RETRY_CONNECTION_DEADLINE`].
fn unavailable_retry_params() -> Params {
    Params {
        max_retries: 6,
        // Buffer that holds the request body for a retry; a larger body is not retried.
        max_request_bytes: 4 * 1024 * 1024,
        backoff: Some(Arc::new(
            ExponentialBackoffMaker::new(
                Duration::from_secs(1),
                Duration::from_secs(30),
                0.05,
                HasherRng::new(),
            )
            .unwrap(),
        )),
    }
}

#[derive(Clone)]
struct SchedulerGrpcClient {
    inner: SchedulerGRPCClient,
}

fn connect_scheduler(
    options: &ClientOptions,
    token_provider: Option<TokenProvider>,
) -> ApiResult<SchedulerChannel> {
    let channel = RUNTIME.block_on(create_channel(options))??;

    let version_layers = version_header_layers();

    let stack =
        ServiceBuilder::new()
            .layer(tower_otel::OtelLayer::client(SERVICE_NAME))
            .layer(SetMultipleRequestHeadersLayer::overriding(version_layers))
            .layer(SetMultipleRequestHeadersLayer::if_not_present(
                options
                    .extra_headers
                    .clone()
                    .unwrap_or_default()
                    .to_header_metadata(),
            ))
            .option_layer(token_provider.map(|token_provider| {
                AsyncRequireAuthorizationLayer::new(Authorize(token_provider))
            }))
            .service(channel);

    Ok(BoxCloneSyncService::new(stack))
}

#[derive(Clone)]
struct Authorize(TokenProvider);

impl<B: Send + 'static> AsyncAuthorizeRequest<B> for Authorize {
    type RequestBody = B;
    type ResponseBody = Body;
    type Future =
        Pin<Box<dyn Future<Output = Result<http::Request<B>, http::Response<Body>>> + Send>>;

    fn authorize(&mut self, mut req: http::Request<B>) -> Self::Future {
        let token_provider = self.0.clone();

        Box::pin(async move {
            if req.headers().contains_key(AUTHORIZATION) {
                return Ok(req);
            }
            let header = token_provider
                .bearer_header()
                .await
                .map_err(|error| Status::unauthenticated(error.to_string()).into_http())?;
            req.headers_mut().insert(AUTHORIZATION, header);
            Ok(req)
        })
    }
}

impl SchedulerGrpcClient {
    fn new(channel: SchedulerChannel) -> Self {
        let retrying = ServiceBuilder::new()
            .map_err(SchedulerTransportError)
            .map_request(|mut req: http::Request<Body>| {
                req.extensions_mut()
                    .insert(RetryUnavailable::new(unavailable_retry_params()));
                req
            })
            .service(HttpRetry::<RetryUnavailable, _>::new(channel));
        Self {
            inner: ClientServiceClient::new(BoxCloneSyncService::new(retrying))
                .max_encoding_message_size(MAX_MESSAGE_LENGTH_UNLIMITED)
                .max_decoding_message_size(MAX_MESSAGE_LENGTH_UNLIMITED),
        }
    }
}

type TokenFuture = Pin<Box<dyn Future<Output = ApiResult<String>> + Send>>;

#[derive(Clone)]
pub struct TokenProvider(Arc<dyn Fn() -> TokenFuture + Send + Sync>);

impl TokenProvider {
    fn control_plane() -> Self {
        Self(Arc::new(|| {
            Box::pin(CTRL_PLN_CLIENT_GLOBAL.get_access_token())
        }))
    }

    async fn bearer_header(&self) -> ApiResult<HeaderValue> {
        let token = self.0().await?;
        format!("Bearer {token}")
            .parse()
            .map_err(|_| ApiError::Other(anyhow!("Access token is not a valid header value")))
    }
}

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct SchedulerClient {
    scheduler_client: SchedulerGrpcClient,
    observatory_client_rest: ObservatoryRestClient,
    flight_client: Flight<SchedulerChannel>,
}

struct AuthorizeMiddleware(TokenProvider);

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for AuthorizeMiddleware {
    async fn handle(
        &self,
        mut req: reqwest::Request,
        extensions: &mut http::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        if !req.headers().contains_key(AUTHORIZATION) {
            let header = self
                .0
                .bearer_header()
                .await
                .map_err(|e| reqwest_middleware::Error::Middleware(anyhow!(e.to_string())))?;
            req.headers_mut().insert(AUTHORIZATION, header);
        }
        next.run(req, extensions).await
    }
}

/// Wraps failures to reach the observatory in [`ObservatoryTransportError`]. Responses, including
/// error statuses, pass through unchanged.
struct ObservatoryContextMiddleware;

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for ObservatoryContextMiddleware {
    async fn handle(
        &self,
        req: reqwest::Request,
        extensions: &mut http::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let url = req.url().clone();
        next.run(req, extensions)
            .await
            .map_err(|error| match error {
                reqwest_middleware::Error::Reqwest(source)
                    if source.is_connect() || source.is_timeout() || source.is_request() =>
                {
                    reqwest_middleware::Error::middleware(ObservatoryTransportError { url, source })
                },
                error => error,
            })
    }
}

#[pyclass(from_py_object, eq, eq_int)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanFormatPy {
    Dot,
    Explain,
    /// The stage graph serialized as JSON; only supported for the physical plan.
    Json,
}

#[pyclass(skip_from_py_object, get_all)]
pub struct QueryPlansPy {
    /// The format `ir_plan` is rendered in.
    pub format: PlanFormatPy,
    pub ir_plan: Option<String>,
    pub phys_plan: Option<String>,
}

#[pyclass(skip_from_py_object, get_all)]
pub struct ComputeVersionsPy {
    pub compute_plane_version: String,
    pub polars_python_version: String,
    pub polars_rust_revision: String,
}

#[derive(Clone, Debug)]
struct ObservatoryRestClient {
    inner: reqwest_middleware::ClientWithMiddleware,
    base_url: String,
}

impl ObservatoryRestClient {
    fn build(
        options: &ClientOptions,
        token_provider: Option<TokenProvider>,
    ) -> Result<Self, ApiError> {
        let mut builder = reqwest::ClientBuilder::new();

        if let Some(tls_options) = &options.tls_options {
            if let Some(ca_cert) = &tls_options.ca_cert {
                let cert = reqwest::Certificate::from_pem(ca_cert)?;
                builder = builder.add_root_certificate(cert);
            }

            if tls_options.insecure {
                builder = builder
                    .danger_accept_invalid_certs(true)
                    .danger_accept_invalid_hostnames(true);
            }
        }

        let uri: Uri = options.uri.clone().into();

        let mut url = reqwest::Url::parse(&uri.to_string())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        if let Some(domain_name) = &options.domain_name {
            let addr_str = uri
                .authority()
                .map(|a| a.as_str())
                .ok_or_else(|| PyValueError::new_err("URI has no authority"))?;
            let addr = addr_str.parse::<SocketAddr>().map_err(|_| {
                PyValueError::new_err(format!(
                    "URI host must be an IP address when `domain_name` is set, got: {addr_str}"
                ))
            })?;
            builder = builder.resolve(domain_name, addr);

            url.set_host(Some(domain_name)).unwrap();
        }

        if let Some(request_headers) = &options.extra_headers {
            builder = builder.default_headers(request_headers.clone().into());
        }

        let mut client = reqwest_middleware::ClientBuilder::new(builder.build()?)
            .with(TracingMiddleware::new().propagate());
        if let Some(token_provider) = token_provider {
            client = client.with(AuthorizeMiddleware(token_provider));
        }
        // Last, so it only sees transport failures and not e.g. a failure to get a token.
        let client = client.with(ObservatoryContextMiddleware);

        Ok(Self {
            inner: client.build(),
            base_url: url.to_string().trim_end_matches('/').to_string(),
        })
    }

    pub fn request(
        &self,
        method: reqwest::Method,
        path: &str,
    ) -> reqwest_middleware::RequestBuilder {
        let url = format!(
            "{}/api/v1/query/{}",
            self.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        );
        self.inner.request(method, url)
    }

    pub fn get(&self, path: &str) -> reqwest_middleware::RequestBuilder {
        self.request(reqwest::Method::GET, path)
    }

    /// GETs `path` and decodes the JSON body. Error statuses become [`ApiError::StatusError`].
    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> ApiResult<T> {
        let response = self.get(path).send().await?;
        let status = response.status();
        if !status.is_success() {
            let url = response.url().clone();
            let body = response.text().await.unwrap_or_default();
            return Err(ApiError::StatusError { status, url, body });
        }
        Ok(response.json().await?)
    }
}

impl SchedulerClient {
    fn build(
        scheduler: ClientOptions,
        observatory: ClientOptions,
        token_provider: Option<TokenProvider>,
    ) -> ApiResult<SchedulerClient> {
        let observatory_client_rest =
            ObservatoryRestClient::build(&observatory, token_provider.clone())?;
        let scheduler_channel = connect_scheduler(&scheduler, token_provider)?;
        let scheduler_client = SchedulerGrpcClient::new(scheduler_channel.clone());
        let flight_client = Flight::new(scheduler_channel);

        Ok(SchedulerClient {
            scheduler_client,
            observatory_client_rest,
            flight_client,
        })
    }
}

#[pymethods]
impl SchedulerClient {
    #[new]
    #[tracing::instrument(name = "SchedulerClient::new", skip_all)]
    pub fn new(scheduler: ClientOptions, observatory: ClientOptions) -> ApiResult<SchedulerClient> {
        Self::build(scheduler, observatory, Some(TokenProvider::control_plane()))
    }

    /// Never contacts the control plane for an access token, so the client sends only the headers
    /// `ClientOptions::extra_headers` carries. Use this against a cluster that authenticates
    /// requests some other way; requests still reach it with whatever credentials those headers
    /// hold.
    #[staticmethod]
    #[tracing::instrument(name = "SchedulerClient::without_default_auth", skip_all)]
    pub fn without_default_auth(
        scheduler: ClientOptions,
        observatory: ClientOptions,
    ) -> ApiResult<SchedulerClient> {
        Self::build(scheduler, observatory, None)
    }

    pub fn cancel_direct_query(&self, py: Python, query_id: Uuid) -> ApiResult<()> {
        let result = py.enter_rust(|| {
            RUNTIME.block_on(async move {
                let query_id = QueryIdentifier::from(query_id);
                let req = Request::new(query_id.into());
                self.scheduler_client.inner.clone().cancel_query(req).await
            })
        })?;
        match result {
            Ok(_) => Ok(()),
            // A finished query has nothing left to cancel.
            Err(status) if status.code() == Code::NotFound => Ok(()),
            Err(status) => Err(status.into()),
        }
    }

    pub fn delete_direct_query_result(&self, py: Python, query_id: Uuid) -> ApiResult<()> {
        let result = py.enter_rust(|| {
            RUNTIME.block_on(async move {
                let query_id = QueryIdentifier::from(query_id);
                let req = Request::new(query_id.into());
                self.scheduler_client
                    .inner
                    .clone()
                    .delete_query_result(req)
                    .await
            })
        })?;
        match result {
            Ok(_) => Ok(()),
            // A result that is already gone needs no deleting.
            Err(status) if status.code() == Code::NotFound => Ok(()),
            Err(status) => Err(status.into()),
        }
    }

    pub fn get_direct_query_status(
        &self,
        py: Python,
        query_id: Uuid,
    ) -> ApiResult<QueryStatusCodeModel> {
        let result = py.enter_rust(|| {
            let query_id = QueryIdentifier::from(query_id);

            RUNTIME.block_on(async move {
                let req = Request::new(query_id.into());
                let result = self
                    .scheduler_client
                    .inner
                    .clone()
                    .get_query_status(req)
                    .await?
                    .into_inner()
                    .message()
                    .await?
                    .ok_or_else(|| ApiError::Other(anyhow!("Unexpected End-of-Stream")))?;
                Ok::<_, ApiError>(QueryStatus::from(result))
            })
        })??;

        query_status_to_code(result).ok_or_else(|| {
            ApiError::PyErr(PyRuntimeError::new_err(
                "Server returned unknown query status code",
            ))
        })
    }

    #[tracing::instrument(name = "await_query_result", skip(self, py), fields(%query_id))]
    pub fn get_direct_query_result(
        &self,
        py: Python<'_>,
        query_id: Uuid,
        timeout_ms: u64,
    ) -> ApiResult<QueryInfoPy> {
        let timed_out = || {
            ApiError::PyErr(PyTimeoutError::new_err(
                "timed out waiting for the query result",
            ))
        };
        py.detach(|| {
            let query_id = QueryIdentifier::from(query_id);
            RUNTIME.block_on(async move {
                let timeout = Duration::from_millis(timeout_ms);
                let mut req = Request::new(query_id.into());
                req.set_timeout(timeout);
                let mut client = self.scheduler_client.inner.clone();

                match tokio::time::timeout(timeout, client.get_query_result(req)).await {
                    Ok(Ok(response)) => Ok(GetQueryResultResponse::from(response.into_inner())),
                    Ok(Err(status)) if status.code() == Code::DeadlineExceeded => Err(timed_out()),
                    Ok(Err(status)) => Err(ApiError::from(status)),
                    Err(_) => Err(timed_out()),
                }
            })
        })?
        .and_then(
            |GetQueryResultResponse {
                 result,
                 compute_info,
                 status,
             }| {
                if status == QueryStatus::Unspecified {
                    return Err(ApiError::PyErr(PyRuntimeError::new_err(
                        "Server returned unknown query status code",
                    )));
                }
                Ok(query_result_to_py(
                    py,
                    result,
                    Some(compute_info),
                    Some(status),
                ))
            },
        )
    }

    pub fn scan_flight(&self, py: Python<'_>, query_id: Uuid) -> ApiResult<Option<FlightResult>> {
        py.enter_rust(|| {
            let query_id = QueryIdentifier::from(query_id);
            RUNTIME.block_on(async {
                let req = Request::new(GetQueryStatusRequest{
                    query_id: Some(query_id.into()),
                });
                let mut status =self.scheduler_client
                    .inner.clone()
                    .get_query_status(req)
                    .await?.into_inner();
                while let Some(status) = status.message().await? {
                    match status.status() {
                        proto::QueryStatus::Scheduled | proto::QueryStatus::Planning => continue,
                        proto::QueryStatus::Success | // The result was empty or small enough to buffer on the scheduler. We can still retrieve it.
                        proto::QueryStatus::Running => break,
                        proto::QueryStatus::Unspecified => return Err(Status::internal("Unknown query status").into()),
                        // Let the caller check the query result
                        proto::QueryStatus::Failed |
                        proto::QueryStatus::Canceled => return Ok(None),
                    }
                }
                self.flight_client.scan_flight(query_id).await.map(Some)
            })
        })?
    }

    #[pyo3(signature = (plan, settings, username=None, labels=None, lineage_context=None))]
    pub fn do_query(
        &self,
        py: Python<'_>,
        plan: Vec<u8>,
        settings: PyQuerySettings,
        username: Option<String>,
        labels: Option<Vec<String>>,
        lineage_context: Option<PyLineageContext>,
    ) -> ApiResult<Uuid> {
        py.enter_rust(|| {
            let request = SubmitQueryRequest {
                query_info: QueryInfo {
                    labels: labels.unwrap_or_default(),
                    lineage_context: lineage_context.map(protos_common::LineageContext::from),
                },
                plan: plan.into(),
                query_settings: settings.into(),
            };

            RUNTIME.block_on(async move {
                let mut req = Request::new(request.into());
                if let Some(username) = username.or_else(ClientConfig::resolve_username) {
                    let shortened_username: String = username.chars().take(64).collect();
                    let metadata = MetadataValue::from_str(&shortened_username)
                        .map_err(|_e| PyValueError::new_err("Invalid username"))?;
                    let metadatakey = MetadataKey::from_str("x-polars-user").unwrap();
                    let _ = req.metadata_mut().insert(metadatakey, metadata);
                }
                let result = self
                    .scheduler_client
                    .inner
                    .clone()
                    .submit_query(req)
                    .await?;
                Ok(result.into_inner())
            })
        })?
        .map(|response| QueryIdentifier::from(response).inner)
    }

    pub fn get_query_details(&self, py: Python<'_>, query_id: Uuid) -> ApiResult<QueryDetailPy> {
        py.enter_rust(|| {
            RUNTIME.block_on(
                self.observatory_client_rest
                    .get_json::<QueryDetailModel>(&query_id.to_string()),
            )
        })?
        .map(QueryDetailPy::from)
    }

    #[pyo3(signature = (query_id, phys = None, ir = false))]
    #[tracing::instrument(name = "get_query_plans", skip(self, query_id, py), fields(%query_id))]
    pub fn get_direct_query_plan(
        &self,
        py: Python<'_>,
        query_id: Uuid,
        phys: Option<PlanFormatPy>,
        ir: bool,
    ) -> ApiResult<QueryPlansPy> {
        let query_plans: ClientQueryPlans = py
            .enter_rust(|| {
                let plans = PlanSelection {
                    ir,
                    phys: phys.is_some(),
                };
                RUNTIME.block_on(async move {
                    let mut client = self.scheduler_client.inner.clone();
                    // The scheduler answers with `FailedPrecondition` until planning is done.
                    retry!(
                        Exponential::new(Duration::from_millis(50))
                            .maximum(Duration::from_millis(250)),
                        async {
                            let req = Request::new(
                                GetQueryPlansRequest {
                                    query_id: query_id.into(),
                                    plan_selection: Some(plans),
                                }
                                .into(),
                            );
                            match client.get_query_plans(req).await {
                                Ok(r) => utils::OperationResult::Ok(r),
                                Err(s) if s.code() == Code::FailedPrecondition => {
                                    utils::OperationResult::Retry(s)
                                },
                                Err(s) => utils::OperationResult::Err(s),
                            }
                        },
                        tokio::time::sleep
                    )
                    .await
                })
            })??
            .into_inner()
            .into();

        let format = match query_plans.format() {
            PlanFormat::Unspecified => {
                return Err(ApiError::PyErr(PyRuntimeError::new_err(
                    "Cluster returned unrecognized query plan format",
                )));
            },
            PlanFormat::Dot => PlanFormatPy::Dot,
            PlanFormat::Explain => PlanFormatPy::Explain,
        };

        let phys_plan = match (phys, query_plans.stage_graph) {
            (Some(phys), Some(stage_graph)) => {
                let stage_graph: StageGraphVisualizationData = rmp_serde::from_slice(&stage_graph)
                    .map_err(|e| {
                        PyValueError::new_err(format!("failed to read the stage graph: {e}"))
                    })?;
                Some(match phys {
                    PlanFormatPy::Dot => stage_graph.display_dot().to_string(),
                    PlanFormatPy::Explain => stage_graph.explain().to_string(),
                    PlanFormatPy::Json => serde_json::to_string(&stage_graph).map_err(|e| {
                        PyValueError::new_err(format!("failed to serialize the stage graph: {e}"))
                    })?,
                })
            },
            _ => None,
        };

        Ok(QueryPlansPy {
            format,
            ir_plan: query_plans.ir_plan,
            phys_plan,
        })
    }

    pub fn get_compute_versions(&self, py: Python<'_>) -> ApiResult<ComputeVersionsPy> {
        let versions: ComputeVersions = py
            .enter_rust(|| {
                RUNTIME.block_on(async move {
                    let req = Request::new(GetComputeVersionsRequest {}.into());
                    self.scheduler_client
                        .inner
                        .clone()
                        .get_compute_versions(req)
                        .await
                })
            })??
            .into_inner()
            .into();

        Ok(ComputeVersionsPy {
            compute_plane_version: versions.compute_plane_version,
            polars_python_version: versions.polars_python_version,
            polars_rust_revision: versions.polars_rust_revision,
        })
    }
}

#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct ClientOptions {
    #[pyo3(get, set)]
    pub uri: ParsedUri,
    #[pyo3(get, set)]
    pub domain_name: Option<DomainName>,
    #[pyo3(get, set)]
    pub extra_headers: Option<ParsedHeaders>,
    #[pyo3(get, set)]
    pub tls_options: Option<TLSOptions>,
}

#[pyclass(from_py_object)]
#[derive(Debug, Clone, Default)]
pub struct TLSOptions {
    #[pyo3(get, set)]
    pub ca_cert: Option<Vec<u8>>,
    #[pyo3(get, set)]
    pub insecure: bool,
}

#[pymethods]
impl TLSOptions {
    #[new]
    #[pyo3(signature = (*, ca_cert=None, insecure=false))]
    fn new(ca_cert: Option<Vec<u8>>, insecure: bool) -> Self {
        Self { ca_cert, insecure }
    }
}

#[pymethods]
impl ClientOptions {
    #[new]
    #[pyo3(signature = (uri, *, domain_name=None, extra_headers=None, tls_options=None))]
    fn new(
        uri: ParsedUri,
        domain_name: Option<DomainName>,
        extra_headers: Option<ParsedHeaders>,
        tls_options: Option<TLSOptions>,
    ) -> Self {
        Self {
            uri,
            domain_name,
            extra_headers,
            tls_options,
        }
    }
}

#[derive(Debug)]
struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer,
        _intermediates: &[CertificateDer],
        _server_name: &ServerName,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

#[allow(clippy::result_large_err)]
async fn create_channel(options: &ClientOptions) -> ApiResult<Channel> {
    let uri: Uri = options.uri.clone().into();
    let mut endpoint = Channel::builder(uri.clone()).http2_keep_alive_interval(KEEP_ALIVE_INTERVAL);
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| PyValueError::new_err("missing uri scheme"))?;

    if let Some(domain_name) = &options.domain_name {
        let port = uri
            .port_u16()
            .ok_or_else(|| PyValueError::new_err("missing uri port"))?;
        let new_origin = format!("{scheme}://{domain_name}:{port}")
            .parse::<Uri>()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        endpoint = endpoint.origin(new_origin);
    }

    if options.tls_options.is_some() || scheme == "https" {
        let tls_options = options.tls_options.clone().unwrap_or_default();
        let mut tls = ClientTlsConfig::new();

        if let Some(domain_name) = &options.domain_name {
            tls = tls.domain_name(domain_name.clone());
        }

        if tls_options.insecure {
            endpoint = endpoint.tls_config_with_verifier(tls, Arc::new(NoVerifier))?
        } else {
            if let Some(ca_cert) = tls_options.ca_cert {
                tls = tls.ca_certificate(Certificate::from_pem(ca_cert));
            }
            endpoint = endpoint.tls_config(tls.with_enabled_roots())?
        }
    };

    utils::retry! {
        Exponential::new(Duration::from_secs(1)).maximum(Duration::from_secs(5)).deadline(GRPC_RETRY_CONNECTION_DEADLINE),
        async {
            let res = endpoint
                .clone()
                .user_agent(user_agent(VERSIONS.get().unwrap().as_ref().map(|(_, versions)| versions)))
                .unwrap()
                .connect()
                .instrument(tracing::info_span!(
                    "grpc connect",
                    "server.address" = uri.host(),
                    "server.port" = uri.port_u16(),
                ))
                .await
                .map_err(SchedulerTransportError)?;

            Ok::<_, ApiError>(res)
        },
        tokio::time::sleep
    }
        .await
}

fn version_header_layers() -> Vec<HeaderMetadata<http::Request<tonic::body::Body>>> {
    let (_, versions) = VERSIONS.get().unwrap().clone().unwrap();

    vec![
        (CLIENT_VERSION_HEADER_NAME, Some(versions.client)).into(),
        (POLARS_VERSION_HEADER_NAME, Some(versions.polars)).into(),
    ]
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use protos_common::tonic::transport::Uri;
    use reqwest::Method;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn unavailable_scheduler_reports_connection_guidance() {
        let channel = BoxCloneSyncService::new(
            Channel::from_shared(closed_port_uri())
                .unwrap()
                .connect_lazy(),
        );
        let mut client = SchedulerGrpcClient::new(channel).inner;

        // The middleware must add context before the Python API sees the error,
        // without changing the status code used by retry logic.
        let status = client
            .get_compute_versions(Request::new(GetComputeVersionsRequest {}.into()))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::Unavailable);
        assert!(status.source().unwrap().to_string().contains("scheduler"));
        Python::attach(|py| assert_connection_guidance(py, ApiError::from(status).into()));

        // Awaiting a result gives up once the retries run out, well before its own timeout.
        let timeout = Duration::from_secs(3600);
        let started = tokio::time::Instant::now();
        let mut req = Request::new(QueryIdentifier::from(Uuid::nil()).into());
        req.set_timeout(timeout);
        let status = client.get_query_result(req).await.unwrap_err();
        assert_eq!(status.code(), Code::Unavailable);
        assert!(started.elapsed() < timeout);
    }

    #[test]
    fn scheduler_transport_errors_report_connection_guidance() {
        let uri = closed_port_uri();

        for wrapped_in_status in [false, true] {
            let transport_error = RUNTIME
                .0
                .block_on(Channel::from_shared(uri.clone()).unwrap().connect())
                .unwrap_err();
            let transport_error = SchedulerTransportError(transport_error);
            assert!(
                transport_error
                    .source()
                    .unwrap()
                    .is::<tonic::transport::Error>()
            );
            let error = if wrapped_in_status {
                // Unknown statuses also retain the context through their source.
                let mut status = Status::unknown("transport error");
                status.set_source(Arc::new(transport_error));
                ApiError::from(status)
            } else {
                ApiError::from(transport_error)
            };

            Python::attach(|py| assert_connection_guidance(py, error.into()));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn scheduler_application_errors_are_preserved() {
        for code in [
            Code::Unauthenticated,
            Code::PermissionDenied,
            Code::InvalidArgument,
            Code::Internal,
            Code::Unknown,
            Code::Unavailable,
        ] {
            let channel = BoxCloneSyncService::new(tower::service_fn(move |_request| async move {
                Ok::<_, tonic::transport::Error>(
                    Status::with_details(
                        code,
                        "original error",
                        "ValueError: invalid query".into(),
                    )
                    .into_http(),
                )
            }));
            let mut client = SchedulerGrpcClient::new(channel);
            let error = client
                .inner
                .get_compute_versions(Request::new(GetComputeVersionsRequest {}.into()))
                .await
                .unwrap_err();
            assert_eq!(error.code(), code);
            assert_eq!(error.message(), "original error");
            assert_eq!(error.details(), b"ValueError: invalid query");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_is_retried() {
        let params = unavailable_retry_params();
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let channel = BoxCloneSyncService::new(tower::service_fn(move |_request| {
            counted.fetch_add(1, Ordering::Relaxed);
            async {
                Ok::<_, tonic::transport::Error>(
                    Status::unavailable("connection refused").into_http(),
                )
            }
        }));
        let mut client = SchedulerGrpcClient::new(channel).inner;

        let status = client
            .get_compute_versions(Request::new(GetComputeVersionsRequest {}.into()))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(calls.load(Ordering::Relaxed), params.max_retries + 1);
    }

    #[test]
    fn unreachable_observatory_reports_its_address() {
        let uri = closed_port_uri();
        let client = ObservatoryRestClient::build(&options(&uri, None).unwrap(), None).unwrap();

        let error: PyErr = RUNTIME
            .0
            .block_on(client.get_json::<QueryDetailModel>(&Uuid::nil().to_string()))
            .expect_err("a closed port should fail")
            .into();

        Python::attach(|py| {
            assert!(error.is_instance_of::<PyRuntimeError>(py));
            let message = cause_chain(py, error);
            assert!(
                message.contains(&format!(
                    "Could not reach the cluster's observatory at {uri}"
                )),
                "{message}"
            );
            assert!(message.contains("error sending request"), "{message}");
        });
    }

    #[test]
    fn observatory_error_statuses_show_the_server_message() {
        let not_found = serve_once(
            "404 Not Found",
            r#"{"message": "Query not found", "errors": {}}"#,
        );
        let server_error = serve_once(
            "500 Internal Server Error",
            r#"{"message": "Internal_server_error", "errors": {}}"#,
        );

        Python::attach(|py| {
            for (uri, message) in [
                (not_found, "Query not found"),
                (server_error, "Internal_server_error"),
            ] {
                let client =
                    ObservatoryRestClient::build(&options(&uri, None).unwrap(), None).unwrap();
                let error = py.detach(|| {
                    RUNTIME
                        .0
                        .block_on(client.get_json::<QueryDetailModel>("id"))
                        .expect_err("an error status should fail")
                });
                assert!(matches!(error, ApiError::StatusError { .. }));
                let error = error.to_string();
                assert!(error.starts_with(message), "{error}");
                // A response means the observatory was reached.
                assert!(!error.contains("Could not reach"), "{error}");
            }
        });
    }

    /// A local HTTP server that answers a single request with `status` and a JSON `body`.
    fn serve_once(status: &'static str, body: &'static str) -> String {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let uri = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = stream.read(&mut [0; 4096]);
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        uri
    }

    fn cause_chain(py: Python<'_>, error: PyErr) -> String {
        std::iter::successors(Some(error), |e| e.cause(py))
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// A URI for a local port that nothing is listening on.
    fn closed_port_uri() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", listener.local_addr().unwrap())
    }

    /// Asserts the scheduler guidance is raised with the transport failure in its cause chain.
    fn assert_connection_guidance(py: Python<'_>, error: PyErr) {
        assert!(error.is_instance_of::<PyRuntimeError>(py));
        let message = cause_chain(py, error);
        assert!(message.contains("cluster is running"), "{message}");
        assert!(message.contains("transport error"), "{message}");
    }

    fn options(uri: &str, domain_name: Option<String>) -> Result<ClientOptions, PyErr> {
        let parsed_uri = uri.parse::<Uri>().unwrap().try_into()?;

        Ok(ClientOptions {
            uri: parsed_uri,
            domain_name: domain_name.map(DomainName::try_from).transpose()?,
            extra_headers: None,
            tls_options: None,
        })
    }

    #[test]
    fn base_url_unchanged_without_authority() {
        let client =
            ObservatoryRestClient::build(&options("https://1.2.3.4:8080", None).unwrap(), None)
                .unwrap();
        assert_eq!(client.base_url, "https://1.2.3.4:8080");
    }

    #[test]
    fn base_url_host_replaced_with_authority() {
        let client = ObservatoryRestClient::build(
            &options("https://1.2.3.4:8080", Some("pola.rs".to_string())).unwrap(),
            None,
        )
        .unwrap();
        assert_eq!(client.base_url, "https://pola.rs:8080");
    }

    #[test]
    fn error_when_uri_host_is_hostname_not_ip() {
        let err = ObservatoryRestClient::build(
            &options(
                "https://somehost.example.com:8080",
                Some("myservice.pola.rs".to_string()),
            )
            .unwrap(),
            None,
        )
        .unwrap_err();
        assert!(matches!(err, ApiError::PyErr(_)));
    }

    #[test]
    fn error_when_uri_has_no_scheme() {
        assert!(options("somehost.example.com:8080", None).is_err());
    }

    #[test]
    fn resolve_redirects_connection_to_original_ip() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let host = "myservice.pola.rs".to_string();

        let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();

        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                buf.truncate(n);
                let _ = tx.send(buf);
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
            }
        });

        let client = ObservatoryRestClient::build(
            &options(&format!("http://127.0.0.1:{port}"), Some(host.clone())).unwrap(),
            None,
        )
        .unwrap();

        let response = RUNTIME
            .block_on(client.request(Method::GET, "/test").send())
            .unwrap()
            .unwrap();

        assert_eq!(response.status(), 200);

        let request_bytes = rx.recv().unwrap();
        let request_str = String::from_utf8_lossy(&request_bytes);

        assert!(
            request_str.contains(&format!("host: {host}:{port}")),
            "Host header not set to authority, got:\n{request_str}"
        );
    }
}
