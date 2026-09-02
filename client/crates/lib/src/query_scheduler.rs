#![allow(clippy::result_large_err)]

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use client_core::constants::SERVICE_NAME;
use client_core::error::ApiResult;
use client_core::{ApiError, RUNTIME};
use pc_observatory_models::QueryDetailModel;
use polars_axum_models::QueryStatusCodeModel;
use polars_backend_client::client::user_agent;
use protos_client_compute::client::client::SubmitQueryRequest;
use protos_client_compute::client::{
    ClientServiceClient, GetComputeVersionsRequest, GetQueryPlansRequest, GetQueryResultResponse,
    PlanSelection, QueryStatus,
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
    QueryPlans,
};
use pyo3::exceptions::{PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use reqwest::header::{AUTHORIZATION, HeaderName};
use reqwest_otel::TracingMiddleware;
use tokio_rustls::rustls;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::{DigitallySignedStruct, SignatureScheme};
use tonic::Status;
use tower::ServiceBuilder;
use tower::util::BoxCloneSyncService;
use tower_http::set_header::HeaderMetadata;
use tower_http::set_header::request::SetMultipleRequestHeadersLayer;
use tracing::Instrument;
use utils::{Backoff, Exponential, retry};
use uuid::Uuid;

use crate::VERSIONS;
use crate::entry::EnterRustExt;
use crate::flight::{Flight, FlightResult};
use crate::query_settings::{PyLineageContext, PyQuerySettings};
use crate::serde_types::{
    DomainName, ParsedHeaders, ParsedUri, QueryDetailPy, QueryInfoPy, query_result_to_py,
    query_status_to_code,
};

type SchedulerChannel =
    BoxCloneSyncService<http::Request<Body>, http::Response<Body>, tonic::transport::Error>;

type SchedulerGRPCClient = ClientServiceClient<SchedulerChannel>;

const POLAR_CLOUD_VERSION_HEADER_NAME: HeaderName = HeaderName::from_static("x-client-version");
const POLAR_VERSION_HEADER_NAME: HeaderName = HeaderName::from_static("x-polars-version");
const GRPC_RETRY_CONNECTION_DEADLINE: Duration = Duration::from_secs(60);

const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(20);
const LONG_POLL_RETRY_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct SchedulerGrpcClient {
    inner: SchedulerGRPCClient,
}

fn connect_scheduler(options: &ClientOptions) -> ApiResult<SchedulerChannel> {
    let channel = RUNTIME.block_on(async { create_channel(options).await })??;

    let version_layers = version_header_layers();

    Ok(BoxCloneSyncService::new(
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
            .service(channel),
    ))
}

impl SchedulerGrpcClient {
    fn new(channel: SchedulerChannel) -> Self {
        Self {
            inner: ClientServiceClient::new(channel)
                .max_encoding_message_size(MAX_MESSAGE_LENGTH_UNLIMITED)
                .max_decoding_message_size(MAX_MESSAGE_LENGTH_UNLIMITED),
        }
    }
}

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct SchedulerClient {
    scheduler_client: SchedulerGrpcClient,
    observatory_client_rest: ObservatoryRestClient,
    flight_client: Flight<SchedulerChannel>,
}

#[pyclass(from_py_object, eq, eq_int)]
#[derive(Clone, PartialEq, Eq)]
pub enum PlanFormatPy {
    Dot,
    Explain,
}

#[pyclass(skip_from_py_object, get_all)]
pub struct QueryPlansPy {
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
    fn build(options: &ClientOptions) -> Result<Self, ApiError> {
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

        Ok(Self {
            inner: reqwest_middleware::ClientBuilder::new(builder.build()?)
                .with(TracingMiddleware::new().propagate())
                .build(),
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
}

#[pymethods]
impl SchedulerClient {
    #[new]
    #[tracing::instrument(name = "SchedulerClient::new", skip_all)]
    pub fn new(scheduler: ClientOptions, observatory: ClientOptions) -> ApiResult<SchedulerClient> {
        let observatory_client_rest = ObservatoryRestClient::build(&observatory)?;
        let scheduler_channel = connect_scheduler(&scheduler)?;
        let scheduler_client = SchedulerGrpcClient::new(scheduler_channel.clone());
        let flight = Flight::new(scheduler_channel);

        Ok(SchedulerClient {
            scheduler_client,
            observatory_client_rest,
            flight_client: flight,
        })
    }

    pub fn cancel_direct_query(
        &self,
        py: Python,
        query_id: Uuid,
        token: Option<String>,
    ) -> ApiResult<()> {
        let _ = py.enter_rust(|| {
            RUNTIME.block_on(async move {
                let query_id = QueryIdentifier::from(query_id);
                let mut req = Request::new(query_id.into());
                req = insert_auth_token(req, token);
                self.scheduler_client.inner.clone().cancel_query(req).await
            })
        })?;
        Ok(())
    }

    pub fn delete_direct_query_result(
        &self,
        py: Python,
        query_id: Uuid,
        token: Option<String>,
    ) -> ApiResult<()> {
        let _ = py.enter_rust(|| {
            RUNTIME.block_on(async move {
                let query_id = QueryIdentifier::from(query_id);
                let mut req = Request::new(query_id.into());
                req = insert_auth_token(req, token);
                self.scheduler_client
                    .inner
                    .clone()
                    .delete_query_result(req)
                    .await
            })
        })?;
        Ok(())
    }

    pub fn get_direct_query_status(
        &self,
        py: Python,
        query_id: Uuid,
        token: Option<String>,
    ) -> ApiResult<QueryStatusCodeModel> {
        let result = py.enter_rust(|| {
            let query_id = QueryIdentifier::from(query_id);

            RUNTIME.block_on(async move {
                let mut req = Request::new(query_id.into());
                req = insert_auth_token(req, token);
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

    #[tracing::instrument(name = "await_query_result", skip(self, py, token_factory), fields(%query_id))]
    pub fn get_direct_query_result(
        &self,
        py: Python<'_>,
        query_id: Uuid,
        token_factory: Py<PyAny>,
        timeout_ms: u64,
    ) -> ApiResult<QueryInfoPy> {
        py.detach(|| {
            let query_id = QueryIdentifier::from(query_id);
            RUNTIME.block_on(async move {
                let deadline = Instant::now() + Duration::from_millis(timeout_ms);
                loop {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(ApiError::PyErr(PyTimeoutError::new_err(
                            "timed out waiting for the query result",
                        )));
                    }

                    let token = Python::attach(|py| -> PyResult<Option<String>> {
                        token_factory.bind(py).call0()?.extract()
                    })?;

                    let mut req = Request::new(query_id.into());
                    req = insert_auth_token(req, token);
                    req.set_timeout(remaining);

                    let mut client = self.scheduler_client.inner.clone();
                    match tokio::time::timeout(remaining, client.get_query_result(req)).await {
                        Ok(Ok(response)) => {
                            return Ok::<_, ApiError>(response.into_inner().into());
                        },
                        Ok(Err(status)) if status.code() == Code::Unavailable => {
                            let backoff = LONG_POLL_RETRY_BACKOFF
                                .min(deadline.saturating_duration_since(Instant::now()));
                            tokio::time::sleep(backoff).await;
                        },
                        Ok(Err(status)) if status.code() == Code::DeadlineExceeded => {
                            return Err(ApiError::PyErr(PyTimeoutError::new_err(
                                "timed out waiting for the query result",
                            )));
                        },
                        Ok(Err(status)) => return Err(ApiError::from(status)),
                        Err(_) => {
                            return Err(ApiError::PyErr(PyTimeoutError::new_err(
                                "timed out waiting for the query result",
                            )));
                        },
                    }
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

    pub fn scan_flight(
        &self,
        py: Python<'_>,
        query_id: Uuid,
        token: Option<String>,
    ) -> ApiResult<Option<FlightResult>> {
        py.enter_rust(|| {
            let query_id = QueryIdentifier::from(query_id);
            RUNTIME.block_on(async {
                let req = insert_auth_token(Request::new(GetQueryStatusRequest{
                    query_id: Some(query_id.into()),
                }), token);
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

    #[pyo3(signature = (plan, settings, token, username=None, labels=None, execution_id=None, lineage_context=None))]
    #[expect(clippy::too_many_arguments)]
    pub fn do_query(
        &self,
        py: Python<'_>,
        plan: Vec<u8>,
        settings: PyQuerySettings,
        token: Option<String>,
        username: Option<String>,
        labels: Option<Vec<String>>,
        execution_id: Option<String>,
        lineage_context: Option<PyLineageContext>,
    ) -> ApiResult<Uuid> {
        py.enter_rust(|| {
            let request = SubmitQueryRequest {
                query_info: QueryInfo {
                    labels: labels.unwrap_or_default(),
                    execution_id,
                    lineage_context: lineage_context.map(protos_common::LineageContext::from),
                },
                plan: plan.into(),
                query_settings: settings.into(),
            };

            RUNTIME.block_on(async move {
                let mut req = Request::new(request.into());
                if let Some(username) = username {
                    let shortened_username: String = username.chars().take(64).collect();
                    let metadata = MetadataValue::from_str(&shortened_username)
                        .map_err(|_e| PyValueError::new_err("Invalid username"))?;
                    let metadatakey = MetadataKey::from_str("x-polars-user").unwrap();
                    let _ = req.metadata_mut().insert(metadatakey, metadata);
                }
                req = insert_auth_token(req, token);
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

    pub fn get_query_details(
        &self,
        py: Python<'_>,
        query_id: Uuid,
        token: Option<String>,
    ) -> ApiResult<QueryDetailPy> {
        py.enter_rust(|| {
            RUNTIME.block_on(async move {
                let mut req = self.observatory_client_rest.get(&format!("{query_id}"));

                if let Some(token) = token {
                    req = req.bearer_auth(token);
                }

                let response = req
                    .send()
                    .await
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                if !response.status().is_success() {
                    return Err(ApiError::PyErr(PyRuntimeError::new_err(format!(
                        "Request failed with status: {}",
                        response.status()
                    ))));
                }

                response
                    .json::<QueryDetailModel>()
                    .await
                    .map(QueryDetailPy::from)
                    .map_err(|e| ApiError::PyErr(PyRuntimeError::new_err(e.to_string())))
            })
        })?
    }

    #[pyo3(signature = (query_id, token,  phys = false, ir = false))]
    #[tracing::instrument(name = "get_query_plans", skip(self, py, token), fields(%query_id))]
    pub fn get_direct_query_plan(
        &self,
        py: Python<'_>,
        query_id: Uuid,
        token: Option<String>,
        phys: bool,
        ir: bool,
    ) -> ApiResult<QueryPlansPy> {
        let query_plans: QueryPlans = py
            .enter_rust(|| {
                let plans = PlanSelection { ir, phys };
                RUNTIME.block_on(async move {
                    let mut client = self.scheduler_client.inner.clone();
                    retry!(
                        Exponential::new(Duration::from_millis(50))
                            .maximum(Duration::from_millis(250)),
                        async {
                            let mut req = Request::new(
                                GetQueryPlansRequest {
                                    query_id: query_id.into(),
                                    plan_selection: Some(plans),
                                }
                                .into(),
                            );
                            req = insert_auth_token(req, token.clone());
                            match client.get_query_plans(req).await {
                                Ok(r) => utils::OperationResult::Ok(r),
                                Err(s) if s.code() == Code::Unavailable => {
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

        Ok(QueryPlansPy {
            format: match query_plans.format() {
                PlanFormat::Unspecified => {
                    return Err(ApiError::PyErr(PyRuntimeError::new_err(
                        "Cluster returned unrecognized query plan format",
                    )));
                },
                PlanFormat::Dot => PlanFormatPy::Dot,
                PlanFormat::Explain => PlanFormatPy::Explain,
            },
            ir_plan: query_plans.ir_plan,
            phys_plan: query_plans.phys_plan,
        })
    }

    pub fn get_compute_versions(
        &self,
        py: Python<'_>,
        token: Option<String>,
    ) -> ApiResult<ComputeVersionsPy> {
        let versions: ComputeVersions = py
            .enter_rust(|| {
                RUNTIME.block_on(async move {
                    let mut req = Request::new(GetComputeVersionsRequest {}.into());
                    req = insert_auth_token(req, token);
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

pub(super) fn insert_auth_token<T>(mut req: Request<T>, token: Option<String>) -> Request<T> {
    if let Some(token) = token {
        req.metadata_mut().insert(
            AUTHORIZATION.as_str(),
            format!("Bearer {}", token).parse().unwrap(),
        );
    }
    req
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
                .await?;

            Ok::<_, ApiError>(res)
        },
        tokio::time::sleep
    }
        .await
}

fn version_header_layers() -> Vec<HeaderMetadata<http::Request<tonic::body::Body>>> {
    let (_, versions) = VERSIONS.get().unwrap().clone().unwrap();

    vec![
        (POLAR_CLOUD_VERSION_HEADER_NAME, Some(versions.polars_cloud)).into(),
        (POLAR_VERSION_HEADER_NAME, Some(versions.polars)).into(),
    ]
}

#[cfg(test)]
mod tests {
    use protos_common::tonic::transport::Uri;
    use reqwest::Method;

    use super::*;

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
            ObservatoryRestClient::build(&options("https://1.2.3.4:8080", None).unwrap()).unwrap();
        assert_eq!(client.base_url, "https://1.2.3.4:8080");
    }

    #[test]
    fn base_url_host_replaced_with_authority() {
        let client = ObservatoryRestClient::build(
            &options("https://1.2.3.4:8080", Some("pola.rs".to_string())).unwrap(),
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
