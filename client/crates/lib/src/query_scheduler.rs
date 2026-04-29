#![allow(clippy::result_large_err)]

use std::str::FromStr;
use std::time::Duration;

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
use protos_client_compute::observatory::{
    GetQueryProfileRequest, QueryProfile, QueryProfileServiceClient,
};
use protos_common::tonic::codegen::http::uri::Scheme;
use protos_common::tonic::metadata::{MetadataKey, MetadataValue};
use protos_common::tonic::service::interceptor::InterceptedService;
use protos_common::tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity, Uri};
use protos_common::tonic::{self, Code, Request};
use protos_common::{
    ComputeVersions, MAX_MESSAGE_LENGTH_UNLIMITED, PlanFormat, QueryIdentifier, QueryInfo,
    QueryPlans,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::{Python, pyclass, pymethods};
use reqwest::header::AUTHORIZATION;
use utils::{Backoff, Exponential, retry};
use uuid::Uuid;

use crate::VERSIONS;
use crate::entry::EnterRustExt;
use crate::query_settings::{PyLineageContext, PyQuerySettings};
use crate::serde_types::{
    QueryDetailPy, QueryInfoPy, QueryProfilePy, query_profile_to_py, query_result_to_py,
};

type SchedulerGRPCClient =
    ClientServiceClient<InterceptedService<Channel, fn(Request<()>) -> tonic::Result<Request<()>>>>;

type ObservatoryGRPCClient = QueryProfileServiceClient<
    InterceptedService<Channel, fn(Request<()>) -> tonic::Result<Request<()>>>,
>;

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct SchedulerClient {
    scheduler_client: SchedulerGRPCClient,
    observatory_client_grpc: ObservatoryGRPCClient,
    observatory_client_rest: ObservatoryRestClient,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[pyclass(from_py_object, eq, eq_int)]
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

#[derive(Clone)]
struct ObservatoryRestClient {
    inner: reqwest::Client,
    base_url: String,
}

impl ObservatoryRestClient {
    fn build(options: &ClientOptions, base_url: String) -> Result<Self, reqwest::Error> {
        let mut builder = reqwest::ClientBuilder::new();

        if let Some(cert_bytes) = &options.public_server_crt {
            let cert = reqwest::Certificate::from_pem(cert_bytes)?;
            builder = builder.add_root_certificate(cert);
        }

        if let (Some(cert), Some(key)) = (&options.tls_certificate, &options.tls_private_key) {
            let mut pem = cert.clone();
            pem.extend_from_slice(key);
            let identity = reqwest::Identity::from_pem(&pem)?;
            builder = builder.identity(identity);
        }

        if options.tls_cert_domain.is_some() {
            builder = builder.https_only(true);
        }

        if options.insecure {
            builder = builder
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true);
        }

        Ok(Self {
            inner: builder.build()?,
            base_url,
        })
    }

    pub fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!(
            "{}/api/v1/query/{}",
            self.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        );
        self.inner.request(method, url)
    }

    pub fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::GET, path)
    }
}

#[pymethods]
impl SchedulerClient {
    #[new]
    pub fn new(
        address: &str,
        grpc_port: u16,
        observatory_port: u16,
        mut client_options: ClientOptions,
    ) -> ApiResult<SchedulerClient> {
        // Overwrite insecure for localhost or 127.0.0.1
        if address == "localhost" || address == "127.0.0.1" {
            client_options.insecure = true;
        }

        let grpc_address = format!("{address}:{grpc_port}");

        let observatory_address = if address.starts_with("http") {
            format!("{address}:{observatory_port}")
        } else {
            format!("http://{address}:{observatory_port}")
        };

        let observatory_client_rest =
            ObservatoryRestClient::build(&client_options, observatory_address)?;

        let channel =
            RUNTIME.block_on(async move { get_channel(&grpc_address, client_options).await })??;
        let scheduler_client =
            ClientServiceClient::with_interceptor(channel.clone(), version_interceptor as _)
                .max_encoding_message_size(MAX_MESSAGE_LENGTH_UNLIMITED)
                .max_decoding_message_size(MAX_MESSAGE_LENGTH_UNLIMITED);

        let observatory_client_grpc =
            QueryProfileServiceClient::with_interceptor(channel, version_interceptor as _)
                .max_encoding_message_size(MAX_MESSAGE_LENGTH_UNLIMITED)
                .max_decoding_message_size(MAX_MESSAGE_LENGTH_UNLIMITED);

        Ok(SchedulerClient {
            scheduler_client,
            observatory_client_grpc,
            observatory_client_rest,
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
                self.scheduler_client.clone().cancel_query(req).await
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
                let result = self.scheduler_client.clone().get_query_status(req).await?;
                Ok::<_, ApiError>(QueryStatus::from(result.into_inner()))
            })
        })??;

        match result {
            QueryStatus::Unspecified => Err(ApiError::PyErr(PyRuntimeError::new_err(
                "Server returned unknown query status code",
            ))),
            QueryStatus::Scheduled => Ok(QueryStatusCodeModel::Scheduled),
            QueryStatus::InProgress => Ok(QueryStatusCodeModel::InProgress),
            QueryStatus::Success => Ok(QueryStatusCodeModel::Success),
            QueryStatus::Failed => Ok(QueryStatusCodeModel::Failed),
            QueryStatus::Canceled => Ok(QueryStatusCodeModel::Canceled),
        }
    }

    pub fn get_direct_query_result(
        &self,
        py: Python<'_>,
        query_id: Uuid,
        token: Option<String>,
    ) -> ApiResult<QueryInfoPy> {
        py.enter_rust(|| {
            let query_id = QueryIdentifier::from(query_id);
            RUNTIME.block_on(async move {
                let mut req = Request::new(query_id.into());
                req = insert_auth_token(req, token);
                let result = self.scheduler_client.clone().get_query_result(req).await?;
                Ok(result.into_inner().into())
            })
        })?
        .map(
            |GetQueryResultResponse {
                 result,
                 compute_info,
             }| query_result_to_py(py, result, Some(compute_info)),
        )
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
                let result = self.scheduler_client.clone().submit_query(req).await?;
                Ok(result.into_inner())
            })
        })?
        .map(|response| QueryIdentifier::from(response).inner)
    }

    pub fn get_direct_query_profile(
        &self,
        py: Python<'_>,
        query_id: Uuid,
        tag: Option<Vec<u8>>,
        token: Option<String>,
    ) -> ApiResult<Option<QueryProfilePy>> {
        py.enter_rust(|| {
            RUNTIME.block_on(async move {
                let query_id = QueryIdentifier::from(query_id);

                let mut req = Request::new(
                    GetQueryProfileRequest {
                        query_id,
                        tag: tag.map(Into::into),
                    }
                    .into(),
                );
                req = insert_auth_token(req, token);
                let response = self
                    .observatory_client_grpc
                    .clone()
                    .get_query_profile(req)
                    .await?;
                Ok(response.into_inner().into())
            })
        })?
        .map(|response: Option<QueryProfile>| {
            response.map(|profile| query_profile_to_py(py, profile))
        })
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
                    let mut client = self.scheduler_client.clone();
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
#[derive(Debug, Clone, Default)]
pub struct ClientOptions {
    #[pyo3(get, set)]
    pub tls_cert_domain: Option<String>,
    #[pyo3(get, set)]
    pub public_server_crt: Option<Vec<u8>>,
    #[pyo3(get, set)]
    pub tls_certificate: Option<Vec<u8>>,
    #[pyo3(get, set)]
    pub tls_private_key: Option<Vec<u8>>,
    #[pyo3(get, set)]
    pub insecure: bool,
}

#[pymethods]
impl ClientOptions {
    #[new]
    fn new() -> Self {
        Self::default()
    }
}

#[allow(clippy::result_large_err)]
async fn get_channel(address: &str, client_options: ClientOptions) -> ApiResult<Channel> {
    let uri_builder = Uri::builder().authority(address).path_and_query("/");

    let endpoint = if client_options.insecure {
        let uri = uri_builder.scheme(Scheme::HTTP).build().unwrap();
        Channel::builder(uri)
    } else {
        let public_server_cert = client_options
            .public_server_crt
            .expect("expected public_server_cert");

        let uri = uri_builder.scheme(Scheme::HTTPS).build().unwrap();
        let ca = Certificate::from_pem(public_server_cert);
        let cert_domain = client_options
            .tls_cert_domain
            .unwrap_or("pola.rs".to_string());

        let mut tls = ClientTlsConfig::new()
            .ca_certificate(ca)
            .domain_name(cert_domain);

        if let Some(certificate) = client_options.tls_certificate
            && let Some(private_key) = client_options.tls_private_key
        {
            let identity = Identity::from_pem(certificate, private_key);
            tls = tls.identity(identity);
        }

        Channel::builder(uri).tls_config(tls)?
    };

    utils::retry! {
        utils::retry::Exponential::new(Duration::from_secs(1)).maximum(Duration::from_secs(5)).deadline(Duration::from_secs(60)),
        async {
            let res = endpoint
                .clone()
                .user_agent(user_agent(VERSIONS.get().unwrap().as_ref().map(|(_, versions)| versions)))
                .unwrap()
                .connect()
                .await?;

            Ok::<_, ApiError>(res)
        },
        tokio::time::sleep
    }
    .await
}

#[allow(clippy::result_large_err)]
fn version_interceptor(mut request: Request<()>) -> Result<Request<()>, tonic::Status> {
    let (_, versions) = VERSIONS.get().unwrap().clone().unwrap();
    let metadata = request.metadata_mut();
    metadata.insert(
        "x-client-version",
        versions.polars_cloud.as_bytes().try_into().unwrap(),
    );
    metadata.insert(
        "x-polars-version",
        versions.polars.as_bytes().try_into().unwrap(),
    );
    Ok(request)
}
