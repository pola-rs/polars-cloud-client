use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, IpcMessage};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use client_core::{ApiError, ApiResult, RUNTIME};
use protos_common::QueryIdentifier;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyResult, Python, pyclass, pymethods};
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::StreamExt as _;
use tonic::{Code, GrpcMethod};
use tower::Service;
use tower::retry::backoff::ExponentialBackoffMaker;
use tower::util::rng::HasherRng;
use tower_grpc_retry::{HttpRetry, Params, RetryUnavailable};

pub(crate) struct AddPolicyService<S> {
    inner: S,
}

impl<S> Service<http::Request<tonic::body::Body>> for AddPolicyService<S>
where
    S: Service<http::Request<tonic::body::Body>>,
{
    type Response = S::Response;
    type Future = S::Future;
    type Error = S::Error;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<tonic::body::Body>) -> Self::Future {
        let non_retryable_message = if let Some(grpc_method) = req.extensions().get::<GrpcMethod>()
            && grpc_method.method() == "DoGet"
        {
            Some("Migrating Partition")
        } else {
            None
        };
        req.extensions_mut().insert(FlightRetryPolicy::new(
            non_retryable_message,
            Params {
                max_retries: 5,
                max_request_bytes: 4 * 1024 * 1024,
                backoff: Some(
                    ExponentialBackoffMaker::new(
                        Duration::from_secs(1),
                        Duration::from_secs(60),
                        0.05,
                        HasherRng::new(),
                    )
                    .unwrap(),
                ),
            },
        ));
        self.inner.call(req)
    }
}

#[derive(Clone)]
pub(crate) struct Flight<S>(S);

impl<S> Flight<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self(inner)
    }
}

pub(crate) type FlightClient<S> =
    arrow_flight::FlightClient<AddPolicyService<HttpRetry<FlightRetryPolicy, S>>>;

pub(crate) fn create_flight_client<S>(channel: S) -> FlightClient<S>
where
    S: Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = tonic::transport::Error,
        > + Send
        + Clone
        + 'static,
    S::Future: Send + 'static,
{
    let client = FlightServiceClient::new(AddPolicyService {
        inner: HttpRetry::new(channel),
    });
    arrow_flight::FlightClient::new_from_inner(client)
}

pub(crate) struct RecordBatchStreamReader {
    _tasks: JoinSet<()>,
    pub(crate) schema: SchemaRef,
    pub(crate) rx: mpsc::Receiver<Result<RecordBatch, FlightError>>,
}

impl Iterator for RecordBatchStreamReader {
    type Item = Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(
            RUNTIME
                .block_on(self.rx.recv())
                .map_err(|e| FlightError::from_external_error(e.into()))
                .transpose()?
                .flatten()
                .map_err(|e| match e {
                    FlightError::Arrow(arrow_error) => arrow_error,
                    FlightError::NotYetImplemented(msg) => ArrowError::NotYetImplemented(msg),
                    FlightError::Tonic(status) => {
                        ArrowError::IoError(status.to_string(), std::io::Error::other(status))
                    },
                    FlightError::DecodeError(err) | FlightError::ProtocolError(err) => {
                        ArrowError::IpcError(err)
                    },
                    FlightError::ExternalError(error) => ArrowError::from_external_error(error),
                }),
        )
    }
}

impl RecordBatchReader for RecordBatchStreamReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }
}

impl<S> Flight<S>
where
    S: Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = tonic::transport::Error,
        > + Send
        + Clone
        + 'static,
    S::Future: Send + 'static,
{
    async fn scan_flight_impl(
        &self,
        query_id: QueryIdentifier,
    ) -> Result<RecordBatchStreamReader, FlightError> {
        let descriptor = FlightDescriptor::new_path(vec![query_id.to_string()]);
        let mut client = create_flight_client(self.0.clone());
        let initial_flight_info = client.poll_flight_info(descriptor).await?;
        let info = initial_flight_info
            .info
            .ok_or_else(|| FlightError::protocol("PollInfo.info missing"))?;
        let ordered = info.ordered;
        let schema: Schema = IpcMessage(info.schema.clone()).try_into()?;
        let (endpoints_tx, mut endpoint_rx) = mpsc::unbounded_channel();
        let mut received_endpoints = info.endpoint.len();
        for endpoint in info.endpoint {
            let _ = endpoints_tx.send(endpoint);
        }

        let (tx, rx) = mpsc::channel(1);
        let runtime = RUNTIME.0.handle();
        let mut tasks = JoinSet::new();
        tasks.spawn_on(
            {
                let tx = tx.clone();
                let mut flight_descriptor = initial_flight_info.flight_descriptor;
                async move {
                    while let Some(descriptor) = flight_descriptor {
                        let response = select! {
                            response = client.poll_flight_info(descriptor) => {
                                response
                            }
                            _ = endpoints_tx.closed() => {
                                return
                            }
                        };
                        match response {
                            Ok(response) => {
                                let info = response.info.expect("Expected info");
                                let n_endpoints = info.endpoint.len();
                                if n_endpoints > received_endpoints {
                                    for endpoint in
                                        info.endpoint.into_iter().skip(received_endpoints)
                                    {
                                        if endpoints_tx.send(endpoint).is_err() {
                                            break;
                                        }
                                    }
                                    received_endpoints = n_endpoints;
                                }
                                flight_descriptor = response.flight_descriptor;
                            },
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                return;
                            },
                        }
                    }
                }
            },
            runtime,
        );

        let mut client = create_flight_client(self.0.clone());
        tasks.spawn_on(
            async move {
                let mut queue = VecDeque::new();

                let mut scratch = Vec::new();
                loop {
                    if endpoint_rx.recv_many(&mut scratch, usize::MAX).await == 0 {
                        break;
                    }
                    queue.extend(scratch.drain(..));
                    while let Some(endpoint) = queue.pop_front() {
                        // All results are streamed through the scheduler, so we expect the location
                        // list to be empty, signalling this
                        assert!(endpoint.location.is_empty());
                        let ticket = endpoint
                            .ticket
                            .clone()
                            .expect("Flight endpoint did not contain a ticket");
                        let response = client.do_get(ticket).await;
                        match response {
                            Ok(mut batches) => {
                                while let Some(msg) = batches.next().await {
                                    let is_err = msg.is_err();
                                    if tx.send(msg).await.is_err() || is_err {
                                        break;
                                    }
                                }
                            },
                            Err(e) => {
                                if let FlightError::Tonic(ref status) = e
                                    && status.code() == Code::Unavailable
                                    && status.message() == "Migrating Partition"
                                {
                                    if ordered {
                                        queue.push_front(endpoint);
                                        tokio::time::sleep(Duration::from_secs(1)).await
                                    } else {
                                        queue.push_back(endpoint);
                                    }
                                } else {
                                    let _ = tx.send(Err(e)).await;
                                    break;
                                }
                            },
                        }
                    }
                }
            },
            runtime,
        );

        Ok::<_, FlightError>(RecordBatchStreamReader {
            schema: schema.into(),
            rx,
            _tasks: tasks,
        })
    }

    pub(crate) async fn scan_flight(&self, query_id: QueryIdentifier) -> ApiResult<FlightResult> {
        let reader = self.scan_flight_impl(query_id).await.map_err(|e| match e {
            FlightError::Tonic(status) => ApiError::GRPCError(status),
            e => ApiError::Other(e.into()),
        })?;
        Ok(FlightResult(Mutex::new(Some(reader))))
    }
}

#[pyclass]
pub struct FlightResult(Mutex<Option<RecordBatchStreamReader>>);

#[pymethods]
impl FlightResult {
    #[pyo3(signature = (schema=None))]
    pub fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = schema;
        let reader = self.0.lock().unwrap().take().ok_or_else(|| {
            PyRuntimeError::new_err("__arrow_c_stream called more than once on FlightResult")
        })?;
        let reader = FFI_ArrowArrayStream::new(Box::new(reader));
        PyCapsule::new_with_value(py, reader, c"arrow_array_stream")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FlightRetryPolicy {
    excluded_message: Option<&'static str>,
    inner: RetryUnavailable,
}

impl FlightRetryPolicy {
    fn new(excluded_message: Option<&'static str>, params: Params) -> Self {
        let inner = RetryUnavailable::new(params);
        Self {
            inner,
            excluded_message,
        }
    }
}

impl tower_grpc_retry::Policy<tonic::transport::Error> for FlightRetryPolicy {
    fn is_retryable(
        &self,
        result: Result<
            &http::Response<tower_grpc_retry::PeekTrailersBody>,
            &tonic::transport::Error,
        >,
    ) -> bool {
        <RetryUnavailable as tower_grpc_retry::Policy<tonic::transport::Error>>::is_retryable(
            &self.inner,
            result,
        ) && {
            if let Some(not_message) = self.excluded_message
                && let Ok(response) = result
                && let Some(trailers) = response.body().peek_trailers()
                && let Some(message) = trailers.get("grpc-message")
                && let Ok(msg) = message.to_str()
                && msg == not_message
            {
                false
            } else {
                true
            }
        }
    }

    fn params(&self) -> tower_grpc_retry::Params {
        <RetryUnavailable as tower_grpc_retry::Policy<tonic::transport::Error>>::params(&self.inner)
    }

    fn set_extensions(&self, dst: &mut http::Extensions, orig: &http::Extensions) {
        dst.extend(orig.clone());
    }
}
