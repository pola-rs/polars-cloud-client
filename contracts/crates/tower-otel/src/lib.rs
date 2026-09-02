use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use opentelemetry::baggage::{Baggage, BaggageExt, BaggageMetadata};
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::Status;
use opentelemetry::{Key, StringValue};
use opentelemetry_semantic_conventions::trace::{
    CLIENT_ADDRESS, CLIENT_PORT, NETWORK_PEER_ADDRESS, NETWORK_PEER_PORT, NETWORK_TRANSPORT,
    SERVER_ADDRESS, SERVER_PORT,
};
use tonic::Code;
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tower::Layer;
use tracing::field::{Empty, display};
use tracing::{Instrument, Span, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

type BaggageFn = Arc<dyn Fn(&Span) -> Option<Baggage> + Send + Sync>;

pub trait BaggageProvider: Send + Sync {
    type Output: Into<Baggage>;

    fn get_baggage(&self, span: &Span) -> Option<Self::Output>;
}

impl<T: Into<Baggage>, F: Fn(&Span) -> Option<T> + Send + Sync> BaggageProvider for F {
    type Output = T;

    fn get_baggage(&self, span: &Span) -> Option<T> {
        self(span)
    }
}

#[derive(Clone)]
pub struct OtelService<S> {
    is_server: bool,
    inner: S,
    extra_baggage: Arc<Vec<BaggageFn>>,
    service_name: &'static str,
}

impl<S: Debug> std::fmt::Debug for OtelService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelService")
            .field("is_server", &self.is_server)
            .field("inner", &self.inner)
            .field("service_name", &self.service_name)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct OtelLayer {
    service_name: &'static str,
    is_server: bool,
    extra_baggage: Arc<Vec<BaggageFn>>,
    _private: (),
}

impl OtelLayer {
    pub fn client(service_name: &'static str) -> Self {
        Self {
            service_name,
            is_server: false,
            extra_baggage: Arc::default(),
            _private: (),
        }
    }
    pub fn server(service_name: &'static str) -> Self {
        Self {
            service_name,
            is_server: true,
            extra_baggage: Arc::default(),
            _private: (),
        }
    }

    pub fn with_baggage<P: BaggageProvider + 'static>(mut self, p: P) -> Self {
        Arc::make_mut(&mut self.extra_baggage)
            .push(Arc::new(move |span| p.get_baggage(span).map(Into::into)));
        self
    }
}

impl<S> Layer<S> for OtelLayer {
    type Service = OtelService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        OtelService {
            service_name: self.service_name,
            is_server: self.is_server,
            extra_baggage: self.extra_baggage.clone(),
            inner,
        }
    }
}

impl<S, ReqBody, ResBody> tower::Service<http::Request<ReqBody>> for OtelService<S>
where
    S: tower::Service<http::Request<ReqBody>, Response = http::Response<ResBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<ReqBody>) -> Self::Future {
        let current = tracing::Span::current();
        let parent_id = if self.is_server {
            // extracted later from baggage (text map propagator) if present
            None
        } else {
            current.id()
        };
        let span = info_span! {
            parent: parent_id,
            "RPC request",
            "service_name" = self.service_name,
            "otel.name" = Empty,
            "otel.kind" = if self.is_server { "server" } else { "client" },
            "rpc.system" = "grpc",
            "rpc.service" = "grpc",
            "rpc.method" = "grpc",
            "rpc.grpc.status_code" = Empty,
            {SERVER_ADDRESS} = Empty,
            {SERVER_PORT} = Empty,
            {NETWORK_TRANSPORT} = Empty,
            {CLIENT_ADDRESS} = Empty,
            {CLIENT_PORT} = Empty,
            {NETWORK_PEER_ADDRESS} = Empty,
            {NETWORK_PEER_PORT} = Empty,
        };
        if let Some(host) = request.uri().host() {
            span.record(SERVER_ADDRESS, host);
            let port: Option<u16> = match request.headers().get("X-Forwarded-Port") {
                Some(port) => port.to_str().ok().and_then(|port| port.parse().ok()),
                None => request.uri().port_u16(),
            };
            if let Some(port) = port {
                span.record(SERVER_PORT, port);
            }
        }
        let mut parts = request.uri().path().rsplitn(3, '/');
        if let Some(method) = parts.next()
            && let Some(service) = parts.next()
        {
            span.record("rpc.method", method);
            span.record("rpc.service", service);
            span.record("otel.name", format!("{service}/{method}"));
        }
        let ext = request.extensions();
        let connect_info = ext
            .get::<TlsConnectInfo<TcpConnectInfo>>()
            .map(|v| v.get_ref())
            .or_else(|| ext.get::<TcpConnectInfo>());
        if let Some(connect_info) = connect_info {
            span.record(NETWORK_TRANSPORT, "tcp");
            if let Some(remote_addr) = connect_info.remote_addr() {
                // These should only be set on the server.
                // Tonic only sets connection info on the server, so
                // this works fine.
                span.record(CLIENT_ADDRESS, display(remote_addr.ip()));
                span.record(CLIENT_PORT, remote_addr.port());
                span.record(NETWORK_PEER_ADDRESS, display(remote_addr.ip()));
                span.record(NETWORK_PEER_PORT, remote_addr.port());
            }
        }
        opentelemetry::global::get_text_map_propagator(|p| {
            let extractor = HeaderExtractor(request.headers());
            if p.fields().any(|field| extractor.get(field).is_some()) {
                let ctx = p.extract_with_context(&current.context(), &extractor);
                if let Err(e) = span.set_parent(ctx) {
                    tracing::warn!("unable to set span parent: {e}")
                }
            } else {
                let context = span.context();
                let clone_kv =
                    |(k, v): (&Key, &(StringValue, BaggageMetadata))| (k.clone(), v.clone());
                let mut baggage = Vec::new();
                for f in self.extra_baggage.iter() {
                    if let Some(b) = f(&current) {
                        baggage.extend(b.iter().map(clone_kv));
                    }
                }

                let context = context.with_baggage(
                    context
                        .baggage()
                        .iter()
                        .map(clone_kv)
                        .chain(baggage)
                        .collect::<Baggage>(),
                );
                p.inject_context(&context, &mut HeaderInjector(request.headers_mut()));
            }
        });

        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let is_server = self.is_server;
        Box::pin(
            async move {
                let response = inner.call(request).await?;
                if let Some(status) = response.headers().get("grpc-status") {
                    let code = Code::from_bytes(status.as_bytes());
                    use Code as C;
                    let status = match (is_server, code) {
                        (false, C::Ok) => Status::Unset,
                        (
                            true,
                            C::Unknown
                            | C::DeadlineExceeded
                            | C::Unimplemented
                            | C::Internal
                            | C::Unavailable
                            | C::DataLoss,
                        )
                        | (false, _) => {
                            if let Some(h) = response.headers().get("grpc-message")
                                && let Ok(msg) = h.to_str()
                            {
                                Status::error(msg.to_owned())
                            } else {
                                Status::error(code.description())
                            }
                        },
                        (true, _) => Status::Unset,
                    };
                    Span::current()
                        .record("rpc.grpc.status_code", code as i32)
                        .set_status(status);
                }

                Ok(response)
            }
            .instrument(span),
        )
    }
}

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key)?.to_str().ok()
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

struct HeaderExtractor<'a>(&'a http::HeaderMap);

struct HeaderInjector<'a>(&'a mut http::HeaderMap);
impl<'a> Injector for HeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        let Ok(name) = http::HeaderName::from_bytes(key.as_bytes()) else {
            return;
        };
        if let Ok(value) = value.try_into() {
            self.0.insert(name, value);
        }
    }
}
