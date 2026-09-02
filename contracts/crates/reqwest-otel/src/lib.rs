use opentelemetry::global;
use opentelemetry::trace::Status;
use opentelemetry_http::HeaderInjector;
#[allow(deprecated)]
use opentelemetry_semantic_conventions::attribute::PEER_SERVICE;
use opentelemetry_semantic_conventions::trace::{
    ERROR_TYPE, EXCEPTION_MESSAGE, HTTP_REQUEST_METHOD, HTTP_RESPONSE_STATUS_CODE, SERVER_ADDRESS,
    SERVER_PORT, URL_FULL, URL_PATH, URL_QUERY, URL_SCHEME,
};
use reqwest_middleware::reqwest::{Request, Response};
use reqwest_middleware::{Middleware, Next};
use tracing::field::Empty;
use tracing::{Instrument as _, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

#[derive(Default)]
pub struct TracingMiddleware {
    peer_service: Option<&'static str>,
    span_name: Option<&'static str>,
    propagate: bool,
}

impl TracingMiddleware {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn peer_service(mut self, peer_service: &'static str) -> Self {
        self.peer_service = Some(peer_service);
        self
    }

    pub fn span_name(mut self, span_name: &'static str) -> Self {
        self.span_name = Some(span_name);
        self
    }

    pub fn propagate(mut self) -> Self {
        self.propagate = true;
        self
    }

    #[allow(deprecated)]
    fn span(&self, method: &http::Method, url: &url::Url) -> Span {
        let name = match self.span_name {
            Some(name) => name.to_string(),
            None => format!("{} {}", method, url.path()),
        };
        tracing::info_span!(
            "http_request",
            "otel.kind" = "client",
            "otel.name" = %name,
            { PEER_SERVICE } = self.peer_service,
            { HTTP_REQUEST_METHOD } = %method,
            { URL_FULL } = %url,
            { URL_SCHEME } = url.scheme(),
            { URL_PATH } = url.path(),
            { URL_QUERY } = url.query(),
            { SERVER_ADDRESS } = url.host_str(),
            { SERVER_PORT } = url.port_or_known_default(),
            { HTTP_RESPONSE_STATUS_CODE } = Empty,
            { ERROR_TYPE } = Empty,
            { EXCEPTION_MESSAGE } = Empty,
        )
    }

    fn inject(&self, span: &Span, headers: &mut http::HeaderMap) {
        if !self.propagate {
            return;
        }
        let context = span.context();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&context, &mut HeaderInjector(headers))
        });
    }
}

fn record_status(span: &Span, status: http::StatusCode) {
    span.record(HTTP_RESPONSE_STATUS_CODE, status.as_u16());
    if status.is_client_error() || status.is_server_error() {
        span.record(ERROR_TYPE, status.as_u16());
        span.set_status(Status::error(status.to_string()));
    }
}

fn record_error(span: &Span, error: &dyn std::fmt::Display) {
    span.record(ERROR_TYPE, "_OTHER");
    span.record(EXCEPTION_MESSAGE, tracing::field::display(error));
    span.set_status(Status::error(error.to_string()));
}

#[async_trait::async_trait]
impl Middleware for TracingMiddleware {
    async fn handle(
        &self,
        mut req: Request,
        extensions: &mut http::Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        let span = self.span(req.method(), req.url());
        self.inject(&span, req.headers_mut());

        let result = next.run(req, extensions).instrument(span.clone()).await;
        match &result {
            Ok(response) => record_status(&span, response.status()),
            Err(error) => record_error(&span, error),
        }
        result
    }
}
