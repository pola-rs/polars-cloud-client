use pyo3::prelude::*;

#[cfg(feature = "otlp")]
mod imp {
    use std::sync::OnceLock;
    use std::time::Duration;

    use client_core::RUNTIME;
    use client_core::constants::SERVICE_NAME as SERVICE;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::{KeyValue, global};
    use opentelemetry_otlp::WithExportConfig as _;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use opentelemetry_semantic_conventions::resource::{SERVICE_NAME, SERVICE_VERSION};
    use pyo3::prelude::*;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    static PROVIDER: OnceLock<Result<SdkTracerProvider, String>> = OnceLock::new();

    const TRACER_NAME: &str = "polars-cloud-client";

    const ENDPOINT_VAR: &str = "OTLP_ENDPOINT";

    const DEFAULT_FILTER: &str =
        "warn,polars_cloud=info,client_core=info,polars_backend_client=info";

    const FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

    fn install(endpoint: Option<String>) -> Result<(), String> {
        match PROVIDER.get_or_init(|| build_provider(endpoint)) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.clone()),
        }
    }

    fn build_provider(endpoint: Option<String>) -> Result<SdkTracerProvider, String> {
        let _guard = RUNTIME.0.enter();

        let mut builder = opentelemetry_otlp::SpanExporter::builder().with_tonic();
        if let Some(endpoint) = endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        let exporter = builder
            .build()
            .map_err(|e| format!("build OTLP span exporter: {e}"))?;

        let mut resource = Resource::builder();
        if std::env::var_os("OTEL_SERVICE_NAME").is_none() {
            resource = resource.with_attribute(KeyValue::new(SERVICE_NAME, SERVICE));
        }
        resource =
            resource.with_attribute(KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")));

        let provider = SdkTracerProvider::builder()
            .with_resource(resource.build())
            .with_batch_exporter(exporter)
            .build();
        let tracer = provider.tracer(TRACER_NAME);

        global::set_text_map_propagator(TraceContextPropagator::new());

        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| DEFAULT_FILTER.into());

        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .try_init()
            .map_err(|_| "a tracing subscriber is already installed in this process".to_string())?;

        Ok(provider)
    }

    fn env_set(var: &str) -> bool {
        std::env::var(var).is_ok_and(|v| !v.is_empty())
    }

    pub fn init_from_env() {
        let vars = [
            ENDPOINT_VAR,
            opentelemetry_otlp::OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
            opentelemetry_otlp::OTEL_EXPORTER_OTLP_ENDPOINT,
        ];
        if !vars.into_iter().any(env_set) {
            return;
        }

        let endpoint = std::env::var(ENDPOINT_VAR).ok().filter(|e| !e.is_empty());
        if let Err(e) = install(endpoint) {
            eprintln!("polars_cloud: tracing disabled ({e})");
        }
    }

    pub fn flush() -> Result<(), String> {
        let Some(Ok(provider)) = PROVIDER.get() else {
            return Ok(());
        };
        let provider = provider.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let _guard = RUNTIME.0.enter();
            let _ = tx.send(provider.force_flush());
        });
        match rx.recv_timeout(FLUSH_TIMEOUT) {
            Ok(result) => result.map_err(|e| format!("flush traces: {e}")),
            Err(_) => Err(format!("flush traces: timed out after {FLUSH_TIMEOUT:?}")),
        }
    }

    pub struct SpanHandle {
        span: tracing::Span,
        entered: Option<tracing::span::EnteredSpan>,
    }

    impl SpanHandle {
        pub fn new(name: &str) -> Self {
            Self {
                span: tracing::info_span!(
                    "polars_cloud_operation",
                    "otel.name" = %name,
                    "otel.status_code" = tracing::field::Empty,
                    "error.type" = tracing::field::Empty,
                    "exception.message" = tracing::field::Empty,
                ),
                entered: None,
            }
        }

        pub fn enter(&mut self) {
            self.entered = Some(self.span.clone().entered());
        }

        pub fn exit(&mut self, exception: Option<Bound<'_, PyAny>>) {
            if let Some(exception) = exception {
                let kind = exception
                    .get_type()
                    .name()
                    .map(|name| name.to_string())
                    .unwrap_or_else(|_| "Exception".to_string());
                self.span.record("otel.status_code", "ERROR");
                self.span.record("error.type", kind.as_str());
                self.span
                    .record("exception.message", tracing::field::display(&exception));
            }
            self.entered.take();
            self.span = tracing::Span::none();
        }
    }
}

#[cfg(not(feature = "otlp"))]
mod imp {
    use pyo3::prelude::*;

    pub fn init_from_env() {
        if std::env::var_os("OTLP_ENDPOINT").is_some_and(|e| !e.is_empty()) {
            eprintln!(
                "polars_cloud: OTLP_ENDPOINT is set but this build has tracing compiled out \
                 (rebuild with `--features otlp`)"
            );
        }
    }

    pub fn flush() -> Result<(), String> {
        Ok(())
    }

    pub struct SpanHandle;

    impl SpanHandle {
        pub fn new(_name: &str) -> Self {
            Self
        }

        pub fn enter(&mut self) {}

        pub fn exit(&mut self, _exception: Option<Bound<'_, PyAny>>) {}
    }
}

pub use imp::init_from_env;

#[pyclass(unsendable)]
pub struct TraceSpan {
    handle: imp::SpanHandle,
}

#[pymethods]
impl TraceSpan {
    #[new]
    fn new(name: String) -> Self {
        Self {
            handle: imp::SpanHandle::new(&name),
        }
    }

    fn __enter__(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.handle.enter();
        slf
    }

    #[pyo3(signature = (exc_type=None, exc_value=None, traceback=None))]
    fn __exit__(
        &mut self,
        exc_type: Option<Bound<'_, PyAny>>,
        exc_value: Option<Bound<'_, PyAny>>,
        traceback: Option<Bound<'_, PyAny>>,
    ) -> bool {
        let _ = (exc_type, traceback);
        self.handle.exit(exc_value);
        false
    }
}

#[pyfunction]
pub fn flush_traces() -> PyResult<()> {
    imp::flush().map_err(pyo3::exceptions::PyRuntimeError::new_err)
}
