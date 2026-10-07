use polars_backend_client::client::user_agent;
use protos_client_control::{ClientServiceClient, MAX_MESSAGE_LENGTH_CONTROL_PLANE, tonic};
use protos_common::tonic::Request;
use protos_common::tonic::service::interceptor::InterceptedService;
use protos_common::tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tower::Layer;
use tower_otel::{OtelLayer, OtelService};

use crate::VERSIONS;
use crate::constants::SERVICE_NAME;

pub type ControlPlaneGRPCClient = ClientServiceClient<
    InterceptedService<OtelService<Channel>, fn(Request<()>) -> tonic::Result<Request<()>>>,
>;

/// `api_addr` must come from [`crate::PolarsCloudConfig::resolve_api_addr`], which only returns
/// addresses that parse as an endpoint.
pub fn get_control_plane_client(api_addr: &str) -> ControlPlaneGRPCClient {
    let endpoint: Endpoint = api_addr
        .parse()
        .expect("resolve_api_addr only returns addresses that parse as an endpoint");

    let channel = endpoint
        .user_agent(user_agent(
            VERSIONS
                .get()
                .unwrap()
                .as_ref()
                .map(|(_, versions)| versions),
        ))
        .unwrap_or_else(|e| panic!("invalid Polars Cloud API address {api_addr:?} ({e})"))
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .unwrap_or_else(|e| panic!("could not configure TLS for {api_addr:?} ({e})"))
        .connect_lazy();

    ClientServiceClient::with_interceptor(
        OtelLayer::client(SERVICE_NAME).layer(channel),
        version_interceptor as _,
    )
    .max_encoding_message_size(MAX_MESSAGE_LENGTH_CONTROL_PLANE)
    .max_decoding_message_size(MAX_MESSAGE_LENGTH_CONTROL_PLANE)
}

#[allow(clippy::result_large_err)]
fn version_interceptor(
    mut request: Request<()>,
) -> std::result::Result<Request<()>, tonic::Status> {
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
