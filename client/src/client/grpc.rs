use polars_backend_client::client::user_agent;
use protos_client_control::{ClientServiceClient, MAX_MESSAGE_LENGTH_CONTROL_PLANE, tonic};
use protos_common::tonic::Request;
use protos_common::tonic::service::interceptor::InterceptedService;
use protos_common::tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::VERSIONS;
use crate::constants::{API_ADDR, RUNTIME};

pub(crate) type ControlPlaneGRPCClient =
    ClientServiceClient<InterceptedService<Channel, fn(Request<()>) -> tonic::Result<Request<()>>>>;

pub(crate) fn get_control_plane_client() -> ControlPlaneGRPCClient {
    let address = API_ADDR.to_string();
    let endpoint: Endpoint = format!("{address}:443").parse().unwrap();
    let channel = RUNTIME.0.block_on(async {
        endpoint
            .user_agent(user_agent(
                VERSIONS
                    .get()
                    .unwrap()
                    .as_ref()
                    .map(|(_, versions)| versions),
            ))
            .unwrap()
            .tls_config(ClientTlsConfig::new().with_enabled_roots())
            .unwrap()
            .connect_lazy()
    });
    ClientServiceClient::with_interceptor(channel, version_interceptor as _)
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
