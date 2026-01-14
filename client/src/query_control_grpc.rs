#![allow(clippy::result_large_err)]

use protos_client_control::{
    ClientServiceClient, MAX_MESSAGE_LENGTH_CONTROL_PLANE, SubmitQueryRequestProto, client,
};
use protos_common::prost::Message;
use protos_common::tonic::Request;
use protos_common::{QueryIdentifier, QueryInfo};
use pyo3::exceptions::PyValueError;
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::client::{CLIENT_GLOBAL, ControlPlaneGRPCClient, WrappedAPIClient};
use crate::entry::EnterRustExt;
use crate::error::ApiError;
use crate::query_settings::PyQuerySettings;
use crate::serde_types::{QueryInfoPy, query_result_to_py};

#[pymethods]
impl WrappedAPIClient {
    pub fn get_query_result(&self, py: Python, query_id: Uuid) -> Result<QueryInfoPy, ApiError> {
        py.enter_rust(|| {
            let query_id = QueryIdentifier::from(query_id);
            let req = Request::new(query_id.into());
            CLIENT_GLOBAL.call_grpc(
                |mut client: ControlPlaneGRPCClient, request: Request<_>| async move {
                    client
                        .get_query_result(request)
                        .await
                        .map(|res| res.into_inner())
                },
                req,
            )
        })
        .map(|query_info| query_result_to_py(py, query_info.into(), None))
    }

    pub fn submit_query(
        &self,
        py: Python,
        compute_id: Uuid,
        plan: Vec<u8>,
        settings: PyQuerySettings,
        labels: Option<Vec<String>>,
    ) -> Result<Uuid, ApiError> {
        py.enter_rust(|| {
            let proto: SubmitQueryRequestProto = client::SubmitQueryRequest {
                compute_id: compute_id.into(),
                settings: settings.into(),
                plan: plan.into(),
                query_info: QueryInfo {
                    labels: labels.unwrap_or_default(),
                },
            }
            .into();
            if proto.encoded_len() > MAX_MESSAGE_LENGTH_CONTROL_PLANE {
                return Err(ApiError::PyErr(PyValueError::new_err(format!(
                    "Query plan exceeds limit of {} MiB",
                    MAX_MESSAGE_LENGTH_CONTROL_PLANE / 1024 / 1024
                ))));
            }
            let req = Request::new(proto);
            CLIENT_GLOBAL.call_grpc(
                |mut client: ClientServiceClient<_>, request: Request<_>| async move {
                    client
                        .submit_query(request)
                        .await
                        .map(|res| QueryIdentifier::from(res.into_inner()).inner)
                },
                req,
            )
        })
    }
}
