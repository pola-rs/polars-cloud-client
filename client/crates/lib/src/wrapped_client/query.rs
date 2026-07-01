#![allow(clippy::result_large_err)]

use client_core::{ApiError, RUNTIME};
use polars_axum_models::{GetQueryArgs, QueryModel, QueryWithStateTimingAndResultModel};
use pyo3::prelude::*;
use uuid::Uuid;

use crate::entry::EnterRustExt;
use crate::wrapped_client::WrappedAPIClient;

#[pymethods]
impl WrappedAPIClient {
    #[pyo3(signature=(workspace_id, query_id))]
    pub fn get_query(
        &self,
        py: Python,
        workspace_id: Uuid,
        query_id: Uuid,
    ) -> Result<QueryWithStateTimingAndResultModel, ApiError> {
        py.enter_rust(|| RUNTIME.block_on(self.client.get_query(workspace_id, query_id))?)
    }

    #[pyo3(signature=(workspace_id, query_id))]
    pub fn cancel_proxy_query(
        &self,
        py: Python,
        workspace_id: Uuid,
        query_id: Uuid,
    ) -> Result<(), ApiError> {
        py.enter_rust(|| RUNTIME.block_on(self.client.cancel_query(workspace_id, query_id))?)
    }

    #[pyo3(signature=(workspace_id))]
    pub fn get_queries(&self, py: Python, workspace_id: Uuid) -> Result<Vec<QueryModel>, ApiError> {
        py.enter_rust(|| {
            RUNTIME.block_on(
                self.client
                    .get_queries(workspace_id, GetQueryArgs::default()),
            )?
        })
    }
}
