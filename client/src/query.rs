#![allow(clippy::result_large_err)]

use polars_axum_models::{
    Pagination, QueryParamsFilter, QueryWithStateTimingAndResultSchema, QueryWithStateTimingSchema,
};
use polars_backend_client::client::ApiClient;
use pyo3::prelude::*;
use uuid::Uuid;

use crate::client::{CLIENT_GLOBAL, WrappedAPIClient};
use crate::entry::EnterRustExt;
use crate::error::ApiError;

#[pymethods]
impl WrappedAPIClient {
    #[pyo3(signature=(workspace_id, query_id))]
    pub fn get_query(
        &self,
        py: Python,
        workspace_id: Uuid,
        query_id: Uuid,
    ) -> Result<QueryWithStateTimingAndResultSchema, ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call(|client: &ApiClient| client.get_query(workspace_id, query_id))
        })
    }

    #[pyo3(signature=(workspace_id, query_id))]
    pub fn cancel_proxy_query(
        &self,
        py: Python,
        workspace_id: Uuid,
        query_id: Uuid,
    ) -> Result<(), ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call(|client: &ApiClient| client.cancel_query(workspace_id, query_id))
        })
    }

    #[pyo3(signature=(workspace_id))]
    pub fn get_queries(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<Vec<QueryWithStateTimingSchema>, ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call_paginated(|client: &ApiClient, page: i64| {
                // TODO: offset is overridden later by (page - 1) * limit, confusing
                let pagination = Pagination {
                    page,
                    limit: 1000,
                    offset: 0,
                };
                let params = QueryParamsFilter::default();
                client.get_queries(workspace_id, params, pagination)
            })
        })
    }
}
