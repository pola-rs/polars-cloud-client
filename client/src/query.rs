#![allow(clippy::result_large_err)]

use polars_axum_models::{
    Pagination, QueryParamsFilter, QueryWithStateTimingAndResultSchema, QueryWithStateTimingSchema,
};
use polars_backend_client::client::ApiClient;
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::client::WrappedAPIClient;
use crate::error::ApiError;

#[pymethods]
impl WrappedAPIClient {
    #[pyo3(signature=(workspace_id, query_id))]
    pub fn get_query(
        &mut self,
        py: Python,
        workspace_id: Uuid,
        query_id: Uuid,
    ) -> Result<QueryWithStateTimingAndResultSchema, ApiError> {
        self.call(py, |client: &ApiClient| {
            client.get_query(workspace_id, query_id)
        })
    }

    #[pyo3(signature=(workspace_id, query_id))]
    pub fn cancel_proxy_query(
        &mut self,
        py: Python,
        workspace_id: Uuid,
        query_id: Uuid,
    ) -> Result<(), ApiError> {
        self.call(py, |client: &ApiClient| {
            client.cancel_query(workspace_id, query_id)
        })
    }

    #[pyo3(signature=(workspace_id))]
    pub fn get_queries(
        &mut self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<Vec<QueryWithStateTimingSchema>, ApiError> {
        self.call_paginated(py, |client: &ApiClient, page: i64| {
            // TODO: offset is overridden later by (page - 1) * limit, confusing
            let pagination = Pagination {
                page,
                limit: 1000,
                offset: 0,
            };
            let params = QueryParamsFilter::default();
            client.get_queries(workspace_id, params, pagination)
        })
    }
}
