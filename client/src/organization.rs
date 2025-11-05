#![allow(clippy::result_large_err)]

use polars_axum_models::{
    OrganizationCreateSchema, OrganizationQuery, OrganizationSchema, Pagination,
};
use polars_backend_client::client::ApiClient;
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::client::WrappedAPIClient;
use crate::error::ApiError;

#[pymethods]
impl WrappedAPIClient {
    pub fn get_organization(
        &mut self,
        py: Python<'_>,
        organization_id: Uuid,
    ) -> Result<OrganizationSchema, ApiError> {
        self.call(py, |client: &ApiClient| {
            client.get_organization(organization_id)
        })
    }

    pub fn create_organization(
        &mut self,
        py: Python<'_>,
        name: String,
    ) -> Result<OrganizationSchema, ApiError> {
        self.call(py, move |client: &ApiClient| {
            let schema = OrganizationCreateSchema { name };
            client.create_organization(schema)
        })
    }

    pub fn delete_organization(
        &mut self,
        py: Python<'_>,
        organization_id: Uuid,
    ) -> Result<(), ApiError> {
        self.call(py, move |client: &ApiClient| {
            client.delete_organization(organization_id)
        })
    }

    pub fn get_organizations(
        &mut self,
        py: Python<'_>,
        name: Option<String>,
    ) -> Result<Vec<OrganizationSchema>, ApiError> {
        self.call_paginated(py, |client: &ApiClient, page: i64| {
            // TODO: offset is overridden later by (page - 1) * limit, confusing
            let pagination = Pagination {
                page,
                limit: 1000,
                offset: 0,
            };
            let query = OrganizationQuery { name: name.clone() };
            client.get_organizations(pagination, query)
        })
    }
}
