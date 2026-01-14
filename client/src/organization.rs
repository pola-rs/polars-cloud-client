#![allow(clippy::result_large_err)]

use polars_axum_models::{
    OrganizationCreateSchema, OrganizationQuery, OrganizationSchema, Pagination,
};
use polars_backend_client::client::ApiClient;
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::client::{CLIENT_GLOBAL, WrappedAPIClient};
use crate::entry::EnterRustExt;
use crate::error::ApiError;

#[pymethods]
impl WrappedAPIClient {
    pub fn get_organization(
        &self,
        py: Python,
        organization_id: Uuid,
    ) -> Result<OrganizationSchema, ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call(|client: &ApiClient| client.get_organization(organization_id))
        })
    }

    pub fn create_organization(
        &self,
        py: Python,
        name: String,
    ) -> Result<OrganizationSchema, ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call(move |client: &ApiClient| {
                let schema = OrganizationCreateSchema { name };
                client.create_organization(schema)
            })
        })
    }

    pub fn delete_organization(&self, py: Python, organization_id: Uuid) -> Result<(), ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL
                .call(move |client: &ApiClient| client.delete_organization(organization_id))
        })
    }

    pub fn get_organizations(
        &self,
        py: Python,
        name: Option<String>,
    ) -> Result<Vec<OrganizationSchema>, ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call_paginated(|client: &ApiClient, page: i64| {
                // TODO: offset is overridden later by (page - 1) * limit, confusing
                let pagination = Pagination {
                    page,
                    limit: 1000,
                    offset: 0,
                };
                let query = OrganizationQuery { name: name.clone() };
                client.get_organizations(pagination, query)
            })
        })
    }
}
