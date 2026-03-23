#![allow(clippy::result_large_err)]

use client_core::{ApiError, CTRL_PLN_CLIENT_GLOBAL};
use polars_axum_models::{
    OrganizationCreateArgs, OrganizationModel, OrganizationQueryArgs, Pagination,
};
use polars_backend_client::client::ApiClient;
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::entry::EnterRustExt;
use crate::wrapped_client::WrappedAPIClient;

#[pymethods]
impl WrappedAPIClient {
    pub fn get_organization(
        &self,
        py: Python,
        organization_id: Uuid,
    ) -> Result<OrganizationModel, ApiError> {
        py.enter_rust(|| {
            CTRL_PLN_CLIENT_GLOBAL
                .call(|client: &ApiClient| client.get_organization(organization_id))
        })
    }

    pub fn create_organization(
        &self,
        py: Python,
        name: String,
    ) -> Result<OrganizationModel, ApiError> {
        py.enter_rust(|| {
            CTRL_PLN_CLIENT_GLOBAL.call(move |client: &ApiClient| {
                client.create_organization(OrganizationCreateArgs { name })
            })
        })
    }

    pub fn delete_organization(&self, py: Python, organization_id: Uuid) -> Result<(), ApiError> {
        py.enter_rust(|| {
            CTRL_PLN_CLIENT_GLOBAL
                .call(move |client: &ApiClient| client.delete_organization(organization_id))
        })
    }

    pub fn get_organizations(
        &self,
        py: Python,
        name: Option<String>,
    ) -> Result<Vec<OrganizationModel>, ApiError> {
        py.enter_rust(|| {
            CTRL_PLN_CLIENT_GLOBAL.call_paginated(|client: &ApiClient, page: i64| {
                // TODO: offset is overridden later by (page - 1) * limit, confusing
                let pagination = Pagination {
                    page,
                    limit: 1000,
                    offset: 0,
                };
                let query = OrganizationQueryArgs { name: name.clone() };
                client.get_organizations(pagination, query)
            })
        })
    }
}
