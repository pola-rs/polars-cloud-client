#![allow(clippy::result_large_err)]

use client_core::{ApiError, RUNTIME};
use polars_axum_models::{OrganizationCreateArgs, OrganizationModel};
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
        py.enter_rust(|| RUNTIME.block_on(self.client.get_organization(organization_id))?)
    }

    pub fn create_organization(
        &self,
        py: Python,
        name: String,
    ) -> Result<OrganizationModel, ApiError> {
        py.enter_rust(|| {
            RUNTIME.block_on(
                self.client
                    .create_organization(OrganizationCreateArgs { name }),
            )?
        })
    }

    pub fn delete_organization(&self, py: Python, organization_id: Uuid) -> Result<(), ApiError> {
        py.enter_rust(|| RUNTIME.block_on(self.client.delete_organization(organization_id))?)
    }

    pub fn get_organizations(
        &self,
        py: Python,
        name: Option<String>,
    ) -> Result<Vec<OrganizationModel>, ApiError> {
        py.enter_rust(|| RUNTIME.block_on(self.client.get_organizations(name))?)
    }
}
