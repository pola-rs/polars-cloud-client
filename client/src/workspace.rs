#![allow(clippy::result_large_err)]

use polars_axum_models::{
    InstanceSpecsSchema, Pagination, WorkspaceClusterDefaultsSchema, WorkspaceQuery,
    WorkspaceSchema,
};
use polars_backend_client::client::ApiClient;
use pyo3::{Python, pyclass, pymethods};
use uuid::Uuid;

use crate::client::{CLIENT_GLOBAL, WrappedAPIClient};
use crate::entry::EnterRustExt;
use crate::error::ApiError;

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct DefaultComputeSpecs {
    instance_type: Option<String>,
    cpus: Option<u32>,
    ram_gb: Option<u32>,
    storage: Option<i32>,
    cluster_size: i32,
}

#[pymethods]
impl WrappedAPIClient {
    #[pyo3(signature=(workspace_id))]
    pub fn get_workspace(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<WorkspaceSchema, ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call(|client: &ApiClient| client.get_workspace(workspace_id))
        })
    }

    #[pyo3(signature=(workspace_id))]
    pub fn get_workspace_cluster_defaults(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<Option<WorkspaceClusterDefaultsSchema>, ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call(|client: &ApiClient| client.get_cluster_defaults(workspace_id))
        })
    }

    #[pyo3(signature=(name=None, organization_id=None))]
    pub fn get_workspaces(
        &self,
        py: Python,
        name: Option<String>,
        organization_id: Option<Uuid>,
    ) -> Result<Vec<WorkspaceSchema>, ApiError> {
        py.enter_rust(|| {
            CLIENT_GLOBAL.call_paginated(|client: &ApiClient, page: i64| {
                // TODO: offset is overridden later by (page - 1) * limit, confusing
                let pagination = Pagination {
                    page,
                    limit: 1000,
                    offset: 0,
                };
                let query = WorkspaceQuery {
                    name: name.clone(),
                    organization_id,
                };
                client.get_workspaces(query, pagination)
            })
        })
    }

    #[pyo3(signature=(workspace_id))]
    pub fn get_workspace_default_compute_specs(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<Option<DefaultComputeSpecs>, ApiError> {
        let defaults = self.get_workspace_cluster_defaults(py, workspace_id)?;
        let Some(defaults) = defaults else {
            return Ok(None);
        };

        let mut specs = DefaultComputeSpecs {
            instance_type: None,
            cpus: None,
            ram_gb: None,
            storage: defaults.storage,
            cluster_size: defaults.cluster_size,
        };

        match defaults.instance_specs {
            InstanceSpecsSchema::InstanceType { standard, .. } => {
                specs.instance_type = Some(standard)
            },
            InstanceSpecsSchema::Specs { cpus, ram_gb, .. } => {
                specs.cpus = Some(cpus);
                specs.ram_gb = Some(ram_gb);
            },
        }
        Ok(Some(specs))
    }
}
