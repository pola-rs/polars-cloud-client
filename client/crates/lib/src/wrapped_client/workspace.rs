#![allow(clippy::result_large_err)]

use client_core::{ApiError, CTRL_PLN_CLIENT_GLOBAL};
use polars_axum_models::{
    DBCPUArchitectureModel, InstanceSpecsModel, Pagination, WorkspaceClusterDefaultsModel,
    WorkspaceModel, WorkspaceQueryArgs,
};
use polars_backend_client::client::ApiClient;
use pyo3::exceptions::PyRuntimeError;
use pyo3::{Python, pyclass, pymethods};
use uuid::Uuid;

use crate::entry::EnterRustExt;
use crate::wrapped_client::WrappedAPIClient;

#[pyclass(from_py_object, get_all)]
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
    ) -> Result<WorkspaceModel, ApiError> {
        py.enter_rust(|| {
            CTRL_PLN_CLIENT_GLOBAL.call(|client: &ApiClient| client.get_workspace(workspace_id))
        })
    }

    #[pyo3(signature=(workspace_id))]
    pub fn get_workspace_cluster_defaults(
        &self,
        py: Python,
        workspace_id: Uuid,
    ) -> Result<Option<WorkspaceClusterDefaultsModel>, ApiError> {
        py.enter_rust(|| {
            CTRL_PLN_CLIENT_GLOBAL
                .call(|client: &ApiClient| client.get_cluster_defaults(workspace_id))
        })
    }

    #[pyo3(signature=(workspace_id, instance_type, cpus, ram_gb, storage, cpu_architectures, cluster_size)
    )]
    #[expect(clippy::too_many_arguments)]
    pub fn set_workspace_cluster_defaults(
        &self,
        py: Python,
        workspace_id: Uuid,
        instance_type: Option<String>,
        cpus: Option<u32>,
        ram_gb: Option<u32>,
        storage: Option<i32>,
        cpu_architectures: Option<Vec<DBCPUArchitectureModel>>,
        cluster_size: i32,
    ) -> Result<(), ApiError> {
        let params = WorkspaceClusterDefaultsModel {
            instance_specs: if let Some(instance_type) = instance_type {
                InstanceSpecsModel::InstanceType {
                    standard: instance_type,
                    big: None,
                }
            } else if let Some(cpus) = cpus
                && let Some(ram_gb) = ram_gb
            {
                InstanceSpecsModel::Specs {
                    cpus,
                    ram_gb,
                    cpu_architectures: cpu_architectures
                        .unwrap_or(vec![DBCPUArchitectureModel::X86_64]),
                    multiplier: None,
                }
            } else {
                return Err(PyRuntimeError::new_err(
                    "Must specify either instance_type or cpus and ram_gb",
                )
                .into());
            },
            storage,
            cluster_size,
        };

        py.enter_rust(|| {
            CTRL_PLN_CLIENT_GLOBAL
                .call(|client: &ApiClient| client.set_cluster_defaults(workspace_id, &params))
        })
    }

    #[pyo3(signature=(name=None, organization_id=None))]
    pub fn get_workspaces(
        &self,
        py: Python,
        name: Option<String>,
        organization_id: Option<Uuid>,
    ) -> Result<Vec<WorkspaceModel>, ApiError> {
        py.enter_rust(|| {
            CTRL_PLN_CLIENT_GLOBAL.call_paginated(|client: &ApiClient, page: i64| {
                // TODO: offset is overridden later by (page - 1) * limit, confusing
                let pagination = Pagination {
                    page,
                    limit: 1000,
                    offset: 0,
                };
                let query = WorkspaceQueryArgs {
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
            InstanceSpecsModel::InstanceType { standard, .. } => {
                specs.instance_type = Some(standard)
            },
            InstanceSpecsModel::Specs { cpus, ram_gb, .. } => {
                specs.cpus = Some(cpus);
                specs.ram_gb = Some(ram_gb);
            },
        }
        Ok(Some(specs))
    }
}
