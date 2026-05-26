#![allow(clippy::result_large_err)]

use std::collections::HashMap;

use client_core::{ApiError, RUNTIME, VERSIONS};
use polars_axum_models::{
    ComputeClusterNodeInfoModel, ComputeClusterPublicInfoModel, ComputeModel, ComputeStatusModel,
    ComputeTokenModel, DBCPUArchitectureModel, DBClusterModeModel, GetClusterFilterArgs,
    InstanceSpecsModel, LogLevelModel, ManifestModel, ManifestQueryArgs,
    RegisterComputeClusterManifestArgs, StartComputeClusterArgs, StartComputeClusterManifestArgs,
};
use pyo3::exceptions::PyValueError;
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::entry::EnterRustExt;
use crate::wrapped_client::WrappedAPIClient;

#[pymethods]
impl WrappedAPIClient {
    pub fn get_compute_cluster_manifest(
        &self,
        py: Python,
        workspace_id: Uuid,
        manifest_name: String,
    ) -> Result<ManifestModel, ApiError> {
        py.enter_rust(|| {
            RUNTIME.block_on(self.client.find_compute_cluster_manifest(
                workspace_id,
                ManifestQueryArgs {
                    name: manifest_name,
                },
            ))?
        })
    }

    pub fn get_compute_cluster(
        &self,
        py: Python,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<ComputeModel, ApiError> {
        py.enter_rust(|| {
            RUNTIME.block_on(self.client.get_compute_cluster(workspace_id, compute_id))?
        })
    }

    pub fn stop_compute_cluster(
        &self,
        py: Python,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<(), ApiError> {
        py.enter_rust(|| {
            RUNTIME.block_on(self.client.stop_compute_cluster(workspace_id, compute_id))?
        })
    }

    pub fn get_compute_server_info(
        &self,
        py: Python,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<ComputeClusterPublicInfoModel, ApiError> {
        py.enter_rust(|| {
            RUNTIME.block_on(self.client.get_public_server_info(workspace_id, compute_id))?
        })
    }

    pub fn get_compute_cluster_token(
        &self,
        py: Python,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<ComputeTokenModel, ApiError> {
        py.enter_rust(|| {
            RUNTIME.block_on(
                self.client
                    .get_compute_cluster_token(workspace_id, compute_id),
            )?
        })
    }
    pub fn get_compute_cluster_nodes(
        &self,
        py: Python,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<Vec<ComputeClusterNodeInfoModel>, ApiError> {
        py.enter_rust(|| {
            RUNTIME.block_on(
                self.client
                    .get_compute_cluster_nodes(workspace_id, compute_id),
            )?
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature=(workspace_id, name, cluster_size, mode, cpus, ram_gb, cpu_architectures, instance_type, storage, big_instance_type, big_instance_multiplier,  big_instance_storage, requirements_txt, env_vars, labels, log_level, idle_timeout_mins))]
    pub fn register_compute_cluster_manifest(
        &self,
        py: Python,
        workspace_id: Uuid,
        name: String,
        cluster_size: u32,
        mode: DBClusterModeModel,
        cpus: Option<u32>,
        ram_gb: Option<u32>,
        cpu_architectures: Option<Vec<DBCPUArchitectureModel>>,
        instance_type: Option<String>,
        storage: Option<u32>,
        big_instance_type: Option<String>,
        big_instance_multiplier: Option<u32>,
        big_instance_storage: Option<u32>,
        requirements_txt: Option<String>,
        env_vars: HashMap<String, String>,
        labels: Option<Vec<String>>,
        log_level: LogLevelModel,
        idle_timeout_mins: Option<u32>,
    ) -> Result<ManifestModel, ApiError> {
        py.enter_rust(|| {
            if (big_instance_type.is_some() || big_instance_multiplier.is_some())
                && cluster_size <= 1
            {
                Err(PyValueError::new_err(
                    "Invalid specification big instance set while cluster size is equal to 1.",
                ))?;
            }

            let instance = match (instance_type, cpus, ram_gb, cpu_architectures) {
                (Some(instance_type), None, None, None) => InstanceSpecsModel::InstanceType {
                    standard: instance_type,
                    big: big_instance_type,
                },
                (None, Some(cpus), Some(ram_gb), Some(cpu_architectures)) => InstanceSpecsModel::Specs {
                    cpus,
                    ram_gb,
                    multiplier: big_instance_multiplier,
                    cpu_architectures,
                },
                _ => Err(PyValueError::new_err(
                    "Invalid parameters: either all three (cpu, memory, and cpu_architectures) parameters or just the instance_type parameter must be specified.",
                ))?,
            };

            let python_version = VERSIONS.get().unwrap().as_ref().unwrap().0.python;
            let polars_version = VERSIONS.get().unwrap().as_ref().unwrap().0.polars;
            let params = RegisterComputeClusterManifestArgs {
                name,
                instance,
                storage,
                big_instance_storage,
                cluster_size,
                mode,
                labels,
                log_level,
                requirements_txt,
                env_vars,
                python_version,
                polars_version,
                idle_timeout_mins,
                settings: None
            };

            RUNTIME.block_on(
                self.client
                    .register_compute_cluster_manifest(workspace_id, params),
            )?
        })
    }

    #[pyo3(signature=(workspace_id, name))]
    pub fn unregister_compute_cluster_manifest(
        &self,
        py: Python,
        workspace_id: Uuid,
        name: String,
    ) -> Result<(), ApiError> {
        py.enter_rust(|| {
            RUNTIME.block_on(
                self.client
                    .unregister_compute_cluster_manifest(workspace_id, name),
            )?
        })
    }

    #[pyo3(signature=(workspace_id, name))]
    pub fn start_compute_cluster_manifest(
        &self,
        py: Python<'_>,
        workspace_id: Uuid,
        name: String,
    ) -> Result<ComputeModel, ApiError> {
        py.enter_rust(|| {
            let python_version = VERSIONS.get().unwrap().as_ref().unwrap().0.python;
            let polars_version = VERSIONS.get().unwrap().as_ref().unwrap().0.polars;
            let params = StartComputeClusterManifestArgs {
                name,
                python_version,
                polars_version,
            };

            RUNTIME.block_on(
                self.client
                    .start_compute_cluster_manifest(workspace_id, params),
            )?
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature=(workspace_id, cluster_size, mode, cpus, ram_gb, cpu_architectures, instance_type, storage, big_instance_type, big_instance_multiplier,  big_instance_storage, requirements_txt, env_vars, labels, log_level, idle_timeout_mins))]
    pub fn start_compute(
        &self,
        py: Python<'_>,
        workspace_id: Uuid,
        cluster_size: u32,
        mode: DBClusterModeModel,
        cpus: Option<u32>,
        ram_gb: Option<u32>,
        cpu_architectures: Option<Vec<DBCPUArchitectureModel>>,
        instance_type: Option<String>,
        storage: Option<u32>,
        big_instance_type: Option<String>,
        big_instance_multiplier: Option<u32>,
        big_instance_storage: Option<u32>,
        requirements_txt: Option<String>,
        env_vars: HashMap<String, String>,
        labels: Option<Vec<String>>,
        log_level: Option<LogLevelModel>,
        idle_timeout_mins: Option<u32>,
    ) -> Result<ComputeModel, ApiError> {
        py.enter_rust(|| {
            if (big_instance_type.is_some() || big_instance_multiplier.is_some())
                && cluster_size <= 1
            {
                Err(PyValueError::new_err(
                    "Invalid specification big instance set while cluster size is equal to 1.",
                ))?;
            }

            let instance = match (instance_type, cpus, ram_gb, cpu_architectures) {
                (Some(instance_type), None, None, None) => InstanceSpecsModel::InstanceType {
                    standard: instance_type,
                    big: big_instance_type,
                },
                (None, Some(cpus), Some(ram_gb), Some(cpu_architectures)) => {
                    InstanceSpecsModel::Specs {
                        cpus,
                        ram_gb,
                        multiplier: big_instance_multiplier,
                        cpu_architectures,
                    }
                },
                _ => Err(PyValueError::new_err(
                    "Invalid parameters: either all three (cpu, memory, and cpu_architectures) parameters or just the instance_type parameter must be specified.",
                ))?,
            };

            let python_version = VERSIONS.get().unwrap().as_ref().unwrap().0.python;
            let polars_version = VERSIONS.get().unwrap().as_ref().unwrap().0.polars;
            let params = StartComputeClusterArgs {
                instance,
                storage,
                big_instance_storage,
                cluster_size,
                mode,
                labels,
                log_level,
                requirements_txt,
                env_vars,
                python_version,
                polars_version,
                idle_timeout_mins,
                settings: None,
            };

            RUNTIME.block_on(self.client.start_compute_cluster(workspace_id, params))?
        })
    }

    #[pyo3(signature=(workspace_id, *, status=None))]
    pub fn get_compute_clusters(
        &self,
        py: Python,
        workspace_id: Uuid,
        status: Option<Vec<ComputeStatusModel>>,
    ) -> Result<Vec<ComputeModel>, ApiError> {
        py.enter_rust(|| {
            RUNTIME.block_on(self.client.get_compute_clusters(
                workspace_id,
                GetClusterFilterArgs {
                    status,
                    current_user_only: false,
                },
            ))?
        })
    }
}
