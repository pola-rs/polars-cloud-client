#![allow(clippy::result_large_err)]

use polars_axum_models::{
    ClusterModeSchema, ComputeClusterPublicInfoSchema, ComputeSchema, ComputeStatusSchema,
    ComputeTokenSchema, DBClusterModeSchema, GetClusterFilterParams, InstanceSpecsSchema,
    LogLevelSchema, ManifestQuery, ManifestSchema, Pagination, PythonVersion,
    RegisterComputeClusterArgs, StartComputeClusterArgs, StartComputeClusterManifestArgs,
};
use polars_backend_client::client::ApiClient;
use pyo3::exceptions::PyValueError;
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::VERSIONS;
use crate::client::WrappedAPIClient;
use crate::error::ApiError;

#[pymethods]
impl WrappedAPIClient {
    pub fn get_compute_cluster_manifest(
        &mut self,
        py: Python,
        workspace_id: Uuid,
        manifest_name: String,
    ) -> Result<ManifestSchema, ApiError> {
        self.call(py, |client: &ApiClient| {
            client.find_compute_cluster_manifest(
                workspace_id,
                ManifestQuery {
                    name: manifest_name,
                },
            )
        })
    }

    pub fn get_compute_cluster(
        &mut self,
        py: Python,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<ComputeSchema, ApiError> {
        self.call(py, |client: &ApiClient| {
            client.get_compute_cluster(workspace_id, compute_id)
        })
    }

    pub fn stop_compute_cluster(
        &mut self,
        py: Python,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<(), ApiError> {
        self.call(py, |client: &ApiClient| {
            client.stop_compute_cluster(workspace_id, compute_id)
        })
    }

    pub fn get_compute_server_info(
        &mut self,
        py: Python,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<ComputeClusterPublicInfoSchema, ApiError> {
        self.call(py, |client: &ApiClient| {
            client.get_public_server_info(workspace_id, compute_id)
        })
    }

    pub fn get_compute_cluster_token(
        &mut self,
        py: Python,
        workspace_id: Uuid,
        compute_id: Uuid,
    ) -> Result<ComputeTokenSchema, ApiError> {
        self.call(py, |client: &ApiClient| {
            client.get_compute_cluster_token(workspace_id, compute_id)
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature=(workspace_id, name, cluster_size, mode, cpus, ram_gb, instance_type, storage, big_instance_type, big_instance_multiplier,  big_instance_storage, requirements_txt, labels, log_level, idle_timeout_mins))]
    pub fn register_compute_cluster_manifest(
        &mut self,
        py: Python<'_>,
        workspace_id: Uuid,
        name: String,
        cluster_size: u32,
        mode: DBClusterModeSchema,
        cpus: Option<u32>,
        ram_gb: Option<u32>,
        instance_type: Option<String>,
        storage: Option<u32>,
        big_instance_type: Option<String>,
        big_instance_multiplier: Option<u32>,
        big_instance_storage: Option<u32>,
        requirements_txt: Option<String>,
        labels: Option<Vec<String>>,
        log_level: LogLevelSchema,
        idle_timeout_mins: Option<u32>,
    ) -> Result<ManifestSchema, ApiError> {
        let mode = if mode == DBClusterModeSchema::Direct {
            ClusterModeSchema::Direct {
                client_public_key: "".to_string(),
            }
        } else {
            ClusterModeSchema::Proxy
        };

        if (big_instance_type.is_some() || big_instance_multiplier.is_some()) && cluster_size <= 1 {
            Err(PyValueError::new_err(
                "Invalid specification big instance set while cluster size is equal to 1.",
            ))?;
        }

        let instance = match (instance_type, cpus, ram_gb) {
            (Some(instance_type), None, None) => InstanceSpecsSchema::InstanceType {
                standard: instance_type,
                big: big_instance_type,
            },
            (None, Some(cpus), Some(ram_gb)) => InstanceSpecsSchema::Specs {
                cpus,
                ram_gb,
                multiplier: big_instance_multiplier,
            },
            _ => Err(PyValueError::new_err(
                "Invalid parameters: either (cpu & memory) or instance type must be specified.",
            ))?,
        };

        let python_version = Python::attach(|py| {
            let version = py.version_info();
            PythonVersion {
                major: version.major,
                minor: version.minor,
                patch: version.patch,
            }
        });
        let polars_version = VERSIONS.get().unwrap().as_ref().unwrap().0.polars;
        let params = RegisterComputeClusterArgs {
            name,
            instance,
            storage,
            big_instance_storage,
            cluster_size,
            mode,
            labels,
            log_level,
            requirements_txt,
            python_version,
            polars_version,
            idle_timeout_mins,
        };

        self.call(py, |client: &ApiClient| {
            client.register_compute_cluster_manifest(workspace_id, params)
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature=(workspace_id, name))]
    pub fn start_compute_cluster_manifest(
        &mut self,
        py: Python<'_>,
        workspace_id: Uuid,
        name: String,
    ) -> Result<ComputeSchema, ApiError> {
        let python_version = Python::attach(|py| {
            let version = py.version_info();
            PythonVersion {
                major: version.major,
                minor: version.minor,
                patch: version.patch,
            }
        });
        let polars_version = VERSIONS.get().unwrap().as_ref().unwrap().0.polars;
        let params = StartComputeClusterManifestArgs {
            name,
            python_version,
            polars_version,
        };

        self.call(py, |client: &ApiClient| {
            client.start_compute_cluster_manifest(workspace_id, params)
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature=(workspace_id, cluster_size, mode, cpus, ram_gb, instance_type, storage, big_instance_type, big_instance_multiplier,  big_instance_storage, requirements_txt, labels, log_level, idle_timeout_mins))]
    pub fn start_compute(
        &mut self,
        py: Python<'_>,
        workspace_id: Uuid,
        cluster_size: u32,
        mode: DBClusterModeSchema,
        cpus: Option<u32>,
        ram_gb: Option<u32>,
        instance_type: Option<String>,
        storage: Option<u32>,
        big_instance_type: Option<String>,
        big_instance_multiplier: Option<u32>,
        big_instance_storage: Option<u32>,
        requirements_txt: Option<String>,
        labels: Option<Vec<String>>,
        log_level: Option<LogLevelSchema>,
        idle_timeout_mins: Option<u32>,
    ) -> Result<ComputeSchema, ApiError> {
        let mode = if mode == DBClusterModeSchema::Direct {
            ClusterModeSchema::Direct {
                client_public_key: "".to_string(),
            }
        } else {
            ClusterModeSchema::Proxy
        };

        if (big_instance_type.is_some() || big_instance_multiplier.is_some()) && cluster_size <= 1 {
            Err(PyValueError::new_err(
                "Invalid specification big instance set while cluster size is equal to 1.",
            ))?;
        }

        let instance = match (instance_type, cpus, ram_gb) {
            (Some(instance_type), None, None) => InstanceSpecsSchema::InstanceType {
                standard: instance_type,
                big: big_instance_type,
            },
            (None, Some(cpus), Some(ram_gb)) => InstanceSpecsSchema::Specs {
                cpus,
                ram_gb,
                multiplier: big_instance_multiplier,
            },
            _ => Err(PyValueError::new_err(
                "Invalid parameters: either (cpu & memory) or instance type must be specified.",
            ))?,
        };

        let python_version = Python::attach(|py| {
            let version = py.version_info();
            PythonVersion {
                major: version.major,
                minor: version.minor,
                patch: version.patch,
            }
        });
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
            python_version,
            polars_version,
            idle_timeout_mins,
        };

        self.call(py, |client: &ApiClient| {
            client.start_compute_cluster(workspace_id, params)
        })
    }

    #[pyo3(signature=(workspace_id, *, status=None))]
    pub fn get_compute_clusters(
        &mut self,
        py: Python,
        workspace_id: Uuid,
        status: Option<Vec<ComputeStatusSchema>>,
    ) -> Result<Vec<ComputeSchema>, ApiError> {
        self.call_paginated(py, |client: &ApiClient, page: i64| {
            // TODO: offset is overridden later by (page - 1) * limit, confusing
            let pagination = Pagination {
                page,
                limit: 1000,
                offset: 0,
            };
            client.get_compute_clusters(
                workspace_id,
                GetClusterFilterParams {
                    status: status.clone(),
                },
                pagination,
            )
        })
    }
}
