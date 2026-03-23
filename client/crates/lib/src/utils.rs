use client_core::{ApiError, ComputeContextSpecs, RUNTIME};
use polars_axum_models::DBCPUArchitectureModel;
use pyo3::{Python, pyfunction};
use uuid::Uuid;

use crate::entry::EnterRustExt;

#[pyfunction]
#[pyo3(signature=(workspace_id, cpus=None, memory=None, cpu_architectures=None, instance_type=None, storage=None, big_instance_type=None, big_instance_multiplier=None, big_instance_storage=None, cluster_size=None))]
#[expect(clippy::too_many_arguments)]
pub fn resolve_compute_context_specs(
    workspace_id: Uuid,
    cpus: Option<u32>,
    memory: Option<u32>,
    cpu_architectures: Option<Vec<DBCPUArchitectureModel>>,
    instance_type: Option<String>,
    storage: Option<u32>,
    big_instance_type: Option<String>,
    big_instance_multiplier: Option<u32>,
    big_instance_storage: Option<u32>,
    cluster_size: Option<u32>,
    py: Python,
) -> Result<ComputeContextSpecs, ApiError> {
    py.enter_rust(|| {
        RUNTIME.block_on(client_core::resolve_compute_context_specs(
            workspace_id,
            cpus,
            memory,
            cpu_architectures,
            instance_type,
            storage,
            big_instance_type,
            big_instance_multiplier,
            big_instance_storage,
            cluster_size,
        ))?
    })
}
