use std::time::Duration;

use anyhow::anyhow;
use polars_axum_models::{
    ComputeStatusModel, DBCPUArchitectureModel, InstanceSpecsModel, WorkspaceClusterDefaultsModel,
};
use pyo3::{pyclass, pymethods};
use uuid::Uuid;

use crate::{ApiError, ApiResult, CTRL_PLN_CLIENT_GLOBAL};

async fn get_or_fetch_defaults(
    cached: &mut Option<Option<WorkspaceClusterDefaultsModel>>,
    workspace_id: Uuid,
) -> anyhow::Result<&Option<WorkspaceClusterDefaultsModel>> {
    if cached.is_none() {
        *cached = Some(
            CTRL_PLN_CLIENT_GLOBAL
                .call_async(|client| client.get_cluster_defaults(workspace_id))
                .await?,
        );
    }
    Ok(cached.as_ref().unwrap())
}

#[expect(clippy::too_many_arguments)]
pub async fn resolve_compute_context_specs(
    workspace_id: Uuid,
    mut cpus: Option<u32>,
    mut memory: Option<u32>,
    mut cpu_architectures: Option<Vec<DBCPUArchitectureModel>>,
    mut instance_type: Option<String>,
    storage: Option<u32>,
    big_instance_type: Option<String>,
    big_instance_multiplier: Option<u32>,
    big_instance_storage: Option<u32>,
    cluster_size: Option<u32>,
) -> ApiResult<ComputeContextSpecs> {
    if instance_type.is_some()
        && (cpus.is_some() || memory.is_some() || cpu_architectures.is_some())
    {
        return Err(ApiError::ComputeClusterMisspecified(
            "cannot specify both `instance_type` AND (`memory` or `cpus` or `cpu_architectures`)"
                .into(),
        ));
    }

    if (memory.is_some() && cpus.is_none()) || (memory.is_none() && cpus.is_some()) {
        return Err(ApiError::ComputeClusterMisspecified(
            "`cpus` and `memory` must be specified together".into(),
        ));
    }

    if big_instance_type.is_some() && big_instance_multiplier.is_some() {
        return Err(ApiError::ComputeClusterMisspecified(
            "cannot specify both `big_instance_type` AND `big_instance_multiplier`".into(),
        ));
    }

    if instance_type.is_some() && big_instance_multiplier.is_some() {
        return Err(ApiError::ComputeClusterMisspecified(
            "cannot specify both `instance_type` AND `big_instance_multiplier`".into(),
        ));
    }

    if (cpus.is_some() || memory.is_some()) && big_instance_type.is_some() {
        return Err(ApiError::ComputeClusterMisspecified(
            "cannot specify both (`memory` or `cpus`) AND `big_instance_type`".into(),
        ));
    }

    if instance_type.is_none() && cpu_architectures.as_ref().is_none_or(|v| v.is_empty()) {
        cpu_architectures = Some(vec![DBCPUArchitectureModel::X86_64]);
    }

    let mut defaults = None;

    if memory.is_none() && cpus.is_none() && instance_type.is_none() {
        if let Some(defaults) = get_or_fetch_defaults(&mut defaults, workspace_id).await? {
            match &defaults.instance_specs {
                InstanceSpecsModel::InstanceType { standard, big: _ } => {
                    instance_type = Some(standard.clone());
                },
                InstanceSpecsModel::Specs {
                    cpus: cpus_d,
                    ram_gb: ram_gb_d,
                    ..
                } => {
                    cpus = Some(*cpus_d);
                    memory = Some(*ram_gb_d);
                },
            }
        } else {
            return Err(ApiError::ComputeClusterMisspecified(
                "Compute specification not provided and no defaults available for workspace."
                    .into(),
            ));
        }
    }

    let storage_resolved = if let Some(storage) = storage {
        Some(storage)
    } else {
        get_or_fetch_defaults(&mut defaults, workspace_id)
            .await?
            .as_ref()
            .and_then(|d| d.storage.map(|x| x as u32))
    };

    let cluster_size_resolved = if let Some(cluster_size) = cluster_size {
        cluster_size
    } else {
        get_or_fetch_defaults(&mut defaults, workspace_id)
            .await?
            .as_ref()
            .map(|d| d.cluster_size as u32)
            .unwrap_or({
                if big_instance_type.is_some() || big_instance_multiplier.is_some() {
                    2
                } else {
                    1
                }
            })
    };

    Ok(ComputeContextSpecs {
        cpus,
        memory,
        cpu_architectures,
        instance_type,
        big_instance_type,
        big_instance_multiplier,
        storage: storage_resolved,
        big_instance_storage,
        cluster_size: cluster_size_resolved,
    })
}

pub async fn poll_compute_status_until(
    workspace_id: Uuid,
    cluster_id: Uuid,
    desired_state: ComputeStatusModel,
    start_delay: Duration,
    interval: Duration,
    timeout: Duration,
) -> ApiResult<ComputeStatusModel> {
    let status = CTRL_PLN_CLIENT_GLOBAL
        .call_async(|client| client.get_compute_cluster(workspace_id, cluster_id))
        .await?
        .status;

    if status == desired_state {
        return Ok(status);
    }

    let max_polls = (timeout.saturating_sub(start_delay)).as_secs() / interval.as_secs();

    tokio::time::sleep(start_delay).await;

    for i in 0..max_polls {
        tracing::debug!("Polling compute status (try {i})");

        let status = CTRL_PLN_CLIENT_GLOBAL
            .call_async(|client| client.get_compute_cluster(workspace_id, cluster_id))
            .await?
            .status;

        tracing::debug!("Got compute status {status:?}");

        if status == desired_state {
            return Ok(status);
        } else if status == ComputeStatusModel::Failed {
            let msg = "Compute cluster in failed state";
            tracing::info!(msg);
            return Err(anyhow!(msg).into());
        }

        tokio::time::sleep(interval).await;
    }

    let msg = format!(
        "Compute context failed to reach desired state {desired_state:?} after polling for {} seconds",
        timeout.as_secs()
    );
    tracing::info!(msg);
    Err(anyhow!(msg).into())
}

#[pyclass(from_py_object, get_all)]
#[derive(Debug, Clone)]
pub struct ComputeContextSpecs {
    pub cpus: Option<u32>,
    pub memory: Option<u32>,
    pub cpu_architectures: Option<Vec<DBCPUArchitectureModel>>,
    pub instance_type: Option<String>,
    pub big_instance_type: Option<String>,
    pub big_instance_multiplier: Option<u32>,
    pub storage: Option<u32>,
    pub big_instance_storage: Option<u32>,
    pub cluster_size: u32,
}

#[pymethods]
impl ComputeContextSpecs {
    #[new]
    #[pyo3(signature=(*, cpus=None, memory=None, cpu_architectures=None, instance_type=None, big_instance_type=None, big_instance_multiplier=None, storage=None, big_instance_storage=None, cluster_size))]
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        cpus: Option<u32>,
        memory: Option<u32>,
        cpu_architectures: Option<Vec<DBCPUArchitectureModel>>,
        instance_type: Option<String>,
        big_instance_type: Option<String>,
        big_instance_multiplier: Option<u32>,
        storage: Option<u32>,
        big_instance_storage: Option<u32>,
        cluster_size: u32,
    ) -> Self {
        Self {
            cpus,
            memory,
            cpu_architectures,
            instance_type,
            big_instance_type,
            big_instance_multiplier,
            storage,
            big_instance_storage,
            cluster_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use polars_axum_models::DBCPUArchitectureModel;
    use rstest::rstest;
    use testresult::TestResult;
    use uuid::Uuid;

    use super::*;

    // Helper to call with commonly used defaults for storage and cluster_size
    async fn resolve(
        cpus: Option<u32>,
        memory: Option<u32>,
        cpu_architectures: Option<Vec<DBCPUArchitectureModel>>,
        instance_type: Option<String>,
    ) -> ApiResult<ComputeContextSpecs> {
        resolve_compute_context_specs(
            Uuid::now_v7(),
            cpus,
            memory,
            cpu_architectures,
            instance_type,
            Some(16), // storage — explicit, avoids defaults fetch
            None,
            None,
            None,
            Some(1), // cluster_size — explicit, avoids defaults fetch
        )
        .await
    }

    #[rstest]
    #[tokio::test]
    async fn test_instance_type_and_cpus_rejected() -> TestResult {
        let err = resolve(Some(4), None, None, Some("t3.micro".into()))
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot specify both `instance_type`"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_instance_type_and_memory_rejected() -> TestResult {
        let err = resolve(None, Some(8), None, Some("t3.micro".into()))
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot specify both `instance_type`")
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_instance_type_and_cpu_architectures_rejected() -> TestResult {
        let err = resolve(
            None,
            None,
            Some(vec![DBCPUArchitectureModel::X86_64]),
            Some("t3.micro".into()),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot specify both `instance_type`")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cpus_without_memory_rejected() -> TestResult {
        let err = resolve(Some(4), None, None, None).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("`cpus` and `memory` must be specified together")
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_memory_without_cpus_rejected() -> TestResult {
        let err = resolve(None, Some(8), None, None).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("`cpus` and `memory` must be specified together")
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_big_instance_type_and_multiplier_rejected() -> TestResult {
        let err = resolve_compute_context_specs(
            Uuid::now_v7(),
            None,
            None,
            None,
            None,
            Some(16),
            Some("r5.4xlarge".into()),
            Some(2),
            None,
            Some(1),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot specify both `big_instance_type` AND `big_instance_multiplier`")
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_instance_type_and_big_instance_multiplier_rejected() -> TestResult {
        let err = resolve_compute_context_specs(
            Uuid::now_v7(),
            None,
            None,
            None,
            Some("t3.micro".into()),
            Some(16),
            None,
            Some(2),
            None,
            Some(1),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot specify both `instance_type` AND `big_instance_multiplier`")
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_cpus_memory_and_big_instance_type_rejected() -> TestResult {
        let err = resolve_compute_context_specs(
            Uuid::now_v7(),
            Some(4),
            Some(8),
            None,
            None,
            Some(16),
            Some("r5.4xlarge".into()),
            None,
            None,
            Some(1),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot specify both (`memory` or `cpus`) AND `big_instance_type`")
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_instance_type_succeeds() -> TestResult {
        let specs = resolve(None, None, None, Some("t3.micro".into()))
            .await
            .unwrap();
        assert_eq!(specs.instance_type, Some("t3.micro".into()));
        assert_eq!(specs.cpu_architectures, None);
        assert_eq!(specs.cluster_size, 1);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_cpus_and_memory_succeeds() -> TestResult {
        let specs = resolve(Some(4), Some(8), None, None).await.unwrap();
        assert_eq!(specs.cpus, Some(4));
        assert_eq!(specs.memory, Some(8));

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_cpus_memory_defaults_to_x86_64() -> TestResult {
        let specs = resolve(Some(4), Some(8), None, None).await.unwrap();
        assert_eq!(
            specs.cpu_architectures,
            Some(vec![DBCPUArchitectureModel::X86_64])
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_explicit_cpu_architecture_preserved() -> TestResult {
        let specs = resolve(
            Some(4),
            Some(8),
            Some(vec![DBCPUArchitectureModel::Arm64]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            specs.cpu_architectures,
            Some(vec![DBCPUArchitectureModel::Arm64])
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_instance_type_cpu_architectures_stays_none() -> TestResult {
        let specs = resolve(None, None, None, Some("t3.micro".into()))
            .await
            .unwrap();
        assert_eq!(specs.cpu_architectures, None);
        Ok(())
    }
}
