use std::time::Duration;

use client_core::{
    ApiError, ApiResult, CTRL_PLN_CLIENT_GLOBAL, VERSIONS, poll_compute_status_until,
    resolve_compute_context_specs,
};
use comfy_table::Table;
use comfy_table::presets::NOTHING;
use polars_axum_models::{
    ClusterModeModel, ComputeModel, ComputeStatusModel, GetClusterFilterArgs, InstanceSpecsModel,
    Pagination, StartComputeClusterArgs, WorkspaceModel,
};
use polars_backend_client::client::ApiClient;
use reqwest::StatusCode;
use uuid::Uuid;

use crate::workspace::{get_all_workspaces, get_workspace_by_name};

pub async fn get_all_clusters(workspace_id: Uuid) -> ApiResult<Vec<ComputeModel>> {
    CTRL_PLN_CLIENT_GLOBAL
        .call_paginated_async(|client: &ApiClient, page: i64| {
            let pagination = Pagination {
                page,
                limit: 1000,
                offset: 0,
            };
            let params = GetClusterFilterArgs {
                status: None,
                current_user_only: false,
            };

            client.get_compute_clusters(workspace_id, params, pagination)
        })
        .await
}

pub async fn print_compute_clusters() -> ApiResult<()> {
    let mut table = Table::new();
    table
        .load_preset(NOTHING)
        .set_header(vec!["ID", "INSTANCE TYPE", "WORKSPACE", "STATUS"]);

    for workspace in get_all_workspaces(None, None).await? {
        for cluster in get_all_clusters(workspace.id).await? {
            table.add_row(vec![
                cluster.id.to_string(),
                cluster
                    .instance_type
                    .map(|x| x.to_string())
                    .unwrap_or("None".into()),
                workspace.name.clone(),
                cluster.status.to_string(),
            ]);
        }
    }

    println!("{table}");

    Ok(())
}

pub async fn stop_compute_cluster(
    organization_name: Option<String>,
    workspace_name: String,
    cluster_id: Uuid,
) -> ApiResult<()> {
    let workspace = get_workspace_by_name(organization_name, workspace_name).await?;

    CTRL_PLN_CLIENT_GLOBAL
        .call_async(|client| client.stop_compute_cluster(workspace.id, cluster_id))
        .await?;

    Ok(())
}

pub async fn print_compute_cluster_details(
    organization_name: Option<String>,
    workspace_name: String,
    cluster_id: Uuid,
) -> ApiResult<()> {
    let workspace = get_workspace_by_name(organization_name, workspace_name).await?;

    let cluster = CTRL_PLN_CLIENT_GLOBAL
        .call_async(|client| client.get_compute_cluster(workspace.id, cluster_id))
        .await?;

    println!("{:#?}", cluster);

    Ok(())
}

pub async fn get_default_workspace() -> ApiResult<WorkspaceModel> {
    let user = CTRL_PLN_CLIENT_GLOBAL
        .call_async(|client| client.get_logged_in_user())
        .await?;

    let Some(workspace_id) = user.default_workspace_id else {
        return Err(anyhow::anyhow!(
            r"No (default) workspace specified.

Hint: Either directly specify the workspace or set your default workspace in the dashboard."
        )
        .into());
    };

    let result = CTRL_PLN_CLIENT_GLOBAL
        .call_async(|client| client.get_workspace(workspace_id))
        .await;

    let workspace = match result {
        Err(ApiError::StatusError {
            status,
            url: _,
            body: _,
        }) if status == StatusCode::NOT_FOUND => {
            return Err(anyhow::anyhow!(
                r"The workspace you had set as default either does not exist anymore or you do not have access anymore.

Hint: Set a new default workspace in the dashboard."
            ).into());
        },
        result => result?,
    };

    Ok(workspace)
}

#[expect(clippy::too_many_arguments)]
pub async fn start_compute_cluster(
    organization_name: Option<String>,
    workspace_name: Option<String>,
    cpus: Option<u32>,
    memory: Option<u32>,
    instance_type: Option<String>,
    storage: Option<u32>,
    cluster_size: u32,
    wait: bool,
) -> ApiResult<()> {
    let workspace = if let Some(workspace_name) = workspace_name {
        get_workspace_by_name(organization_name, workspace_name).await?
    } else {
        get_default_workspace().await?
    };

    let specs = resolve_compute_context_specs(
        workspace.id,
        cpus,
        memory,
        None,
        instance_type,
        storage,
        None,
        None,
        None,
        Some(cluster_size),
    )
    .await?;

    let specs = if let Some(instance_type) = &specs.instance_type {
        InstanceSpecsModel::InstanceType {
            standard: instance_type.clone(),
            big: None,
        }
    } else if let Some(cpus) = specs.cpus
        && let Some(ram_gb) = specs.memory
    {
        InstanceSpecsModel::Specs {
            cpus,
            ram_gb,
            cpu_architectures: specs.cpu_architectures.into_iter().flatten().collect(),
            multiplier: specs.big_instance_multiplier,
        }
    } else {
        unreachable!("resolve_compute_context_specs should check at least one of the fields is set")
    };

    let python_version = VERSIONS.get().unwrap().as_ref().unwrap().0.python;
    let polars_version = VERSIONS.get().unwrap().as_ref().unwrap().0.polars;
    let cluster = CTRL_PLN_CLIENT_GLOBAL
        .call_async(|client| {
            client.start_compute_cluster(
                workspace.id,
                StartComputeClusterArgs {
                    instance: specs,
                    storage,
                    big_instance_storage: None,
                    cluster_size,
                    mode: ClusterModeModel::Proxy,
                    python_version,
                    polars_version,
                    labels: None,
                    log_level: None,
                    idle_timeout_mins: None,
                    requirements_txt: None,
                },
            )
        })
        .await?;

    if wait {
        poll_compute_status_until(
            workspace.id,
            cluster.id,
            ComputeStatusModel::Idle,
            Duration::from_secs(30),
            Duration::from_secs(3),
            Duration::from_secs(300),
        )
        .await?;
    }

    println!("Cluster was started with ID: {}", cluster.id);

    Ok(())
}
