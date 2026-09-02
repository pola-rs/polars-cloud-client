use std::collections::HashMap;
use std::time::Duration;

use anyhow::anyhow;
use client_core::{
    ApiResult, Client, VERSIONS, poll_compute_status_until, resolve_compute_context_specs,
};
use comfy_table::Table;
use comfy_table::presets::NOTHING;
use polars_axum_models::{
    ComputeModel, ComputeStatusModel, DBClusterModeModel, GetClusterFilterArgs, InstanceSpecsModel,
    StartComputeClusterArgs, WorkspaceModel,
};
use uuid::Uuid;

use crate::organization::get_organization_by_name;
use crate::workspace::{get_all_workspaces, get_workspace_by_name};

pub async fn get_all_clusters(client: &Client, workspace_id: Uuid) -> ApiResult<Vec<ComputeModel>> {
    let params = GetClusterFilterArgs {
        status: None,
        deployment_type: None,
        current_user_only: false,
    };

    client.get_compute_clusters(workspace_id, params).await
}

/// The workspaces `pc compute list` reports on: one when named, otherwise every workspace in the
/// organization, otherwise every workspace you have access to.
async fn workspaces_to_list(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: Option<String>,
) -> ApiResult<Vec<WorkspaceModel>> {
    if let Some(workspace_name) = workspace_name {
        return Ok(vec![
            get_workspace_by_name(client, organization_name, workspace_name).await?,
        ]);
    }

    let organization_id = match organization_name {
        Some(name) => match get_organization_by_name(client, name.clone()).await? {
            Some(organization) => Some(organization.id),
            None => {
                return Err(anyhow!("No organization with the name {name} was found").into());
            },
        },
        None => None,
    };

    get_all_workspaces(client, None, organization_id).await
}

pub async fn print_compute_clusters(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: Option<String>,
) -> ApiResult<()> {
    let workspaces = workspaces_to_list(client, organization_name, workspace_name).await?;

    let mut table = Table::new();
    table
        .load_preset(NOTHING)
        .set_header(vec!["ID", "INSTANCE TYPE", "WORKSPACE", "STATUS"]);

    let mut any = false;
    for workspace in workspaces {
        for cluster in get_all_clusters(client, workspace.id).await? {
            any = true;
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

    if !any {
        println!("No compute clusters found. Run `pc compute start -w <workspace>` to start one.");
        return Ok(());
    }

    println!("{table}");

    Ok(())
}

pub async fn stop_compute_cluster(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: String,
    cluster_id: Uuid,
) -> ApiResult<()> {
    let workspace = get_workspace_by_name(client, organization_name, workspace_name).await?;

    client
        .stop_compute_cluster(workspace.id, cluster_id)
        .await?;

    println!("Stopping cluster {cluster_id}. Run `pc compute list` to check when it has stopped.");

    Ok(())
}

pub async fn print_compute_cluster_details(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: String,
    cluster_id: Uuid,
) -> ApiResult<()> {
    let workspace = get_workspace_by_name(client, organization_name, workspace_name).await?;

    let cluster = client.get_compute_cluster(workspace.id, cluster_id).await?;

    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.add_row(vec!["ID", &cluster.id.to_string()]);
    table.add_row(vec!["Status", &cluster.status.to_string()]);
    table.add_row(vec![
        "Instance type",
        &cluster
            .instance_type
            .map(|x| x.to_string())
            .unwrap_or("None".into()),
    ]);
    table.add_row(vec!["Cluster size", &cluster.cluster_size.to_string()]);
    table.add_row(vec!["Workspace", &workspace.name]);
    table.add_row(vec!["Polars version", &cluster.polars_version.to_string()]);
    table.add_row(vec!["Requested at", &cluster.request_time.to_string()]);

    println!("{table}");

    Ok(())
}

#[expect(clippy::too_many_arguments)]
pub async fn start_compute_cluster(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: String,
    cpus: Option<u32>,
    memory: Option<u32>,
    instance_type: Option<String>,
    storage: Option<u32>,
    cluster_size: u32,
    env_vars: HashMap<String, String>,
    wait: bool,
) -> ApiResult<()> {
    let workspace = get_workspace_by_name(client, organization_name, workspace_name).await?;

    let specs = resolve_compute_context_specs(
        client.clone(),
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
    let cluster = client
        .start_compute_cluster(
            workspace.id,
            StartComputeClusterArgs {
                instance: specs,
                storage,
                big_instance_storage: None,
                cluster_size,
                mode: DBClusterModeModel::Proxy,
                python_version,
                polars_version,
                labels: None,
                log_level: None,
                idle_timeout_mins: None,
                requirements_txt: None,
                settings: None,
                env_vars,
            },
        )
        .await?;

    if wait {
        println!(
            "Starting cluster {}. This can take a few minutes...",
            cluster.id
        );

        poll_compute_status_until(
            client.clone(),
            workspace.id,
            cluster.id,
            ComputeStatusModel::Idle,
            Duration::from_secs(30),
            Duration::from_secs(3),
            Duration::from_secs(300),
        )
        .await?;

        println!("Cluster {} is ready.", cluster.id);
    } else {
        println!(
            "Starting cluster {}. Run `pc compute list` to check when it is ready.",
            cluster.id
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use client_core::MockControlPlaneClient;

    use super::*;
    use crate::test_fixtures::{organization, workspace as workspace_fixture};

    #[tokio::test]
    async fn list_only_asks_for_the_named_organization() {
        let wanted = organization("wanted");
        let wanted_id = wanted.id;
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_organizations()
            .returning(move |_| Ok(vec![wanted.clone(), organization("other")]));
        mock.expect_get_workspaces()
            .withf(move |_, organization_id| *organization_id == Some(wanted_id))
            .times(1)
            .returning(|_, id| Ok(vec![workspace_fixture("ws", id.unwrap())]));
        mock.expect_get_compute_clusters()
            .returning(|_, _| Ok(vec![]));
        let client: Client = Arc::new(mock);

        print_compute_clusters(&client, Some("wanted".into()), None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn list_rejects_an_organization_that_does_not_exist() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_organizations()
            .returning(|_| Ok(vec![organization("other")]));
        mock.expect_get_workspaces().never();
        mock.expect_get_compute_clusters().never();
        let client: Client = Arc::new(mock);

        let error = print_compute_clusters(&client, Some("missing".into()), None)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("No organization with the name missing")
        );
    }

    #[tokio::test]
    async fn list_only_asks_for_the_named_workspace() {
        let organization_id = Uuid::now_v7();
        let wanted = workspace_fixture("wanted", organization_id);
        let wanted_id = wanted.id;
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_workspaces().returning(move |_, _| {
            Ok(vec![
                wanted.clone(),
                workspace_fixture("other", organization_id),
            ])
        });
        mock.expect_get_compute_clusters()
            .withf(move |workspace_id, _| *workspace_id == wanted_id)
            .times(1)
            .returning(|_, _| Ok(vec![]));
        let client: Client = Arc::new(mock);

        print_compute_clusters(&client, None, Some("wanted".into()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn list_rejects_a_workspace_that_does_not_exist() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_workspaces()
            .returning(|_, _| Ok(vec![workspace_fixture("other", Uuid::now_v7())]));
        mock.expect_get_compute_clusters().never();
        let client: Client = Arc::new(mock);

        let error = print_compute_clusters(&client, None, Some("missing".into()))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("No workspace with the name missing")
        );
    }
}
