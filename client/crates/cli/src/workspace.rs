use std::time::Duration;

use anyhow::anyhow;
use client_core::{ApiResult, Client};
use comfy_table::Table;
use comfy_table::presets::NOTHING;
use polars_axum_models::{WorkspaceDeploymentModel, WorkspaceModel, WorkspaceStateModel};
use uuid::Uuid;

use crate::organization::get_organization_by_name;

pub async fn get_all_workspaces(
    client: &Client,
    name: Option<String>,
    organization_id: Option<Uuid>,
) -> ApiResult<Vec<WorkspaceModel>> {
    client.get_workspaces(name, organization_id).await
}

pub async fn get_workspace_by_name(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: String,
) -> ApiResult<WorkspaceModel> {
    let mut workspaces = if let Some(organization_name) = organization_name {
        let Some(organization) =
            get_organization_by_name(client, organization_name.clone()).await?
        else {
            return Err(
                anyhow!("No organization with the name {organization_name} was found").into(),
            );
        };
        get_all_workspaces(client, Some(workspace_name.clone()), Some(organization.id)).await?
    } else {
        get_all_workspaces(client, Some(workspace_name.clone()), None).await?
    };

    workspaces.retain(|x| x.name == workspace_name);

    let workspace = match workspaces.len() {
        0 => return Err(anyhow!("No workspace with the name {workspace_name} was found").into()),
        1 => workspaces.remove(0),
        _ => return Err(anyhow!(
            "Multiple workspaces with the name {workspace_name} were found. Specify an organization"
        )
        .into()),
    };

    Ok(workspace)
}

pub async fn wait_until_active(
    client: &Client,
    mut workspace: WorkspaceModel,
    interval_secs: u64,
    timeout_secs: u64,
) -> ApiResult<bool> {
    let max_polls = (timeout_secs / interval_secs) + 1;

    tracing::debug!("polling workspace details endpoint");

    for _ in 0..max_polls {
        let prev_status = workspace.status.clone();

        // Assuming load() updates self by calling the API
        workspace = client.get_workspace(workspace.id).await?;
        tracing::debug!(status = ?workspace.status, "current workspace status");

        match workspace.status {
            WorkspaceStateModel::Active => {
                tracing::info!("workspace successfully verified");
                return Ok(true);
            },
            WorkspaceStateModel::Deleted => {
                tracing::info!(status = ?workspace.status, "workspace verification failed");
                return Ok(false);
            },
            _ => {},
        }

        if workspace.status != prev_status {
            match workspace.status {
                WorkspaceStateModel::Pending => {
                    tracing::info!("workspace stack is being deployed");
                },
                WorkspaceStateModel::Failed => {
                    let msg = format!(
                        "Deploying the workspace failed. Check the status in AWS CloudFormation: {}",
                        workspace.cloud_resources_url.unwrap_or("N/A".into())
                    );
                    tracing::debug!("{msg}");
                    return Err(anyhow!(msg).into());
                },
                _ => {},
            }
        }

        tokio::time::sleep(Duration::from_secs(interval_secs)).await;
    }

    let base_msg = if workspace.status == WorkspaceStateModel::Failed {
        "Workspace verification has timed out or we failed to detect a status change."
    } else {
        "Workspace verification has timed out."
    };

    let mut msg = format!(
        "{base_msg} Check the status of the deployment in your AWS CloudFormation dashboard",
    );

    if let Some(ref url) = workspace.cloud_resources_url
        && !url.is_empty()
    {
        msg.push_str(&format!(" or by following this link: {url}"));
    }

    tracing::debug!("{msg}");
    Err(anyhow!(msg).into())
}

pub async fn verify_workspace(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: String,
    interval_secs: Option<u64>,
    timeout: Option<u64>,
) -> ApiResult<()> {
    let workspace = get_workspace_by_name(client, organization_name, workspace_name).await?;
    wait_until_active(
        client,
        workspace,
        interval_secs.unwrap_or(2),
        timeout.unwrap_or(300),
    )
    .await?;
    Ok(())
}

pub async fn print_workspaces(client: &Client) -> ApiResult<()> {
    let mut table = Table::new();
    table
        .load_preset(NOTHING)
        .set_header(vec!["NAME", "ID", "STATUS"]);

    for ws in get_all_workspaces(client, None, None).await? {
        table.add_row(vec![ws.name, ws.id.to_string(), ws.status.to_string()]);
    }

    println!("{table}");

    Ok(())
}

pub async fn print_workspace_details(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: String,
) -> ApiResult<()> {
    let workspace = get_workspace_by_name(client, organization_name, workspace_name).await?;
    println!("{:#?}", workspace);

    Ok(())
}

pub async fn delete_workspace(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: String,
) -> ApiResult<()> {
    let workspace = get_workspace_by_name(client, organization_name, workspace_name).await?;

    match workspace.deployment {
        WorkspaceDeploymentModel::Aws => {
            client.delete_aws_workspace(workspace.id).await?;
        },
        WorkspaceDeploymentModel::OnPrem => {
            client.delete_on_prem_workspace(workspace.id).await?;
        },
    }

    Ok(())
}
