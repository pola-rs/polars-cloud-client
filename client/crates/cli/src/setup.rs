use std::error::Error;

use client_core::{ApiResult, Client};
use polars_axum_models::{
    OrganizationModel, WorkSpaceArgs, WorkspaceDeploymentModel, WorkspaceModel, WorkspaceStateModel,
};
use uuid::Uuid;

use crate::get_user_input;
use crate::organization::{get_all_organizations, set_up_organization};
use crate::workspace::{get_all_workspaces, wait_until_active};

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum WorkspaceKind {
    Aws,
    Kubernetes,
}

async fn select_or_set_up_organization(
    client: &Client,
    organization_name: Option<String>,
) -> ApiResult<Option<OrganizationModel>> {
    let organizations = get_all_organizations(client, organization_name.clone()).await?;

    if let Some(ref name) = organization_name {
        if let Some(org) = organizations.iter().find(|o| &o.name == name) {
            return Ok(Some(org.clone()));
        } else {
            return Ok(Some(set_up_organization(client, organization_name).await?));
        }
    }

    if organizations.is_empty() {
        return Ok(Some(set_up_organization(client, None).await?));
    }

    // 4. Interactive selection
    println!("\nFound {} available organizations:", organizations.len());
    println!("{:-<45}", "");
    println!("{:<3} {:<25}", "#", "Name");
    println!("{:-<45}", "");

    for (i, org) in organizations.iter().enumerate() {
        println!(
            "{:<3} {:<25}",
            i + 1,
            org.name.chars().take(25).collect::<String>()
        );
    }
    println!("{:<3} {:<25}", organizations.len() + 1, "<Create new>");

    loop {
        let choice = get_user_input(&format!(
            "\nSelect organization (1-{} or q): ",
            organizations.len() + 1
        ))
        .await?
        .to_lowercase();

        if choice == "q" {
            return Ok(None);
        }

        if let Ok(idx) = choice.parse::<usize>() {
            if idx > 0 && idx <= organizations.len() {
                return Ok(Some(organizations[idx - 1].clone()));
            } else if idx == organizations.len() + 1 {
                return Ok(Some(set_up_organization(client, None).await?));
            }
        }
        println!("Enter 1-{} or q", organizations.len() + 1);
    }
}

async fn select_workspace_type() -> ApiResult<Option<WorkspaceKind>> {
    println!("\nSelect workspace type:");
    println!("{:-<30}", "");
    println!("{:<3} {:<15}", "1", "AWS");
    println!("{:<3} {:<15}", "2", "Kubernetes");

    loop {
        let choice = get_user_input("\nSelect type (1-2 or q): ")
            .await?
            .to_lowercase();

        if choice == "q" {
            return Ok(None);
        }

        match choice.as_str() {
            "1" => return Ok(Some(WorkspaceKind::Aws)),
            "2" => return Ok(Some(WorkspaceKind::Kubernetes)),
            _ => println!("Enter 1-2 or q"),
        }
    }
}

async fn resolve_kind(kind: Option<WorkspaceKind>) -> ApiResult<Option<WorkspaceKind>> {
    match kind {
        Some(k) => Ok(Some(k)),
        None => select_workspace_type().await,
    }
}

async fn create_new_workspace(
    client: &Client,
    org_id: Uuid,
    name: String,
    kind: &WorkspaceKind,
) -> ApiResult<WorkspaceModel> {
    let params = WorkSpaceArgs {
        organization_id: org_id,
        name,
    };
    match kind {
        WorkspaceKind::Aws => Ok(client.create_aws_workspace(params).await?.workspace),
        WorkspaceKind::Kubernetes => client.create_on_prem_workspace(params).await,
    }
}

async fn select_or_set_up_workspace(
    client: &Client,
    org: &OrganizationModel,
    workspace_name: Option<String>,
    workspace_kind: Option<WorkspaceKind>,
) -> ApiResult<Option<(WorkspaceModel, WorkspaceKind)>> {
    let workspaces = get_all_workspaces(client, workspace_name.clone(), Some(org.id)).await?;

    if let Some(ref name) = workspace_name {
        if let Some(ws) = workspaces.iter().find(|w| &w.name == name) {
            let Some(kind) = resolve_kind(workspace_kind).await? else {
                return Ok(None);
            };
            return Ok(Some((ws.clone(), kind)));
        }

        let Some(kind) = resolve_kind(workspace_kind).await? else {
            return Ok(None);
        };
        let ws = create_new_workspace(client, org.id, name.clone(), &kind).await?;
        return Ok(Some((ws, kind)));
    }

    // Filter for deployable workspaces (Uninitialized or Failed)
    let deployable: Vec<_> = workspaces
        .into_iter()
        .filter(|ws| {
            matches!(
                ws.status,
                WorkspaceStateModel::Uninitialized | WorkspaceStateModel::Failed
            )
        })
        .collect();

    if deployable.is_empty() {
        let Some(kind) = resolve_kind(workspace_kind).await? else {
            return Ok(None);
        };
        let name = get_user_input("New workspace name: ").await?;
        let ws = create_new_workspace(client, org.id, name, &kind).await?;
        return Ok(Some((ws, kind)));
    }

    // Interactive selection
    println!(
        "\nFound {} (re)deployable (Uninitialized/Failed) workspaces:",
        deployable.len()
    );
    println!("{:-<45}", "");
    println!("{:<3} {:<25} {:<12}", "#", "Name", "Status");
    println!("{:-<45}", "");

    for (i, ws) in deployable.iter().enumerate() {
        println!(
            "{:<3} {:<25} {:<12?}",
            i + 1,
            ws.name.chars().take(25).collect::<String>(),
            ws.status
        );
    }
    println!("{:<3} {:<25}", deployable.len() + 1, "<Create new>");

    loop {
        let choice = get_user_input(&format!(
            "\nSelect workspace (1-{} or q): ",
            deployable.len() + 1
        ))
        .await?
        .to_lowercase();

        if choice == "q" {
            return Ok(None);
        }

        if let Ok(idx) = choice.parse::<usize>() {
            if idx > 0 && idx <= deployable.len() {
                let ws = deployable[idx - 1].clone();
                let kind = match ws.deployment {
                    WorkspaceDeploymentModel::Aws => WorkspaceKind::Aws,
                    WorkspaceDeploymentModel::OnPrem => WorkspaceKind::Kubernetes,
                };
                return Ok(Some((ws, kind)));
            } else if idx == deployable.len() + 1 {
                let Some(kind) = resolve_kind(workspace_kind.clone()).await? else {
                    return Ok(None);
                };
                let name = get_user_input("New workspace name: ").await?;
                let ws = create_new_workspace(client, org.id, name, &kind).await?;
                return Ok(Some((ws, kind)));
            }
        }
        println!("Enter 1-{} or q", deployable.len() + 1);
    }
}

pub async fn setup(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: Option<String>,
    workspace_kind: Option<WorkspaceKind>,
    verify: bool,
) -> ApiResult<()> {
    let Some(organization) = select_or_set_up_organization(client, organization_name).await? else {
        return Ok(());
    };

    let Some((workspace, kind)) =
        select_or_set_up_workspace(client, &organization, workspace_name, workspace_kind).await?
    else {
        return Ok(());
    };

    if !matches!(
        workspace.status,
        WorkspaceStateModel::Uninitialized | WorkspaceStateModel::Failed
    ) {
        return Ok(());
    }

    match kind {
        WorkspaceKind::Aws => {
            let url = client.get_aws_workspace_setup_url(workspace.id).await?;

            println!(
                r"Please complete the workspace setup process in your browser.
Workspace creation may take up to 5 minutes to complete after clicking 'Create stack'.
If your browser did not open automatically, please go to the following URL:
{}",
                url.full_setup_url
            );

            if let Err(error) = webbrowser::open(&url.full_setup_url) {
                tracing::warn!(error = &error as &dyn Error, "Could not open browser");
            }
        },
        WorkspaceKind::Kubernetes => {
            println!(
                "Kubernetes workspace '{}' has been created. It will become active once your cluster connects to Polars Cloud.",
                workspace.name
            );
        },
    }

    if verify {
        wait_until_active(client, workspace, 2, 300).await?;
        tracing::info!("Workspace setup completed");
    }

    Ok(())
}
