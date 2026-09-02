use client_core::{ApiResult, Client};
use polars_axum_models::{OrganizationModel, WorkSpaceArgs, WorkspaceModel};
use uuid::Uuid;

use crate::organization::{get_all_organizations, set_up_organization};
use crate::workspace::get_all_workspaces;
use crate::{get_user_input, workspace_aws};

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
            println!("Cancelled. Nothing was changed.");
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

async fn ask_connect_aws() -> ApiResult<Option<bool>> {
    loop {
        let choice = get_user_input("\nConnect an AWS account to this workspace? (y/n or q): ")
            .await?
            .to_lowercase();

        match choice.as_str() {
            "y" => return Ok(Some(true)),
            "n" => return Ok(Some(false)),
            "q" => {
                println!("Cancelled. Nothing was changed.");
                return Ok(None);
            },
            _ => println!("Enter y, n or q"),
        }
    }
}

async fn create_new_workspace(
    client: &Client,
    org_id: Uuid,
    name: String,
) -> ApiResult<WorkspaceModel> {
    let workspace = client
        .create_workspace(WorkSpaceArgs {
            organization_id: org_id,
            name,
        })
        .await?;

    println!("Created workspace {}.", workspace.name);

    Ok(workspace)
}

async fn select_or_set_up_workspace(
    client: &Client,
    org: &OrganizationModel,
    workspace_name: Option<String>,
) -> ApiResult<Option<WorkspaceModel>> {
    let workspaces = get_all_workspaces(client, workspace_name.clone(), Some(org.id)).await?;

    if let Some(ref name) = workspace_name {
        if let Some(ws) = workspaces.iter().find(|w| &w.name == name) {
            return Ok(Some(ws.clone()));
        }

        return Ok(Some(
            create_new_workspace(client, org.id, name.clone()).await?,
        ));
    }

    if workspaces.is_empty() {
        let name = get_user_input("New workspace name: ").await?;
        return Ok(Some(create_new_workspace(client, org.id, name).await?));
    }

    // Interactive selection
    println!("\nFound {} workspaces:", workspaces.len());
    println!("{:-<45}", "");
    println!("{:<3} {:<25}", "#", "Name");
    println!("{:-<45}", "");

    for (i, ws) in workspaces.iter().enumerate() {
        println!(
            "{:<3} {:<25}",
            i + 1,
            ws.name.chars().take(25).collect::<String>()
        );
    }
    println!("{:<3} {:<25}", workspaces.len() + 1, "<Create new>");

    loop {
        let choice = get_user_input(&format!(
            "\nSelect workspace (1-{} or q): ",
            workspaces.len() + 1
        ))
        .await?
        .to_lowercase();

        if choice == "q" {
            println!("Cancelled. Nothing was changed.");
            return Ok(None);
        }

        if let Ok(idx) = choice.parse::<usize>() {
            if idx > 0 && idx <= workspaces.len() {
                return Ok(Some(workspaces[idx - 1].clone()));
            } else if idx == workspaces.len() + 1 {
                let name = get_user_input("New workspace name: ").await?;
                return Ok(Some(create_new_workspace(client, org.id, name).await?));
            }
        }
        println!("Enter 1-{} or q", workspaces.len() + 1);
    }
}

pub async fn setup(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: Option<String>,
    connect_aws: Option<bool>,
    verify: bool,
) -> ApiResult<()> {
    let Some(organization) = select_or_set_up_organization(client, organization_name).await? else {
        return Ok(());
    };

    let Some(workspace) = select_or_set_up_workspace(client, &organization, workspace_name).await?
    else {
        return Ok(());
    };

    let connect_aws = match connect_aws {
        Some(connect_aws) => connect_aws,
        None => {
            let Some(connect_aws) = ask_connect_aws().await? else {
                return Ok(());
            };
            connect_aws
        },
    };

    if !connect_aws {
        println!(
            "Workspace '{}' is ready. Install the Polars Cloud Helm chart in your Kubernetes \
             cluster to run compute on it, or run `pc workspace aws connect` to connect AWS.",
            workspace.name
        );
        return Ok(());
    }

    if workspace_aws::is_connected(client, workspace.id).await? {
        println!(
            "Workspace '{}' already has an AWS account connected. Run `pc compute start -w {}` to \
             start compute on it.",
            workspace.name, workspace.name
        );
        return Ok(());
    }

    workspace_aws::connect_by_id(client, workspace.id, verify).await?;

    println!(
        "Workspace '{}' is ready. Run `pc compute start -w {}` to start compute on it.",
        workspace.name, workspace.name
    );

    Ok(())
}
