use std::collections::HashMap;

use anyhow::anyhow;
use client_core::{ApiResult, Client};
use comfy_table::Table;
use comfy_table::presets::NOTHING;
use polars_axum_models::{WorkSpaceArgs, WorkspaceModel};
use uuid::Uuid;

use crate::organization::{get_all_organizations, get_organization_by_name, resolve_organization};
use crate::workspace_aws;

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

/// Create a workspace without attaching any infrastructure to it.
pub async fn create_workspace(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: String,
    connect_aws: bool,
    verify: bool,
) -> ApiResult<()> {
    let organization = resolve_organization(client, organization_name).await?;

    tracing::debug!("creating workspace");
    let workspace = client
        .create_workspace(WorkSpaceArgs {
            organization_id: organization.id,
            name: workspace_name,
        })
        .await?;

    println!("Created workspace {}.", workspace.name);

    if connect_aws {
        workspace_aws::connect_by_id(client, workspace.id, verify).await?;
    } else {
        println!(
            "Run `pc workspace aws connect -w {}` to connect an AWS account to it.",
            workspace.name
        );
    }

    Ok(())
}

pub async fn delete_workspace(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: String,
) -> ApiResult<()> {
    let workspace = get_workspace_by_name(client, organization_name, workspace_name).await?;

    workspace_aws::ensure_deletable(client, workspace.id).await?;

    client
        .delete_workspace(workspace.id)
        .await
        .map_err(workspace_aws::explain_delete_conflict)?;

    println!("Deleted workspace {}.", workspace.name);

    Ok(())
}

pub async fn print_workspaces(client: &Client, organization_name: Option<String>) -> ApiResult<()> {
    let organization = match organization_name {
        Some(name) => match get_organization_by_name(client, name.clone()).await? {
            Some(organization) => Some(organization),
            None => return Err(anyhow!("No organization with the name {name} was found").into()),
        },
        None => None,
    };

    let (organizations, workspaces) = tokio::try_join!(
        get_all_organizations(client, None),
        get_all_workspaces(client, None, organization.as_ref().map(|o| o.id)),
    )?;
    let organizations: HashMap<Uuid, String> = organizations
        .into_iter()
        .map(|org| (org.id, org.name))
        .collect();

    if workspaces.is_empty() {
        match &organization {
            Some(organization) => println!(
                "Organization {} has no workspaces yet. Run `pc workspace create -o {} -w <name>` \
                 to create one.",
                organization.name, organization.name
            ),
            None => println!(
                "No workspaces yet. Run `pc workspace create -o <org> -w <name>` to create one."
            ),
        }
        return Ok(());
    }

    let mut table = Table::new();
    table
        .load_preset(NOTHING)
        .set_header(vec!["NAME", "ID", "ORGANIZATION"]);

    for ws in workspaces {
        let organization = organizations
            .get(&ws.organization_id)
            .cloned()
            .unwrap_or_else(|| ws.organization_id.to_string());
        table.add_row(vec![ws.name, ws.id.to_string(), organization]);
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
    let aws_connected = workspace_aws::is_connected(client, workspace.id).await?;

    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.add_row(vec!["Name", &workspace.name]);
    table.add_row(vec!["ID", &workspace.id.to_string()]);
    table.add_row(vec!["Description", &workspace.description]);
    table.add_row(vec![
        "Organization ID",
        &workspace.organization_id.to_string(),
    ]);
    table.add_row(vec![
        "AWS connected",
        if aws_connected { "yes" } else { "no" },
    ]);
    table.add_row(vec![
        "Idle timeout (mins)",
        &workspace.idle_timeout_mins.to_string(),
    ]);
    table.add_row(vec!["Created at", &workspace.created_at.to_string()]);

    println!("{table}");

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use client_core::MockControlPlaneClient;
    use polars_axum_models::AwsConnectionStatusModel;
    use reqwest::StatusCode;

    use super::*;
    use crate::test_fixtures::{
        aws_connection, organization, setup_url, status_error, workspace as workspace_fixture,
    };

    #[tokio::test]
    async fn delete_refuses_while_the_stack_is_still_being_created() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_workspaces()
            .returning(|_, _| Ok(vec![workspace_fixture("ws", Uuid::now_v7())]));
        mock.expect_get_aws_connection()
            .returning(|id| Ok(aws_connection(id, AwsConnectionStatusModel::Pending)));
        mock.expect_delete_workspace().never();
        let client: Client = Arc::new(mock);

        let error = delete_workspace(&client, None, "ws".into())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("still setting up"));
    }

    #[tokio::test]
    async fn delete_points_at_disconnect_when_aws_is_still_attached() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_workspaces()
            .returning(|_, _| Ok(vec![workspace_fixture("ws", Uuid::now_v7())]));
        mock.expect_get_aws_connection()
            .returning(|id| Ok(aws_connection(id, AwsConnectionStatusModel::Completed)));
        mock.expect_delete_workspace()
            .returning(|_| Err(status_error(StatusCode::BAD_REQUEST)));
        let client: Client = Arc::new(mock);

        let error = delete_workspace(&client, None, "ws".into())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("pc workspace aws disconnect"));
    }

    #[tokio::test]
    async fn delete_succeeds_when_no_aws_is_involved() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_workspaces()
            .returning(|_, _| Ok(vec![workspace_fixture("ws", Uuid::now_v7())]));
        mock.expect_get_aws_connection()
            .returning(|_| Err(status_error(StatusCode::NOT_FOUND)));
        mock.expect_delete_workspace()
            .times(1)
            .returning(|_| Ok(()));
        let client: Client = Arc::new(mock);

        delete_workspace(&client, None, "ws".into()).await.unwrap();
    }

    #[tokio::test]
    async fn create_reuses_an_existing_organization() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_organizations()
            .returning(|_| Ok(vec![organization("org")]));
        mock.expect_create_organization().never();
        mock.expect_create_workspace()
            .times(1)
            .returning(|params| Ok(workspace_fixture("ws", params.organization_id)));
        let client: Client = Arc::new(mock);

        create_workspace(&client, Some("org".into()), "ws".into(), false, true)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn create_rejects_an_organization_that_does_not_exist() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_organizations().returning(|_| Ok(vec![]));
        mock.expect_create_organization().never();
        mock.expect_create_workspace().never();
        let client: Client = Arc::new(mock);

        let error = create_workspace(&client, Some("org".into()), "ws".into(), false, true)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("pc organization setup --name org")
        );
    }

    #[tokio::test]
    async fn create_uses_the_only_organization_when_none_is_named() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_organizations()
            .returning(|_| Ok(vec![organization("org")]));
        mock.expect_create_organization().never();
        mock.expect_create_workspace()
            .times(1)
            .returning(|params| Ok(workspace_fixture("ws", params.organization_id)));
        let client: Client = Arc::new(mock);

        create_workspace(&client, None, "ws".into(), false, true)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn create_asks_which_organization_when_there_are_several() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_organizations()
            .returning(|_| Ok(vec![organization("one"), organization("two")]));
        mock.expect_create_workspace().never();
        let client: Client = Arc::new(mock);

        let error = create_workspace(&client, None, "ws".into(), false, true)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("one, two"));
    }

    #[tokio::test]
    async fn create_points_at_organization_setup_when_there_are_none() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_organizations().returning(|_| Ok(vec![]));
        mock.expect_create_organization().never();
        mock.expect_create_workspace().never();
        let client: Client = Arc::new(mock);

        let error = create_workspace(&client, None, "ws".into(), false, true)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("pc organization setup"));
    }

    #[tokio::test]
    async fn create_only_touches_aws_when_asked_to() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_organizations()
            .returning(|_| Ok(vec![organization("org")]));
        mock.expect_create_workspace()
            .returning(|params| Ok(workspace_fixture("ws", params.organization_id)));
        mock.expect_get_aws_workspace_setup_url().never();
        let client: Client = Arc::new(mock);

        create_workspace(&client, Some("org".into()), "ws".into(), false, false)
            .await
            .unwrap();

        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_organizations()
            .returning(|_| Ok(vec![organization("org")]));
        mock.expect_create_workspace()
            .returning(|params| Ok(workspace_fixture("ws", params.organization_id)));
        mock.expect_get_aws_workspace_setup_url()
            .times(1)
            .returning(|_| Ok(setup_url()));
        let client: Client = Arc::new(mock);

        create_workspace(&client, Some("org".into()), "ws".into(), true, false)
            .await
            .unwrap();
    }

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
        let client: Client = Arc::new(mock);

        print_workspaces(&client, Some("wanted".into()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn list_rejects_an_organization_that_does_not_exist() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_organizations()
            .returning(|_| Ok(vec![organization("other")]));
        mock.expect_get_workspaces().never();
        let client: Client = Arc::new(mock);

        let error = print_workspaces(&client, Some("missing".into()))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("No organization with the name missing")
        );
    }
}
