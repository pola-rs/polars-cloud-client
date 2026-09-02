use client_core::{ApiResult, Client};
use polars_axum_models::WorkSpaceTokenBodyArgs;

use crate::workspace::get_workspace_by_name;

pub async fn create_service_account(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: String,
    name: String,
    description: Option<String>,
) -> ApiResult<()> {
    let workspace = get_workspace_by_name(client, organization_name, workspace_name).await?;
    let token = client
        .create_workspace_token(workspace.id, WorkSpaceTokenBodyArgs { name, description })
        .await?;

    println!("Service account created.");
    println!("ID:            {}", token.id);
    println!("API client ID: {}", token.username);
    println!("API secret:    {}", token.api_secret);
    println!("Workspace ID:  {}", token.workspace_id);
    println!("Created at:    {}", token.created_at);
    if let Some(desc) = token.description {
        println!("Description:   {}", desc);
    }
    println!();
    println!("Store the API secret securely — it will not be shown again.");

    Ok(())
}
