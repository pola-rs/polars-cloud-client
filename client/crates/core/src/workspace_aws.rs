use polars_axum_models::{AwsConnectionStatusModel, WorkspaceAwsConnectionModel};
use reqwest::StatusCode;
use uuid::Uuid;

use crate::client_trait::Client;
use crate::error::ApiError;

/// The workspace's AWS connection, `None` when it was never connected.
///
/// The route answers with a 404 in that case, so the mapping lives here instead of at each call
/// site.
pub async fn connection(
    client: &Client,
    workspace_id: Uuid,
) -> Result<Option<WorkspaceAwsConnectionModel>, ApiError> {
    match client.get_aws_connection(workspace_id).await {
        Ok(connection) => Ok(Some(connection)),
        Err(e) if e.status() == Some(StatusCode::NOT_FOUND) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Whether the workspace has an AWS account connected and ready to run clusters.
pub async fn is_connected(client: &Client, workspace_id: Uuid) -> Result<bool, ApiError> {
    Ok(connection(client, workspace_id)
        .await?
        .is_some_and(|connection| connection.status == AwsConnectionStatusModel::Completed))
}
