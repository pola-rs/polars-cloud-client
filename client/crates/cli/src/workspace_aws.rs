use std::time::Duration;

use anyhow::anyhow;
use client_core::{ApiError, ApiResult, Client, workspace_aws};
use polars_axum_models::{AwsConnectionStatusModel, WorkspaceAwsConnectionModel};
use reqwest::StatusCode;
use uuid::Uuid;

use crate::workspace::get_workspace_by_name;

const POLL_INTERVAL_SECS: u64 = 2;
const POLL_TIMEOUT_SECS: u64 = 300;

pub use workspace_aws::is_connected;

pub async fn wait_until_connected(
    client: &Client,
    workspace_id: Uuid,
    interval_secs: u64,
    timeout_secs: u64,
) -> ApiResult<()> {
    use AwsConnectionStatusModel::{Completed, Deleted, Failed};

    let max_polls = (timeout_secs / interval_secs) + 1;

    tracing::debug!("polling workspace AWS connection endpoint");

    let mut connection = None;
    let mut prev_status = None;
    for poll in 0..max_polls {
        if poll > 0 {
            if poll == 1 {
                println!("Waiting for AWS to finish setting up. This can take a few minutes...");
            }

            tokio::time::sleep(Duration::from_secs(interval_secs)).await;
        }

        connection = workspace_aws::connection(client, workspace_id).await?;
        let status = connection.as_ref().map(|c| c.status.clone());
        tracing::debug!(?status, "current AWS connection status");

        // Connecting after a disconnect or a rolled back stack keeps reporting the previous
        // attempt's status until CloudFormation calls back, so only a change into an end state
        // tells us anything about this attempt.
        let reached_end_state = poll > 0 && status != prev_status;

        match status {
            Some(Completed) => {
                tracing::info!("AWS connection successfully verified");
                return Ok(());
            },
            Some(Failed) if reached_end_state => {
                return Err(anyhow!(
                    "Connecting AWS failed.{}",
                    cloudformation_hint(connection.as_ref())
                )
                .into());
            },
            Some(Deleted) if reached_end_state => {
                return Err(
                    anyhow!("The AWS connection was removed while we were connecting it.").into(),
                );
            },
            _ => {},
        }

        prev_status = status;
    }

    Err(anyhow!(
        "Verifying the AWS connection has timed out.{}",
        cloudformation_hint(connection.as_ref())
    )
    .into())
}

/// Point the user at the CloudFormation stack, by link when we know where it is.
fn cloudformation_hint(connection: Option<&WorkspaceAwsConnectionModel>) -> String {
    let mut hint =
        " Check the status of the deployment in your AWS CloudFormation dashboard".to_string();

    if let Some(url) = connection.and_then(|c| c.console_url.as_deref()) {
        hint.push_str(" or by following this link: ");
        hint.push_str(url);
    }

    hint
}

pub async fn connect(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: String,
    verify: bool,
) -> ApiResult<()> {
    let workspace = get_workspace_by_name(client, organization_name, workspace_name).await?;

    if is_connected(client, workspace.id).await? {
        println!(
            "Workspace '{}' is already connected to AWS.\n\
             Run `pc workspace aws disconnect -w {}` first if you want to reconnect it.",
            workspace.name, workspace.name
        );
        return Ok(());
    }

    connect_by_id(client, workspace.id, verify).await
}

pub async fn connect_by_id(client: &Client, workspace_id: Uuid, verify: bool) -> ApiResult<()> {
    let url = client.get_aws_workspace_setup_url(workspace_id).await?;

    println!(
        r"Please complete the AWS connection setup process in your browser.
It may take up to 5 minutes to complete after clicking 'Create stack'.
If your browser did not open automatically, please go to the following URL:
{}",
        url.full_setup_url
    );

    open_browser(&url.full_setup_url);

    if verify {
        wait_until_connected(client, workspace_id, POLL_INTERVAL_SECS, POLL_TIMEOUT_SECS).await?;
        println!("Workspace is successfully connected to AWS.");
    }

    Ok(())
}

fn open_browser(url: &str) {
    // Launching a real browser from a test run is never what we want.
    if cfg!(test) {
        return;
    }

    if let Err(error) = webbrowser::open(url) {
        tracing::warn!(
            error = &error as &dyn std::error::Error,
            "Could not open browser"
        );
    }
}

pub async fn disconnect(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: String,
) -> ApiResult<()> {
    let workspace = get_workspace_by_name(client, organization_name, workspace_name).await?;

    if workspace_aws::connection(client, workspace.id)
        .await?
        .is_none()
    {
        println!(
            "Workspace '{}' has no AWS account connected, so there is nothing to disconnect.",
            workspace.name
        );
        return Ok(());
    }

    let stack = client.delete_workspace_aws_connection(workspace.id).await?;

    println!(
        "To finish removing the AWS connection, delete the {} CloudFormation stack in AWS.\n\
         {}",
        stack.stack_name, stack.url
    );

    Ok(())
}

/// Report where the workspace's AWS connection stands, without waiting on it.
pub async fn verify(
    client: &Client,
    organization_name: Option<String>,
    workspace_name: String,
) -> ApiResult<()> {
    use AwsConnectionStatusModel::{Completed, Deleted, Failed, Pending};

    let workspace = get_workspace_by_name(client, organization_name, workspace_name).await?;
    let name = &workspace.name;

    let Some(connection) = workspace_aws::connection(client, workspace.id).await? else {
        return Err(anyhow!(
            "Workspace '{name}' has no AWS account connected.\n\
             Run `pc workspace aws connect -w {name}` to connect one."
        )
        .into());
    };

    match connection.status {
        Completed => {
            println!("Workspace '{name}' is connected to AWS.");
            Ok(())
        },
        Pending => Err(anyhow!(
            "Workspace '{name}' is still connecting to AWS. Check the CloudFormation setup in \
             your browser, then run this again.{}",
            cloudformation_hint(Some(&connection))
        )
        .into()),
        Failed => Err(anyhow!(
            "The AWS connection for workspace '{name}' failed.{}",
            cloudformation_hint(Some(&connection))
        )
        .into()),
        Deleted => Err(anyhow!(
            "The AWS connection for workspace '{name}' was removed.\n\
             Run `pc workspace aws connect -w {name}` to connect an account again."
        )
        .into()),
    }
}

/// Refuse to delete a workspace whose CloudFormation stack has not reported back yet.
pub async fn ensure_deletable(client: &Client, workspace_id: Uuid) -> ApiResult<()> {
    let status = workspace_aws::connection(client, workspace_id)
        .await?
        .map(|connection| connection.status);

    if status == Some(AwsConnectionStatusModel::Pending) {
        return Err(anyhow!(
            "This workspace is still setting up its AWS connection. Wait for it to finish, then \
             run `pc workspace aws disconnect` before deleting the workspace."
        )
        .into());
    }

    Ok(())
}

/// Add the way out to the neutral delete endpoint's refusal, keeping the server's own wording.
pub fn explain_delete_conflict(error: ApiError) -> ApiError {
    if error.status() != Some(StatusCode::BAD_REQUEST) {
        return error;
    }

    anyhow!(
        "{error}\n\nRun `pc workspace aws disconnect` and delete the returned CloudFormation \
         stack in AWS first, then delete the workspace."
    )
    .into()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use AwsConnectionStatusModel::{Completed, Deleted, Failed, Pending};
    use client_core::MockControlPlaneClient;

    use super::*;
    use crate::test_fixtures::{aws_connection, status_error, workspace};

    /// A client whose connection route walks `statuses`, repeating the last one once exhausted.
    fn client_returning(statuses: Vec<AwsConnectionStatusModel>) -> Client {
        let mut mock = MockControlPlaneClient::new();
        let mut poll = 0;
        mock.expect_get_aws_connection().returning(move |id| {
            let status = statuses[poll.min(statuses.len() - 1)].clone();
            poll += 1;
            Ok(aws_connection(id, status))
        });
        Arc::new(mock)
    }

    /// A client whose connection route always answers with `status`.
    fn client_with(status: AwsConnectionStatusModel) -> Client {
        client_returning(vec![status])
    }

    /// A client whose connection route always fails with `status`.
    fn client_failing_with(status: StatusCode) -> Client {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_aws_connection()
            .returning(move |_| Err(status_error(status)));
        Arc::new(mock)
    }

    #[tokio::test]
    async fn is_connected_reports_true_when_completed() {
        assert!(
            is_connected(&client_with(Completed), Uuid::now_v7())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn is_connected_reports_false_when_pending() {
        assert!(
            !is_connected(&client_with(Pending), Uuid::now_v7())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn is_connected_maps_not_found_to_false() {
        let client = client_failing_with(StatusCode::NOT_FOUND);

        assert!(!is_connected(&client, Uuid::now_v7()).await.unwrap());
    }

    #[tokio::test]
    async fn is_connected_propagates_other_errors() {
        let client = client_failing_with(StatusCode::INTERNAL_SERVER_ERROR);

        let error = is_connected(&client, Uuid::now_v7()).await.unwrap_err();
        assert_eq!(error.status(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    }

    #[tokio::test(start_paused = true)]
    async fn wait_until_connected_returns_once_completed() {
        let client = client_with(Completed);

        wait_until_connected(&client, Uuid::now_v7(), 2, 300)
            .await
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn wait_until_connected_gives_up_on_a_stack_that_rolled_back() {
        // The stack is still deploying, then it rolls back.
        let client = client_returning(vec![Pending, Failed]);

        let error = wait_until_connected(&client, Uuid::now_v7(), 2, 300)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Connecting AWS failed"));
        // The console URL comes off the poll we already made, not a second request.
        assert!(error.to_string().contains("example.invalid/console"));
    }

    #[tokio::test(start_paused = true)]
    async fn wait_until_connected_keeps_polling_through_a_stale_end_state() {
        // A reconnect keeps reporting the previous attempt's status until CloudFormation
        // calls back, so an end state that was already there is not this attempt's answer.
        let client = client_returning(vec![Deleted, Deleted, Completed]);

        wait_until_connected(&client, Uuid::now_v7(), 2, 300)
            .await
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn wait_until_connected_times_out_while_pending() {
        let client = client_with(Pending);

        let error = wait_until_connected(&client, Uuid::now_v7(), 2, 4)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));
    }

    /// `verify` answers about the connection as it stands. It used to poll for a change, so a
    /// connection resting in an end state burned the whole timeout and then blamed a timeout.
    #[tokio::test]
    async fn verify_reports_the_status_it_finds() {
        for (status, expected) in [
            (Completed, "is connected to AWS"),
            (Pending, "still connecting"),
            (Failed, "failed"),
            (Deleted, "was removed"),
        ] {
            let mut mock = MockControlPlaneClient::new();
            mock.expect_get_workspaces()
                .returning(|_, _| Ok(vec![workspace("ws", Uuid::now_v7())]));
            let returned = status.clone();
            mock.expect_get_aws_connection()
                .returning(move |id| Ok(aws_connection(id, returned.clone())));
            let client: Client = Arc::new(mock);

            let result = verify(&client, None, "ws".into()).await;

            match result {
                Ok(()) => assert_eq!(status, Completed),
                Err(error) => assert!(
                    error.to_string().contains(expected),
                    "{status:?} said {error}"
                ),
            }
        }
    }

    #[tokio::test]
    async fn connect_refuses_a_workspace_that_is_already_connected() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_workspaces()
            .returning(|_, _| Ok(vec![workspace("ws", Uuid::now_v7())]));
        mock.expect_get_aws_connection()
            .returning(|id| Ok(aws_connection(id, Completed)));
        // The whole point: no second CloudFormation stack.
        mock.expect_get_aws_workspace_setup_url().never();
        let client: Client = Arc::new(mock);

        connect(&client, None, "ws".into(), true).await.unwrap();
    }

    #[tokio::test]
    async fn disconnect_says_so_when_aws_was_never_connected() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_workspaces()
            .returning(|_, _| Ok(vec![workspace("ws", Uuid::now_v7())]));
        mock.expect_get_aws_connection()
            .returning(|_| Err(status_error(StatusCode::NOT_FOUND)));
        mock.expect_delete_workspace_aws_connection().never();
        let client: Client = Arc::new(mock);

        disconnect(&client, None, "ws".into()).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn verify_fails_immediately_when_aws_was_never_connected() {
        let mut mock = MockControlPlaneClient::new();
        mock.expect_get_workspaces()
            .returning(|_, _| Ok(vec![workspace("ws", Uuid::now_v7())]));
        mock.expect_get_aws_connection()
            .returning(|_| Err(status_error(StatusCode::NOT_FOUND)));
        let client: Client = Arc::new(mock);

        let error = verify(&client, None, "ws".into()).await.unwrap_err();
        assert!(error.to_string().contains("no AWS account connected"));
    }

    #[tokio::test]
    async fn ensure_deletable_refuses_while_pending() {
        let client = client_with(Pending);

        let error = ensure_deletable(&client, Uuid::now_v7()).await.unwrap_err();
        assert!(error.to_string().contains("still setting up"));
    }

    #[tokio::test]
    async fn ensure_deletable_allows_a_workspace_with_no_connection() {
        let client = client_failing_with(StatusCode::NOT_FOUND);

        ensure_deletable(&client, Uuid::now_v7()).await.unwrap();
    }

    #[test]
    fn explain_delete_conflict_rewrites_bad_request_only() {
        let rewritten = explain_delete_conflict(status_error(StatusCode::BAD_REQUEST));
        assert!(
            rewritten
                .to_string()
                .contains("pc workspace aws disconnect")
        );
        // The server's own wording survives alongside the hint.
        assert!(rewritten.to_string().contains("400"));

        let untouched = explain_delete_conflict(status_error(StatusCode::NOT_FOUND));
        assert_eq!(untouched.status(), Some(StatusCode::NOT_FOUND));
    }
}
