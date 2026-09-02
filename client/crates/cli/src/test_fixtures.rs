use chrono::{DateTime, Utc};
use client_core::ApiError;
use polars_axum_models::{
    AwsConnectionStatusModel, OrganizationModel, OrganizationSubscriptionStateModel,
    OrganizationTierModel, WorkspaceAwsConnectionModel, WorkspaceDeploymentModel, WorkspaceModel,
    WorkspaceStateModel,
};
use reqwest::StatusCode;
use uuid::Uuid;

pub fn workspace(name: &str, organization_id: Uuid) -> WorkspaceModel {
    WorkspaceModel {
        id: Uuid::now_v7(),
        organization_id,
        name: name.into(),
        description: String::new(),
        deployment: WorkspaceDeploymentModel::Aws,
        creator_id: Uuid::now_v7(),
        status: WorkspaceStateModel::Active,
        idle_timeout_mins: 10,
        created_at: DateTime::<Utc>::default(),
        updated_at: DateTime::<Utc>::default(),
        deleted_at: None,
    }
}

pub fn organization(name: &str) -> OrganizationModel {
    OrganizationModel {
        id: Uuid::now_v7(),
        name: name.into(),
        description: String::new(),
        avatar_url: None,
        creator_id: Uuid::now_v7(),
        subscription_state: OrganizationSubscriptionStateModel::Subscribed,
        trial_started_at: None,
        trial_expires_at: None,
        tier: OrganizationTierModel::PayAsYouGo,
        created_at: DateTime::<Utc>::default(),
        updated_at: DateTime::<Utc>::default(),
        deleted_at: None,
    }
}

pub fn status_error(status: StatusCode) -> ApiError {
    ApiError::StatusError {
        status,
        url: "https://api.cloud.pola.rs/api/v1/workspace"
            .parse()
            .unwrap(),
        body: String::new(),
    }
}

pub fn aws_connection(
    workspace_id: Uuid,
    status: AwsConnectionStatusModel,
) -> WorkspaceAwsConnectionModel {
    WorkspaceAwsConnectionModel {
        workspace_id,
        status,
        console_url: Some("https://example.invalid/console".into()),
        stack_name: None,
        region: None,
        account_id: None,
        worker_role_arn: None,
        cfn_template_version: None,
        latest_cfn_template_version: "1".into(),
    }
}

pub fn setup_url() -> polars_axum_models::WorkspaceSetupUrlModel {
    polars_axum_models::WorkspaceSetupUrlModel {
        full_setup_url: "https://example.invalid/setup".into(),
        barebones_setup_url: "https://example.invalid/barebones".into(),
        full_template_url: "https://example.invalid/template".into(),
        barebones_template_url: "https://example.invalid/barebones-template".into(),
    }
}
