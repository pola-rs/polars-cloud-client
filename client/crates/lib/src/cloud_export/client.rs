use std::sync::Arc;

use anyhow::Context;
use chrono::{DateTime, Utc};
use client_core::{AutoRefreshApiControlPlaneClient, ControlPlaneClient};
use pc_observatory_models::convert::{
    description_to_ir_visualization, description_to_physical_visualization,
};
use polars_axum_models::{
    QueryFailedArgs, QueryObservedPlanArgs, QueryPhysNodeMetricsModel, QueryStartedArgs,
    QueryUpdateArgs,
};
use uuid::Uuid;

use crate::CTRL_PLN_CLIENT_GLOBAL;
use crate::cloud_export::QueryId;

#[derive(Clone)]
pub(crate) struct CloudApiClient {
    client: Arc<AutoRefreshApiControlPlaneClient>,
    workspace_id: Uuid,
}

impl CloudApiClient {
    pub async fn connect() -> anyhow::Result<Self> {
        let client = CTRL_PLN_CLIENT_GLOBAL.clone();

        let user = client
            .get_logged_in_user()
            .await
            .map_err(|e| anyhow::anyhow!("failed to resolve user for cloud export: {e:#}"))?;

        let workspace_id = user.default_workspace_id.ok_or_else(|| {
            anyhow::anyhow!(
                "no default workspace set; set a default workspace to enable cloud export"
            )
        })?;

        Ok(Self {
            client,
            workspace_id,
        })
    }

    pub async fn submit_started(
        &self,
        query_id: QueryId,
        timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.client
            .observe_query_started(self.workspace_id, query_id, QueryStartedArgs { timestamp })
            .await
            .map_err(|e| anyhow::anyhow!("failed to register query: {e:#}"))
    }

    pub async fn submit_plan(
        &self,
        query_id: QueryId,
        timestamp: DateTime<Utc>,
        ir_plan: &[u8],
        physical_plan: &Option<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let ir_plan = try_serialize_to_ir_plan_visualization(ir_plan).unwrap_or_else(|error| {
            tracing::warn!(?query_id, ?error, "failed to serialize IR plan");
            None
        });
        let phys_plan = physical_plan.as_deref().and_then(|physical_plan| {
            try_serialize_to_phys_plan_visualization(physical_plan).unwrap_or_else(|error| {
                tracing::warn!(?query_id, ?error, "failed to serialize physical plan");
                None
            })
        });

        let update = QueryUpdateArgs {
            timestamp,
            plan: Some(QueryObservedPlanArgs { ir_plan, phys_plan }),
            metrics: None,
            is_final: false,
        };

        self.client
            .observe_query_update(self.workspace_id, query_id, update)
            .await
            .map_err(|e| anyhow::anyhow!("failed to submit plan: {e:#}"))
    }

    pub async fn submit_metrics(
        &self,
        query_id: QueryId,
        timestamp: DateTime<Utc>,
        metrics: Vec<QueryPhysNodeMetricsModel>,
        is_final: bool,
    ) -> anyhow::Result<()> {
        let update = QueryUpdateArgs {
            timestamp,
            plan: None,
            metrics: Some(metrics),
            is_final,
        };

        self.client
            .observe_query_update(self.workspace_id, query_id, update)
            .await
            .map_err(|e| anyhow::anyhow!("failed to submit metrics: {e:#}"))
    }

    pub async fn submit_failed(
        &self,
        query_id: QueryId,
        timestamp: DateTime<Utc>,
        error: String,
    ) -> anyhow::Result<()> {
        self.client
            .observe_query_failed(
                self.workspace_id,
                query_id,
                QueryFailedArgs { timestamp, error },
            )
            .await
            .map_err(|e| anyhow::anyhow!("failed to mark query failed: {e:#}"))
    }
}

fn try_serialize_to_ir_plan_visualization(
    ir_plan_description: &[u8],
) -> anyhow::Result<Option<serde_json::Value>> {
    let ir_nodes = rmp_serde::from_slice(ir_plan_description).context("failed to parse IR plan")?;
    let json = serde_json::to_value(description_to_ir_visualization(ir_nodes))
        .context("failed to serialize IR plan")?;
    Ok(Some(json))
}

fn try_serialize_to_phys_plan_visualization(
    phys_plan_description: &[u8],
) -> anyhow::Result<Option<serde_json::Value>> {
    let Some(phys_nodes) =
        rmp_serde::from_slice(phys_plan_description).context("failed to parse physical plan")?
    else {
        return Ok(None);
    };
    let json = serde_json::to_value(description_to_physical_visualization(phys_nodes))
        .context("failed to serialize physical plan")?;
    Ok(Some(json))
}
