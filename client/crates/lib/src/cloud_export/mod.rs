mod client;
mod metrics;
mod observer;

use chrono::{DateTime, Utc};
pub use metrics::QueryMetricPoller;
pub use observer::QueryCloudObserver;
use polars_axum_models::QueryPhysNodeMetricsModel;
use strum_macros::IntoStaticStr;
use tokio::sync::oneshot;
use uuid::Uuid;

type QueryId = Uuid;

#[derive(IntoStaticStr)]
enum QueryStateMessage {
    Started {
        query_id: QueryId,
        now: DateTime<Utc>,
    },
    Planned {
        query_id: QueryId,
        now: DateTime<Utc>,
        ir_plan: Vec<u8>,
        physical_plan: Option<Vec<u8>>,
    },
    Failed {
        query_id: QueryId,
        now: DateTime<Utc>,
        err: String,
    },
    Metrics {
        query_id: QueryId,
        now: DateTime<Utc>,
        metrics: Vec<QueryPhysNodeMetricsModel>,
        is_final: bool,
        ack: Option<oneshot::Sender<()>>,
    },
}

impl QueryStateMessage {
    fn with_ack(mut self) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        if let QueryStateMessage::Metrics { ack, .. } = &mut self {
            *ack = Some(tx);
        }
        (self, rx)
    }
}
