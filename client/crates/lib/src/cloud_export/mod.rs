mod client;
mod metrics;
mod observer;

use chrono::{DateTime, Utc};
pub use metrics::QueryMetricPoller;
pub use observer::QueryCloudObserver;
use polars_axum_models::QueryPhysNodeMetricsModel;
use strum_macros::IntoStaticStr;
use tokio::sync::oneshot;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

type QueryId = Uuid;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .try_init();
}

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
