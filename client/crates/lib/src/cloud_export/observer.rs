use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use client_core::RUNTIME;
use polars_axum_models::QueryPhysNodeKind;
use polars_descriptions::{PhysicalNodeDescription, PhysicalPropsDescription};
use pyo3::exceptions::PyRuntimeError;
use pyo3::{Py, PyAny, PyResult, Python, pyclass, pymethods};
use tokio::sync::mpsc;

use crate::cloud_export::client::CloudApiClient;
use crate::cloud_export::metrics::{QueryMetricPoller, init_metric_poller};
use crate::cloud_export::{QueryId, QueryStateMessage, init_tracing};

const METRICS_POLL_INTERVAL: Duration = Duration::from_secs(10);

#[pyclass]
pub struct QueryCloudObserver {
    sender: mpsc::Sender<QueryStateMessage>,
}

#[pymethods]
impl QueryCloudObserver {
    #[new]
    fn new(py: Python<'_>) -> PyResult<Self> {
        py.detach(|| {
            init_tracing();
            tracing::debug!("initializing new QueryCloudObserver");

            let client = RUNTIME
                .0
                .block_on(CloudApiClient::connect())
                .map_err(|error| {
                    PyRuntimeError::new_err(format!("failed to connect to Polars Cloud: {error:#}"))
                })?;

            let (sender, receiver) = mpsc::channel(8);

            let mut handler = QueryCloudObserverHandler::new(receiver);
            RUNTIME.0.spawn(async move {
                while let Some(msg) = handler.receiver.recv().await {
                    let message_type: &'static str = (&msg).into();
                    if let Err(error) = handler.handle_message(&client, msg).await {
                        tracing::warn!(message_type, ?error, "failed to handle message");
                    }
                }
            });

            Ok(Self { sender })
        })
    }

    fn on_query_started(&self, py: Python<'_>, query_id: QueryId) {
        py.detach(|| {
            tracing::debug!(?query_id, "received `on_query_started` hook");
            let _ = self.sender.blocking_send(QueryStateMessage::Started {
                query_id,
                now: Utc::now(),
            });
        });
    }

    fn on_query_planned(
        &self,
        py: Python<'_>,
        query_id: QueryId,
        metric_exporter_handle: Py<PyAny>,
        ir_plan: Vec<u8>,
        physical_plan: Option<Vec<u8>>,
    ) -> QueryMetricPoller {
        let phys_node_lookup = py.detach(|| {
            tracing::debug!(?query_id, "received `on_query_planned` hook");

            let phys_node_lookup = create_phys_lookup_table(physical_plan.as_deref());
            let _ = self.sender.blocking_send(QueryStateMessage::Planned {
                query_id,
                now: Utc::now(),
                ir_plan,
                physical_plan,
            });

            // We keep a copy of the phys_plan so we can resolve the node_kind in the metrics
            phys_node_lookup
        });

        init_metric_poller(
            self.sender.clone(),
            query_id,
            metric_exporter_handle,
            phys_node_lookup,
            METRICS_POLL_INTERVAL,
        )
    }

    fn on_query_failed(&self, py: Python<'_>, query_id: QueryId, err: String) {
        py.detach(|| {
            tracing::debug!(?query_id, "received `on_query_failed` hook");
            let _ = self.sender.blocking_send(QueryStateMessage::Failed {
                query_id,
                now: Utc::now(),
                err,
            });
        });
    }
}

fn create_phys_lookup_table(phys_plan: Option<&[u8]>) -> Option<HashMap<u64, QueryPhysNodeKind>> {
    let phys_plan = phys_plan?;

    let Ok(Some(phys_nodes)) =
        rmp_serde::from_slice::<Option<Vec<PhysicalNodeDescription>>>(phys_plan)
    else {
        return None;
    };

    Some(
        phys_nodes
            .iter()
            .map(|n| {
                use PhysicalPropsDescription::*;
                let node_kind = match n.properties {
                    InMemorySource { .. } | PythonScan { .. } | MultiScan { .. } => {
                        QueryPhysNodeKind::Scan
                    },
                    FileSink { .. }
                    | PartitionSink { .. }
                    | SinkMultiple { .. }
                    | CallbackSink { .. }
                    | InMemorySink => QueryPhysNodeKind::Sink,
                    _ => QueryPhysNodeKind::Other,
                };
                (n.id, node_kind)
            })
            .collect::<HashMap<_, _>>(),
    )
}

struct QueryCloudObserverHandler {
    receiver: mpsc::Receiver<QueryStateMessage>,
}

impl QueryCloudObserverHandler {
    fn new(receiver: mpsc::Receiver<QueryStateMessage>) -> Self {
        Self { receiver }
    }

    async fn handle_message(
        &mut self,
        client: &CloudApiClient,
        message: QueryStateMessage,
    ) -> anyhow::Result<()> {
        match message {
            QueryStateMessage::Started { query_id, now } => {
                client.submit_started(query_id, now).await
            },
            QueryStateMessage::Failed { query_id, now, err } => {
                client.submit_failed(query_id, now, err).await
            },
            QueryStateMessage::Planned {
                query_id,
                now,
                ir_plan,
                physical_plan,
            } => {
                client
                    .submit_plan(query_id, now, &ir_plan, &physical_plan)
                    .await
            },
            QueryStateMessage::Metrics {
                query_id,
                now,
                metrics,
                is_final,
                ack,
            } => {
                let result = client
                    .submit_metrics(query_id, now, metrics, is_final)
                    .await;
                if let Some(ack) = ack {
                    let _ = ack.send(());
                }
                result
            },
        }
    }
}
