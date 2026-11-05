use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use client_core::RUNTIME;
use polars_axum_models::{
    QueryCustomMetricModel, QueryMetricUnitModel, QueryPhysNodeKind, QueryPhysNodeMetricsModel,
};
use polars_descriptions::{
    CustomMetricDescription, MetricUnit, MetricsSnapshotDescription, NodeMetricsDescription,
};
use pyo3::{Py, PyAny, PyResult, Python, pyclass, pymethods};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

use crate::cloud_export::{QueryId, QueryStateMessage};

pub(crate) fn init_metric_poller(
    sender: Sender<QueryStateMessage>,
    query_id: QueryId,
    metric_handle: Py<PyAny>,
    node_kind_lookup: Option<HashMap<u64, QueryPhysNodeKind>>,
    poll_interval: Duration,
) -> QueryMetricPoller {
    let cancel = CancellationToken::new();

    let cancel_task = cancel.clone();
    let task = RUNTIME.0.spawn(async move {
        let mut ticker = tokio::time::interval(poll_interval);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = cancel_task.cancelled() => break,
                _ = ticker.tick() => sample_and_submit(query_id, &metric_handle, &sender, &node_kind_lookup, false).await,
            }
        }

        sample_and_submit(query_id, &metric_handle, &sender, &node_kind_lookup, true).await;
    });

    QueryMetricPoller {
        query_id,
        poller: MetricPoller::new(task, cancel),
        closed: false,
    }
}

#[pyclass]
pub struct QueryMetricPoller {
    query_id: QueryId,
    poller: MetricPoller,
    closed: bool,
}

#[pymethods]
impl QueryMetricPoller {
    pub fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        self.poller.cancel.cancel();
        tracing::info!(query_id = ?self.query_id, "close guard called, finishing query");
        if let Some(task) = self.poller.task.take() {
            py.detach(|| {
                let _ = RUNTIME.0.block_on(task);
            });
        }
        Ok(())
    }
}

impl Drop for QueryMetricPoller {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        let _ = Python::attach(|py| self.close(py));
    }
}

async fn sample_and_submit(
    query_id: QueryId,
    handle: &Py<PyAny>,
    sender: &Sender<QueryStateMessage>,
    node_kind_lookup: &Option<HashMap<u64, QueryPhysNodeKind>>,
    is_final: bool,
) {
    let bytes = match Python::attach(|py| -> PyResult<Vec<u8>> {
        handle
            .call_method0(py, "snapshot_query_metrics")?
            .extract(py)
    }) {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::warn!(?error, "failed to sample query metrics");
            return;
        },
    };

    let rows = match decode_snapshot(&bytes) {
        Ok(snapshot) => snapshot.nodes,
        Err(error) => {
            tracing::warn!(?error, "failed to deserialize query metrics");
            return;
        },
    };

    if rows.is_empty() {
        return;
    }

    let Some(node_kind_lookup) = node_kind_lookup else {
        // Currently we only support 'Streaming' engine, which means we will always get metrics + phys_plan
        tracing::debug!("cannot deserialize metrics correctly without phys plan");
        return;
    };

    let metrics = rows
        .iter()
        .map(|n| {
            let node_kind = *node_kind_lookup
                .get(&n.phys_node_key)
                .expect("metrics contained phys_node_key which was not in the plan");
            node_phys_metrics_model(n, node_kind)
        })
        .collect();

    let message = QueryStateMessage::Metrics {
        query_id,
        now: Utc::now(),
        metrics,
        is_final,
        ack: None,
    };

    let result: anyhow::Result<()> = if is_final {
        // Await until final message has actually been submitted
        let (message, ack_rx) = message.with_ack();
        match sender.send(message).await {
            Ok(()) => ack_rx.await.map_err(Into::into),
            Err(error) => Err(error.into()),
        }
    } else {
        sender.try_send(message).map_err(Into::into)
    };

    if let Err(error) = result {
        tracing::warn!(?error, "failed to submit query metrics");
    }
}

/// Polars before pola-rs/polars#29792 sends only the node rows, as a list. Later versions send the
/// whole snapshot, as a map. The marker tells them apart: a list of rows must not be read as the
/// snapshot struct, field by field, which turns one bad row into an empty snapshot.
fn decode_snapshot(bytes: &[u8]) -> Result<MetricsSnapshotDescription, rmp_serde::decode::Error> {
    if is_msgpack_array(bytes) {
        Ok(MetricsSnapshotDescription {
            nodes: rmp_serde::from_slice(bytes)?,
            ..Default::default()
        })
    } else {
        rmp_serde::from_slice(bytes)
    }
}

/// Whether the payload starts with a msgpack array marker: fixarray, array 16 or array 32.
fn is_msgpack_array(bytes: &[u8]) -> bool {
    matches!(bytes.first(), Some(0x90..=0x9f | 0xdc | 0xdd))
}

pub(crate) struct MetricPoller {
    cancel: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl MetricPoller {
    pub(crate) fn new(task: tokio::task::JoinHandle<()>, cancel: CancellationToken) -> Self {
        Self {
            cancel,
            task: Some(task),
        }
    }
}

pub(crate) fn node_phys_metrics_model(
    m: &NodeMetricsDescription,
    node_kind: QueryPhysNodeKind,
) -> QueryPhysNodeMetricsModel {
    QueryPhysNodeMetricsModel {
        phys_node_key: m.phys_node_key,
        query_phys_node_kind: node_kind,
        total_polls: m.total_polls,
        total_stolen_polls: m.total_stolen_polls,
        total_poll_time_ns: m.total_poll_time_ns,
        max_poll_time_ns: m.max_poll_time_ns,
        total_state_updates: m.total_state_updates,
        total_state_update_time_ns: m.total_state_update_time_ns,
        max_state_update_time_ns: m.max_state_update_time_ns,
        morsels_sent: m.morsels_sent,
        rows_sent: m.rows_sent,
        largest_morsel_sent: m.largest_morsel_sent,
        morsels_received: m.morsels_received,
        rows_received: m.rows_received,
        largest_morsel_received: m.largest_morsel_received,
        io_total_active_ns: m.io_total_active_ns,
        io_total_bytes_requested: m.io_total_bytes_requested,
        io_total_bytes_received: m.io_total_bytes_received,
        io_total_bytes_sent: m.io_total_bytes_sent,
        total_time_ns: m.total_time_ns,
        custom: m.custom.iter().filter_map(IntoModel::into_model).collect(),
        done: m.done,
    }
}

trait IntoModel<T> {
    fn into_model(self) -> T;
}

impl IntoModel<Option<QueryCustomMetricModel>> for &CustomMetricDescription {
    fn into_model(self) -> Option<QueryCustomMetricModel> {
        Some(QueryCustomMetricModel {
            key: self.key.clone(),
            unit: self.unit.into_model(),
            value: self.value?,
        })
    }
}

impl IntoModel<QueryMetricUnitModel> for MetricUnit {
    fn into_model(self) -> QueryMetricUnitModel {
        match self {
            MetricUnit::Unit => QueryMetricUnitModel::Unit,
            MetricUnit::Bytes => QueryMetricUnitModel::Bytes,
            MetricUnit::DurationNs => QueryMetricUnitModel::DurationNs,
        }
    }
}

#[cfg(test)]
mod tests {
    use polars_descriptions::QueryMetricsDescription;

    use super::*;

    fn node(phys_node_key: u64) -> NodeMetricsDescription {
        NodeMetricsDescription {
            phys_node_key,
            rows_sent: 10,
            ..Default::default()
        }
    }

    fn keys(snapshot: &MetricsSnapshotDescription) -> Vec<u64> {
        snapshot.nodes.iter().map(|n| n.phys_node_key).collect()
    }

    #[test]
    fn the_whole_snapshot_decodes() {
        let snapshot = MetricsSnapshotDescription {
            query: QueryMetricsDescription {
                io_rx_active_ns: 5,
                ..Default::default()
            },
            nodes: vec![node(1), node(2)],
        };
        let bytes = rmp_serde::to_vec_named(&snapshot).unwrap();

        let decoded = decode_snapshot(&bytes).unwrap();

        assert_eq!(keys(&decoded), [1, 2]);
        assert_eq!(decoded.query.io_rx_active_ns, 5);
        assert_eq!(decoded.nodes[0].rows_sent, 10);
    }

    #[test]
    fn the_node_rows_of_an_older_polars_decode() {
        // One row would also decode as the snapshot struct, field by field.
        for rows in [vec![node(1)], vec![node(1), node(2)], vec![]] {
            let bytes = rmp_serde::to_vec_named(&rows).unwrap();

            let decoded = decode_snapshot(&bytes).unwrap();

            assert_eq!(
                keys(&decoded),
                rows.iter().map(|n| n.phys_node_key).collect::<Vec<_>>()
            );
            assert_eq!(decoded.query.io_rx_active_ns, 0);
        }
    }

    #[test]
    fn a_payload_that_is_neither_is_an_error() {
        let bytes = rmp_serde::to_vec_named(&"not metrics").unwrap();

        assert!(decode_snapshot(&bytes).is_err());
    }

    #[test]
    fn a_bad_node_row_reports_its_own_error() {
        // One row, which would decode as an empty snapshot, field by field.
        let rows = vec![HashMap::from([("phys_node_key", "not a key")])];
        let bytes = rmp_serde::to_vec_named(&rows).unwrap();

        let error = decode_snapshot(&bytes).unwrap_err().to_string();

        let rows_error = rmp_serde::from_slice::<Vec<NodeMetricsDescription>>(&bytes).unwrap_err();
        assert_eq!(error, rows_error.to_string());
    }

    #[test]
    fn a_bad_snapshot_reports_its_own_error() {
        let snapshot = HashMap::from([("query", "not metrics")]);
        let bytes = rmp_serde::to_vec_named(&snapshot).unwrap();

        let error = decode_snapshot(&bytes).unwrap_err().to_string();

        assert!(error.contains("not metrics"), "{error}");
    }
}
