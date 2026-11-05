use std::collections::{BTreeSet, HashMap};

use pc_observatory_types::{OriginNodeId, PhysNodeId, StageNumber};
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::Serialize;

use crate::phys::PhysicalPlanVisualizationData;
use crate::physical::AggregatedPhysicalNodeMetricFieldsModel;

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct LogicalMetricsModel {
    pub metrics: HashMap<OriginNodeId, LogicalNodeMetricsModel>,
    pub total_node_time_ns: i64,
}

// Polls, state updates and morsels are left out: they describe how a physical node ran, not what
// the logical node cost. I/O active time is left out because it is wall time, and the physical
// nodes of one logical node can wait on I/O at the same time, so it cannot be summed.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct LogicalNodeMetricsModel {
    pub rows_received: Option<i64>,
    pub rows_sent: Option<i64>,
    pub io_total_bytes_received: i64,
    pub total_time_ns: i64,
    pub done: bool,
    pub stages: Vec<StageNumber>,
}

pub struct StageVariationMetrics<'a> {
    pub stage_number: StageNumber,
    pub plan: &'a PhysicalPlanVisualizationData,
    pub metrics: &'a HashMap<PhysNodeId, AggregatedPhysicalNodeMetricFieldsModel>,
}

pub fn aggregate_logical_metrics(variations: &[StageVariationMetrics<'_>]) -> LogicalMetricsModel {
    let mut attributed: HashMap<OriginNodeId, Accumulator> = HashMap::new();
    let mut total_node_time_ns = 0;

    // Physical node ids repeat across the variations of a stage, so look nodes up within one
    // variation at a time.
    for variation in variations {
        let origin_of: HashMap<u64, Option<u64>> = variation
            .plan
            .nodes
            .iter()
            .map(|n| (n.id, n.origin_node_id))
            .collect();
        let type_of: HashMap<u64, &str> = variation
            .plan
            .nodes
            .iter()
            .map(|n| (n.id, n.type_id.as_str()))
            .collect();
        let metrics_of = |id: u64| variation.metrics.get(&PhysNodeId(id as i64));
        // Edges point from the consumer (`source`) to the producer (`target`). A consumer that
        // reads two ports of one producer appears twice in these lists.
        let mut consumers: HashMap<u64, Vec<u64>> = HashMap::new();
        let mut producers: HashMap<u64, Vec<u64>> = HashMap::new();
        for edge in &variation.plan.edges {
            consumers.entry(edge.target).or_default().push(edge.source);
            producers.entry(edge.source).or_default().push(edge.target);
        }
        // A producer's rows are split evenly over its consumers. Multiplexer ports are the
        // exception: a reader such as a head slice can stop early, so the ports carry different
        // row counts. When the reader has no other input, take what it received instead.
        let edge_rows = |producer: u64, consumer: u64| -> i64 {
            if type_of.get(&producer) == Some(&"Multiplexer")
                && producers.get(&consumer).is_some_and(|p| p.len() == 1)
                && let Some(rows) = metrics_of(consumer).and_then(|m| m.rows_received)
            {
                return rows;
            }
            metrics_of(producer)
                .and_then(|m| m.rows_sent)
                .map_or(0, |rows| rows / consumers[&producer].len() as i64)
        };

        for node in &variation.plan.nodes {
            let Some(metrics) = metrics_of(node.id) else {
                continue;
            };
            total_node_time_ns += metrics.total_time_ns;

            // Plans from older polars clients and stored plans from before origins existed have
            // nodes without one. Their time still counts in the total.
            let Some(origin) = node.origin_node_id else {
                continue;
            };
            let same_origin = |id: &&u64| origin_of.get(*id).copied().flatten() == Some(origin);
            // If every edge on one side stays inside the logical node, no rows cross that side.
            // Leave the count unset rather than 0, so that a logical node nothing flows into,
            // such as a scan, has no `rows_received` and the view shows the bytes it read instead.
            let all_internal = |edges: Option<&Vec<u64>>| {
                edges.is_some_and(|e| e.iter().all(|id| same_origin(&id)))
            };
            let rows_received = metrics
                .rows_received
                .filter(|_| !all_internal(producers.get(&node.id)))
                .map(|rows| {
                    let internal: i64 = producers
                        .get(&node.id)
                        .into_iter()
                        .flatten()
                        .filter(same_origin)
                        .map(|&producer| edge_rows(producer, node.id))
                        .sum();
                    (rows - internal).max(0)
                });
            let rows_sent = metrics
                .rows_sent
                .filter(|_| !all_internal(consumers.get(&node.id)))
                .map(|rows| {
                    let internal: i64 = consumers
                        .get(&node.id)
                        .into_iter()
                        .flatten()
                        .filter(same_origin)
                        .map(|&consumer| edge_rows(node.id, consumer))
                        .sum();
                    (rows - internal).max(0)
                });

            attributed.entry(OriginNodeId(origin)).or_default().merge(
                metrics,
                variation.stage_number,
                rows_sent,
                rows_received,
            );
        }
    }

    LogicalMetricsModel {
        metrics: attributed
            .into_iter()
            .map(|(id, acc)| (id, acc.finish()))
            .collect(),
        total_node_time_ns,
    }
}

struct Accumulator {
    metrics: LogicalNodeMetricsModel,
    stages: BTreeSet<u32>,
}

impl Default for Accumulator {
    fn default() -> Self {
        // `done` starts true: a logical node with no physical nodes yet has nothing left to
        // finish.
        Self {
            metrics: LogicalNodeMetricsModel {
                rows_received: None,
                rows_sent: None,
                io_total_bytes_received: 0,
                total_time_ns: 0,
                done: true,
                stages: Vec::new(),
            },
            stages: BTreeSet::new(),
        }
    }
}

impl Accumulator {
    fn merge(
        &mut self,
        m: &AggregatedPhysicalNodeMetricFieldsModel,
        stage: StageNumber,
        rows_sent: Option<i64>,
        rows_received: Option<i64>,
    ) {
        let acc = &mut self.metrics;
        acc.io_total_bytes_received += m.io_total_bytes_received;
        acc.total_time_ns += m.total_time_ns;
        acc.done &= m.done;
        if let Some(rows) = rows_sent {
            *acc.rows_sent.get_or_insert(0) += rows;
        }
        if let Some(rows) = rows_received {
            *acc.rows_received.get_or_insert(0) += rows;
        }
        self.stages.insert(stage.0);
    }

    fn finish(self) -> LogicalNodeMetricsModel {
        LogicalNodeMetricsModel {
            stages: self.stages.into_iter().map(StageNumber).collect(),
            ..self.metrics
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use pc_observatory_types::{OriginNodeId, PhysNodeId, StageNumber};

    use super::*;
    use crate::Edge;
    use crate::phys::models::PhysNodeProperties;
    use crate::phys::{PhysNodeInfo, PhysicalPlanVisualizationData};

    fn node(id: u64, logical: Option<u64>) -> PhysNodeInfo {
        typed_node(id, logical, "Test")
    }

    fn typed_node(id: u64, logical: Option<u64>, type_id: &str) -> PhysNodeInfo {
        PhysNodeInfo {
            id,
            type_id: type_id.into(),
            origin_node_id: logical,
            title: None,
            subtitle: None,
            properties: PhysNodeProperties::Default,
            warnings: vec![],
        }
    }

    fn plan(nodes: Vec<PhysNodeInfo>, edges: &[(u64, u64)]) -> PhysicalPlanVisualizationData {
        PhysicalPlanVisualizationData {
            title: String::new(),
            num_roots: 1,
            nodes,
            edges: edges
                .iter()
                .map(|&(consumer, producer)| Edge {
                    source: consumer,
                    target: producer,
                })
                .collect(),
        }
    }

    fn metric(
        time: i64,
        sent: Option<i64>,
        received: Option<i64>,
    ) -> AggregatedPhysicalNodeMetricFieldsModel {
        let mut m = AggregatedPhysicalNodeMetricFieldsModel {
            total_polls: 0,
            total_stolen_polls: 0,
            total_poll_time_ns: 0,
            max_poll_time_ns: 0,
            total_state_updates: 0,
            total_state_update_time_ns: 0,
            max_state_update_time_ns: 0,
            morsels_sent: 0,
            rows_sent: None,
            largest_morsel_sent: 0,
            morsels_received: 0,
            rows_received: None,
            largest_morsel_received: 0,
            io_total_active_ns: 0,
            io_total_bytes_requested: 0,
            io_total_bytes_received: 0,
            io_total_bytes_sent: 0,
            total_time_ns: 0,
            custom: Vec::new(),
            done: true,
        };
        m.total_time_ns = time;
        m.rows_sent = sent;
        m.rows_received = received;
        m
    }

    fn metrics(
        rows: &[(u64, AggregatedPhysicalNodeMetricFieldsModel)],
    ) -> HashMap<PhysNodeId, AggregatedPhysicalNodeMetricFieldsModel> {
        rows.iter()
            .map(|(id, m)| (PhysNodeId(*id as i64), m.clone()))
            .collect()
    }

    #[test]
    fn a_chain_sums_time_and_counts_rows_only_at_its_boundaries() {
        let plan = plan(
            vec![node(1, Some(10)), node(2, Some(10)), node(3, Some(20))],
            &[(2, 1), (3, 2)],
        );
        let metrics = metrics(&[
            (1, metric(5, Some(100), Some(100))),
            (2, metric(7, Some(90), Some(100))),
            (3, metric(1, Some(90), Some(90))),
        ]);

        let out = aggregate_logical_metrics(&[StageVariationMetrics {
            stage_number: StageNumber(0),
            plan: &plan,
            metrics: &metrics,
        }]);

        let ten = &out.metrics[&OriginNodeId(10)];
        assert_eq!(ten.total_time_ns, 12);
        assert_eq!(ten.rows_received, Some(100), "node 1 has no producer");
        assert_eq!(ten.rows_sent, Some(90), "node 2 feeds another logical node");
        let twenty = &out.metrics[&OriginNodeId(20)];
        assert_eq!(twenty.rows_received, Some(90));
        assert_eq!(twenty.rows_sent, Some(90), "node 3 has no consumer");
        assert_eq!(out.total_node_time_ns, 13);
    }

    #[test]
    fn two_variations_of_one_stage_sum_without_colliding_physical_ids() {
        let first = plan(vec![node(1, Some(10))], &[]);
        let second = plan(vec![node(1, Some(10))], &[]);
        let first_metrics = metrics(&[(1, metric(3, None, None))]);
        let second_metrics = metrics(&[(1, metric(4, None, None))]);

        let out = aggregate_logical_metrics(&[
            StageVariationMetrics {
                stage_number: StageNumber(0),
                plan: &first,
                metrics: &first_metrics,
            },
            StageVariationMetrics {
                stage_number: StageNumber(0),
                plan: &second,
                metrics: &second_metrics,
            },
        ]);

        let ten = &out.metrics[&OriginNodeId(10)];
        assert_eq!(ten.total_time_ns, 7);
        assert_eq!(ten.stages, vec![StageNumber(0)]);
    }

    #[test]
    fn a_node_without_an_origin_counts_only_toward_the_total() {
        let plan = plan(vec![node(1, Some(10)), node(2, None)], &[(2, 1)]);
        let metrics = metrics(&[(1, metric(3, None, None)), (2, metric(4, None, None))]);

        let out = aggregate_logical_metrics(&[StageVariationMetrics {
            stage_number: StageNumber(1),
            plan: &plan,
            metrics: &metrics,
        }]);

        assert_eq!(out.metrics.len(), 1);
        assert_eq!(out.metrics[&OriginNodeId(10)].total_time_ns, 3);
        assert_eq!(out.total_node_time_ns, 7);
    }

    #[test]
    fn stages_list_every_stage_a_node_ran_in_and_done_needs_every_node() {
        let a = plan(vec![node(1, Some(10))], &[]);
        let b = plan(vec![node(7, Some(10))], &[]);
        let mut done = metric(1, None, None);
        done.done = true;
        let mut running = metric(1, None, None);
        running.done = false;
        let a_metrics = metrics(&[(1, done)]);
        let b_metrics = metrics(&[(7, running)]);

        let out = aggregate_logical_metrics(&[
            StageVariationMetrics {
                stage_number: StageNumber(2),
                plan: &b,
                metrics: &b_metrics,
            },
            StageVariationMetrics {
                stage_number: StageNumber(0),
                plan: &a,
                metrics: &a_metrics,
            },
        ]);

        let ten = &out.metrics[&OriginNodeId(10)];
        assert_eq!(ten.stages, vec![StageNumber(0), StageNumber(2)]);
        assert!(!ten.done);
    }

    #[test]
    fn a_join_with_a_same_origin_shuffle_read_counts_only_the_foreign_input() {
        let stage_0 = plan(
            vec![
                node(1, Some(5)),
                typed_node(2, Some(6), "Map"),
                typed_node(3, Some(6), "PartitionSink"),
            ],
            &[(2, 1), (3, 2)],
        );
        let stage_0_metrics = metrics(&[
            (1, metric(0, Some(180), None)),
            (2, metric(0, Some(180), Some(180))),
            (3, metric(0, None, Some(180))),
        ]);
        let stage_1 = plan(
            vec![
                typed_node(4, Some(6), "MultiScan"),
                typed_node(5, Some(6), "EquiJoin"),
                node(6, Some(20)),
            ],
            &[(5, 4), (5, 6)],
        );
        let stage_1_metrics = metrics(&[
            (4, metric(0, Some(180), Some(0))),
            (5, metric(0, Some(9), Some(193))),
            (6, metric(0, Some(13), Some(13))),
        ]);

        let out = aggregate_logical_metrics(&[
            StageVariationMetrics {
                stage_number: StageNumber(0),
                plan: &stage_0,
                metrics: &stage_0_metrics,
            },
            StageVariationMetrics {
                stage_number: StageNumber(1),
                plan: &stage_1,
                metrics: &stage_1_metrics,
            },
        ]);

        let six = &out.metrics[&OriginNodeId(6)];
        assert_eq!(
            six.rows_received,
            Some(193),
            "180 rows from logical node 5 and 13 from logical node 20"
        );
        assert_eq!(six.rows_sent, Some(9));
        assert_eq!(out.metrics[&OriginNodeId(5)].rows_sent, Some(180));
        let twenty = &out.metrics[&OriginNodeId(20)];
        assert_eq!(twenty.rows_received, Some(13));
        assert_eq!(twenty.rows_sent, Some(13));
    }

    #[test]
    fn a_multiplexer_port_that_closes_early_does_not_leave_a_residue() {
        let plan = plan(
            vec![
                node(1, Some(14)),
                typed_node(2, Some(15), "Map"),
                typed_node(3, Some(15), "Multiplexer"),
                typed_node(4, Some(15), "FileSink"),
                typed_node(5, Some(15), "Slice"),
                typed_node(6, Some(15), "Reduce"),
                typed_node(7, Some(15), "InMemorySink"),
                typed_node(8, Some(15), "InMemorySink"),
            ],
            &[(2, 1), (3, 2), (4, 3), (5, 3), (6, 3), (7, 5), (8, 6)],
        );
        let metrics = metrics(&[
            (1, metric(0, Some(175), None)),
            (2, metric(0, Some(175), Some(175))),
            (3, metric(0, Some(396), Some(175))),
            (4, metric(0, None, Some(175))),
            (5, metric(0, Some(40), Some(46))),
            (6, metric(0, Some(4), Some(175))),
            (7, metric(0, Some(0), Some(40))),
            (8, metric(0, Some(0), Some(4))),
        ]);

        let out = aggregate_logical_metrics(&[StageVariationMetrics {
            stage_number: StageNumber(0),
            plan: &plan,
            metrics: &metrics,
        }]);

        let fifteen = &out.metrics[&OriginNodeId(15)];
        assert_eq!(fifteen.rows_received, Some(175));
        assert_eq!(fifteen.rows_sent, Some(0));
    }

    #[test]
    fn an_interior_zip_of_two_ports_from_one_producer_is_not_a_boundary() {
        let plan = plan(
            vec![node(1, Some(3)), node(2, Some(3)), node(3, Some(4))],
            &[(2, 1), (2, 1), (3, 2)],
        );
        let metrics = metrics(&[
            (1, metric(0, Some(20), Some(10))),
            (2, metric(0, Some(20), Some(20))),
            (3, metric(0, None, Some(20))),
        ]);

        let out = aggregate_logical_metrics(&[StageVariationMetrics {
            stage_number: StageNumber(0),
            plan: &plan,
            metrics: &metrics,
        }]);

        let three = &out.metrics[&OriginNodeId(3)];
        assert_eq!(three.rows_received, Some(10), "node 1 has no producer");
        assert_eq!(three.rows_sent, Some(20), "node 2's consumer is foreign");
        assert_eq!(out.metrics[&OriginNodeId(4)].rows_received, Some(20));
    }

    #[test]
    fn a_scan_whose_reads_meet_in_a_union_receives_no_rows() {
        let plan = plan(
            vec![
                typed_node(1, Some(7), "MultiScan"),
                typed_node(2, Some(7), "MultiScan"),
                typed_node(3, Some(7), "Union"),
                node(4, Some(8)),
            ],
            &[(3, 1), (3, 2), (4, 3)],
        );
        let mut first = metric(0, Some(50), None);
        first.io_total_bytes_received = 1000;
        let mut second = metric(0, Some(30), None);
        second.io_total_bytes_received = 500;
        let metrics = metrics(&[
            (1, first),
            (2, second),
            (3, metric(0, Some(80), Some(80))),
            (4, metric(0, Some(10), Some(80))),
        ]);

        let out = aggregate_logical_metrics(&[StageVariationMetrics {
            stage_number: StageNumber(0),
            plan: &plan,
            metrics: &metrics,
        }]);

        let scan = &out.metrics[&OriginNodeId(7)];
        assert_eq!(scan.rows_received, None);
        assert_eq!(scan.rows_sent, Some(80));
        assert_eq!(scan.io_total_bytes_received, 1500);
        assert_eq!(out.metrics[&OriginNodeId(8)].rows_received, Some(80));
    }
}
