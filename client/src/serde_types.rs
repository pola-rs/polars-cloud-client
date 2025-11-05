use std::collections::BTreeMap;

use protos_client_compute::client::{ComputeQueryInfo, StageStatistics};
use protos_client_compute::observatory::QueryProfile;
use protos_common::query_info::FileType;
use protos_common::{QueryOutput, QueryResult};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pyclass;
use pyo3::types::PyBytes;

use crate::query_settings::{PyEngine, PyQuerySettings, PyQueryType, PyShuffleOpts};

#[derive(FromPyObject)]
pub struct DistributedSettings {
    sort_partitioned: bool,
    pre_aggregation: bool,
    equi_join_broadcast_limit: u64,
}

#[allow(clippy::needless_lifetimes)]
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature=(*, engine, prefer_dot, shuffle_opts, n_retries, distributed_settings))]
pub fn serialize_query_settings(
    engine: &str,
    prefer_dot: bool,
    shuffle_opts: PyShuffleOpts,
    n_retries: u32,
    distributed_settings: Option<DistributedSettings>,
) -> PyResult<PyQuerySettings> {
    let query_type = match distributed_settings {
        None => PyQueryType::Single(),
        Some(settings) => PyQueryType::Distributed {
            shuffle_opts,
            pre_aggregation: settings.pre_aggregation,
            sort_partitioned: settings.sort_partitioned,
            equi_join_broadcast_limit: settings.equi_join_broadcast_limit,
        },
    };

    let engine = match engine {
        "gpu" => PyEngine::Gpu,
        "auto" => PyEngine::Auto,
        "in-memory" => PyEngine::InMemory,
        "streaming" => PyEngine::Streaming,
        v => {
            let msg =
                format!("expected one of {{'auto', 'in-memory', 'streaming', 'gpu'}}, got {v}",);
            return Err(PyValueError::new_err(msg));
        },
    };

    let settings = PyQuerySettings {
        engine,
        prefer_dot,
        n_retries,
        query_type,
    };

    Ok(settings)
}

#[pyclass]
#[derive(Clone)]
pub struct StageStatsPy {
    #[pyo3(get)]
    num_workers_used: u32,
}

#[pyclass]
pub struct QueryInfoPy {
    #[pyo3(get)]
    pub total_stages: u32,
    #[pyo3(get)]
    pub finished_stages: u32,
    #[pyo3(get)]
    pub failed_stages: u32,
    pub head: Option<Result<Py<PyBytes>, String>>,
    #[pyo3(get)]
    pub n_rows_result: Option<u64>,
    #[pyo3(get)]
    pub errors: Vec<String>,
    #[pyo3(get)]
    pub sink_dst: Vec<String>,
    #[pyo3(get)]
    pub file_type_sink: String,
    #[pyo3(get)]
    pub ir_plan_explain: Option<String>,
    #[pyo3(get)]
    pub ir_plan_dot: Option<String>,
    #[pyo3(get)]
    pub phys_plan_explain: Option<String>,
    #[pyo3(get)]
    pub phys_plan_dot: Option<String>,
    #[pyo3(get)]
    pub stages_stats: Option<BTreeMap<u32, StageStatsPy>>,
}
#[pymethods]
impl QueryInfoPy {
    #[getter]
    fn head(&self) -> PyResult<Option<&Py<PyBytes>>> {
        if let Some(b) = &self.head {
            match b {
                Ok(b) => Ok(Some(b)),
                Err(msg) => Err(PyRuntimeError::new_err(msg.clone())),
            }
        } else {
            Ok(None)
        }
    }
}

impl From<&StageStatistics> for StageStatsPy {
    fn from(value: &StageStatistics) -> Self {
        StageStatsPy {
            num_workers_used: value.num_workers_used,
        }
    }
}

pub(crate) fn query_result_to_py(
    py: Python,
    query_info: QueryResult,
    mut compute_info: Option<ComputeQueryInfo>,
) -> QueryInfoPy {
    let file_type = match query_info.output {
        Some(QueryOutput { file_type, .. }) => match file_type {
            Some(FileType::Parquet) => "parquet",
            Some(FileType::Ipc) => "ipc",
            Some(FileType::Csv) => "csv",
            Some(FileType::Json) => "json",
            Some(FileType::Ndjson) => "ndjson",
            None => "unknown",
        },
        None => "none",
    };
    QueryInfoPy {
        total_stages: query_info.total_stages,
        finished_stages: query_info.finished_stages,
        failed_stages: query_info.failed_stages,
        n_rows_result: query_info.output.as_ref().map(|o| o.n_rows_result),
        errors: query_info.errors,
        file_type_sink: file_type.to_string(),
        ir_plan_explain: None,
        ir_plan_dot: None,
        phys_plan_explain: None,
        phys_plan_dot: None,
        sink_dst: query_info.output.map(|o| o.sink_dst).unwrap_or_default(),
        stages_stats: compute_info
            .as_mut()
            .and_then(|ci| std::mem::take(&mut ci.stage_statistics))
            .map(|v| v.iter().map(|(k, v)| (*k, v.into())).collect()),
        head: compute_info.and_then(|ci| {
            ci.head
                .map(|res| res.map(|b| PyBytes::new(py, b.as_ref()).unbind()))
        }),
    }
}

#[pyclass]
pub struct QueryProfilePy {
    #[pyo3(get)]
    pub tag: Py<PyBytes>,
    #[pyo3(get)]
    pub total_stages: Option<u32>,
    #[pyo3(get)]
    pub phys_plan_dot: Option<String>,
    #[pyo3(get)]
    pub phys_plan_explain: Option<String>,
    #[pyo3(get)]
    pub data: Py<PyBytes>,
}

pub(crate) fn query_profile_to_py(py: Python, progress: QueryProfile) -> QueryProfilePy {
    QueryProfilePy {
        tag: PyBytes::new(py, progress.tag.as_ref()).unbind(),
        total_stages: progress.total_stages,
        phys_plan_explain: progress.phys_plan_explain,
        phys_plan_dot: progress.phys_plan_dot,
        data: PyBytes::new(py, &progress.data).unbind(),
    }
}
