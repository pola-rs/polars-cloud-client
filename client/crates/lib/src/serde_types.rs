use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;

use client_core::{ApiError, ApiResult};
use pc_observatory_models::QueryDetailModel;
use protos_client_compute::client::{ComputeQueryInfo, StageStatistics};
use protos_client_compute::tonic::body::Body;
use protos_client_compute::tonic::codegen::http;
use protos_client_compute::tonic::codegen::http::header::{
    CONTENT_LENGTH, HOST, TRANSFER_ENCODING,
};
use protos_client_compute::tonic::codegen::http::{HeaderMap, HeaderName, HeaderValue};
use protos_client_compute::tonic::transport::Uri;
use protos_common::query_info::FileType;
use protos_common::{QueryOutput, QueryResult};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pyclass;
use pyo3::types::{PyBytes, PyDict, PyString};
use tonic::codegen::http::uri::Authority;
use tower_http::set_header::HeaderMetadata;

use crate::query_settings::{
    PyEngine, PyNumWorkers, PyQuerySettings, PyQueryType, PyShuffleOpts, PySingleWorkerOps,
};

pub struct DistributedSettings {
    sort_partitioned: bool,
    pre_aggregation: bool,
    expression_lowering: Option<bool>,
    equi_join_broadcast_limit: u64,
    partitions_per_worker: Option<u32>,
    single_worker_ops: PySingleWorkerOps,
}

impl<'a, 'py> FromPyObject<'a, 'py> for DistributedSettings {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let sort_partitioned = obj.getattr("sort_partitioned")?.extract()?;
        let pre_aggregation = obj.getattr("pre_aggregation")?.extract()?;
        let expression_lowering = obj.getattr("expression_lowering")?.extract()?;
        let equi_join_broadcast_limit = obj.getattr("equi_join_broadcast_limit")?.extract()?;
        let partitions_per_worker = obj.getattr("partitions_per_worker")?.extract()?;
        let single_worker_ops = match obj.getattr("single_worker_ops")?.extract::<&str>()? {
            "auto" => PySingleWorkerOps::Auto,
            "allow" => PySingleWorkerOps::Allow,
            "forbid" => PySingleWorkerOps::Forbid,
            v => {
                let msg = format!("expected one of {{'auto', 'allow', 'forbid'}}, got {v}",);
                return Err(PyValueError::new_err(msg));
            },
        };

        Ok(DistributedSettings {
            sort_partitioned,
            pre_aggregation,
            expression_lowering,
            equi_join_broadcast_limit,
            partitions_per_worker,
            single_worker_ops,
        })
    }
}

#[allow(clippy::needless_lifetimes)]
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature=(*, engine, prefer_dot, shuffle_opts, n_retries, n_workers, distributed_settings, optimization_flags))]
pub fn serialize_query_settings(
    engine: &str,
    prefer_dot: bool,
    shuffle_opts: PyShuffleOpts,
    n_retries: u32,
    n_workers: Option<PyNumWorkers>,
    distributed_settings: Option<DistributedSettings>,
    optimization_flags: Option<u32>,
) -> PyResult<PyQuerySettings> {
    let query_type = match distributed_settings {
        None => PyQueryType::Single(),
        Some(settings) => PyQueryType::Distributed {
            shuffle_opts,
            pre_aggregation: settings.pre_aggregation,
            expression_lowering: settings.expression_lowering,
            sort_partitioned: settings.sort_partitioned,
            equi_join_broadcast_limit: settings.equi_join_broadcast_limit,
            partitions_per_worker: settings.partitions_per_worker,
            single_worker_ops: settings.single_worker_ops,
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
        optimization_flags,
        n_workers,
    };

    Ok(settings)
}

#[pyclass(from_py_object)]
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

#[pyclass(from_py_object, get_all)]
#[derive(Clone)]
pub struct QueryPlanTimingPy {
    pub plan_start_time: Option<String>,
    pub parse_start_time: Option<String>,
    pub optimize_start_time: Option<String>,
    pub distribute_start_time: Option<String>,
    pub distribute_end_time: Option<String>,
    pub plan_end_time: Option<String>,
}

#[pyclass(skip_from_py_object, get_all)]
pub struct QueryDetailPy {
    pub id: String,
    pub user_name: String,
    pub status: String,
    pub request_time: String,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub query_plan_timing: QueryPlanTimingPy,
    pub total_num_stages: Option<u32>,
    pub failed_stage: Option<u32>,
    pub in_progress_stages: Vec<u32>,
    pub finished_stages: Vec<u32>,
    pub total_bytes_shuffled: Option<i64>,
    pub total_node_time_ns: i64,
    pub percentage_time_shuffling: Option<f64>,
    pub total_num_files: Option<u64>,
    pub original_num_files: Option<u64>,
    pub total_rows_read: Option<i64>,
    pub errors: Option<Vec<String>>,
    pub engine: String,
    pub query_type: String,
    pub output_location: Option<String>,
    pub output_files: Option<i64>,
    pub output_rows: Option<i64>,
}

impl From<QueryDetailModel> for QueryDetailPy {
    fn from(value: QueryDetailModel) -> Self {
        // Use the IntoStaticStr impl from strum to convert enums to &str
        // without needing to import the enum types directly.
        let status: &'static str = value.status.into();
        let engine: &'static str = value.engine.into();
        let query_type: &'static str = value.query_type.into();

        let mut in_progress: Vec<u32> = value.in_progress_stages.into_iter().map(|s| s.0).collect();
        in_progress.sort_unstable();

        let mut finished: Vec<u32> = value.finished_stages.into_iter().map(|s| s.0).collect();
        finished.sort_unstable();

        // Extract the plan timing fields before moving value.
        let t = value.query_plan_timing;
        let query_plan_timing = QueryPlanTimingPy {
            plan_start_time: t.plan_start_time.map(|dt| dt.to_rfc3339()),
            parse_start_time: t.parse_start_time.map(|dt| dt.to_rfc3339()),
            optimize_start_time: t.optimize_start_time.map(|dt| dt.to_rfc3339()),
            distribute_start_time: t.distribute_start_time.map(|dt| dt.to_rfc3339()),
            distribute_end_time: t.distribute_end_time.map(|dt| dt.to_rfc3339()),
            plan_end_time: t.plan_end_time.map(|dt| dt.to_rfc3339()),
        };

        QueryDetailPy {
            id: value.id.to_string(),
            user_name: value.user.name,
            status: status.to_string(),
            request_time: value.request_time.to_rfc3339(),
            start_time: value.start_time.map(|dt| dt.to_rfc3339()),
            end_time: value.end_time.map(|dt| dt.to_rfc3339()),
            query_plan_timing,
            total_num_stages: value.total_num_stages,
            failed_stage: value.failed_stage.map(|s| s.0),
            in_progress_stages: in_progress,
            finished_stages: finished,
            total_bytes_shuffled: value.total_bytes_shuffled,
            total_node_time_ns: value.total_node_time_ns,
            percentage_time_shuffling: value.percentage_time_shuffling,
            total_num_files: value.total_num_files,
            original_num_files: value.original_num_files,
            total_rows_read: value.total_rows_read,
            errors: value.errors,
            engine: engine.to_string(),
            query_type: query_type.to_string(),
            output_location: value.output_location,
            output_files: value.output_files,
            output_rows: value.output_rows,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParsedUri(Uri);

impl TryFrom<Uri> for ParsedUri {
    type Error = PyErr;

    fn try_from(uri: Uri) -> Result<Self, Self::Error> {
        let scheme = uri
            .scheme()
            .ok_or_else(|| PyValueError::new_err("missing scheme"))?;

        if scheme != "http" && scheme != "https" {
            return Err(PyValueError::new_err("scheme must be `http` or `https`"));
        }

        let _ = uri
            .port_u16()
            .ok_or_else(|| PyValueError::new_err("port must be defined"))?;

        Ok(ParsedUri(uri))
    }
}

impl From<ParsedUri> for Uri {
    fn from(uri: ParsedUri) -> Self {
        uri.0
    }
}

impl Deref for ParsedUri {
    type Target = Uri;
    fn deref(&self) -> &Uri {
        &self.0
    }
}

impl Display for ParsedUri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for ParsedUri {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let uri_str: String = ob.extract()?;

        let uri = uri_str
            .strip_suffix("/")
            .unwrap_or(&uri_str)
            .parse::<Uri>()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        uri.try_into()
    }
}

impl<'py> IntoPyObject<'py> for ParsedUri {
    type Target = PyString;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.0.to_string().into_pyobject(py).map_err(Into::into)
    }
}

#[derive(Clone, Debug)]
pub struct DomainName(String);

impl TryFrom<String> for DomainName {
    type Error = PyErr;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let authority = s
            .parse::<Authority>()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        if authority.port().is_some() {
            return Err(PyValueError::new_err("domain name must not contain a port"));
        }
        let host = authority.host();
        if host.parse::<std::net::IpAddr>().is_ok() {
            return Err(PyValueError::new_err(
                "expected a domain name, not an IP address",
            ));
        }
        Ok(DomainName(host.to_string()))
    }
}

impl From<DomainName> for String {
    fn from(domain_name: DomainName) -> String {
        domain_name.0
    }
}

impl Deref for DomainName {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl Display for DomainName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for DomainName {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let s: String = ob.extract()?;
        s.try_into()
    }
}

impl<'py> IntoPyObject<'py> for DomainName {
    type Target = PyString;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.0.to_string().into_pyobject(py).map_err(Into::into)
    }
}

#[derive(Clone, Default, Debug)]
pub struct ParsedHeaders(HeaderMap);

impl From<HeaderMap> for ParsedHeaders {
    fn from(map: HeaderMap) -> Self {
        ParsedHeaders(map)
    }
}

impl From<ParsedHeaders> for HeaderMap {
    fn from(map: ParsedHeaders) -> Self {
        map.0
    }
}

impl Deref for ParsedHeaders {
    type Target = HeaderMap;
    fn deref(&self) -> &HeaderMap {
        &self.0
    }
}

impl ParsedHeaders {
    const DISALLOWED_HEADERS: &[HeaderName] = &[HOST, CONTENT_LENGTH, TRANSFER_ENCODING];

    pub fn from_string_map(map: &HashMap<String, String>) -> ApiResult<Self> {
        let mut headers = HeaderMap::with_capacity(map.len());

        for (header, value) in map {
            let key = HeaderName::from_str(header).map_err(|_| {
                ApiError::PyErr(PyValueError::new_err(format!(
                    "invalid header name: {header:?}"
                )))
            })?;

            if Self::DISALLOWED_HEADERS.contains(&key) {
                return Err(ApiError::PyErr(PyValueError::new_err(format!(
                    "setting header {header:?} is forbidden"
                ))));
            }

            let value = HeaderValue::from_str(value).map_err(|_| {
                ApiError::PyErr(PyValueError::new_err(format!(
                    "invalid header value for {header:?}"
                )))
            })?;

            headers.insert(key, value);
        }

        Ok(Self(headers))
    }

    pub fn to_string_map(&self) -> HashMap<String, String> {
        self.0
            .iter()
            .filter_map(|(k, v)| {
                v.to_str()
                    .ok()
                    .map(|v| (k.as_str().to_owned(), v.to_owned()))
            })
            .collect()
    }

    pub fn to_header_metadata(&self) -> Vec<HeaderMetadata<http::Request<Body>>> {
        self.0
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()).into())
            .collect()
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for ParsedHeaders {
    type Error = PyErr;

    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let map: HashMap<String, String> = ob.extract()?;
        ParsedHeaders::from_string_map(&map).map_err(PyErr::from)
    }
}

impl<'py> IntoPyObject<'py> for ParsedHeaders {
    type Target = PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.to_string_map().into_pyobject(py)
    }
}
