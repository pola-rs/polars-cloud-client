use protos_client_compute::client::{
    DistributedOpts, Engine, GraphFormat, LineageContext, QuerySettings, QueryType,
    ShuffleCompression, ShuffleFormat, ShuffleOpts, SingleWorkerOps,
};
use pyo3::exceptions::PyValueError;
use pyo3::{PyResult, pyclass, pymethods};
use uuid::Uuid;

#[pyclass(from_py_object)]
#[derive(Debug, Clone, Copy)]
pub enum PyEngine {
    Auto,
    InMemory,
    Streaming,
    Gpu,
}

#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct PyShuffleOpts {
    pub format: PyShuffleFormat,
    pub compression: PyShuffleCompression,
    pub compression_level: Option<i32>,
}

#[pyo3::pymethods]
impl PyShuffleOpts {
    #[staticmethod]
    pub fn new(compression: &str, format: &str, compression_level: Option<i32>) -> PyResult<Self> {
        Ok(Self {
            format: match format {
                "auto" => PyShuffleFormat::Auto,
                "ipc" => PyShuffleFormat::Ipc,
                "parquet" => PyShuffleFormat::Parquet,
                v => {
                    let msg = format!("expected one of {{'auto', 'ipc', 'parquet'}}, got {v}",);
                    return Err(PyValueError::new_err(msg));
                },
            },
            compression: match compression {
                "auto" => PyShuffleCompression::Auto,
                "lz4" => PyShuffleCompression::Lz4,
                "zstd" => PyShuffleCompression::Zstd,
                "uncompressed" => PyShuffleCompression::Uncompressed,
                v => {
                    let msg = format!(
                        "expected one of {{'auto', 'lz4', 'zstd', 'uncompressed'}}, got {v}",
                    );
                    return Err(PyValueError::new_err(msg));
                },
            },
            compression_level,
        })
    }
}

impl From<PyShuffleOpts> for ShuffleOpts {
    fn from(value: PyShuffleOpts) -> Self {
        Self {
            format: value.format.into(),
            compression: value.compression.into(),
            compression_level: value.compression_level,
        }
    }
}

#[pyclass(from_py_object)]
#[derive(Clone, Debug)]
pub enum PySingleWorkerOps {
    Auto,
    Allow,
    Forbid,
}

impl From<PySingleWorkerOps> for SingleWorkerOps {
    fn from(value: PySingleWorkerOps) -> Self {
        match value {
            PySingleWorkerOps::Auto => Self::Auto,
            PySingleWorkerOps::Allow => Self::Allow,
            PySingleWorkerOps::Forbid => Self::Forbid,
        }
    }
}

#[pyclass(from_py_object)]
#[derive(Clone, Debug)]
pub enum PyQueryType {
    Single(),
    Distributed {
        shuffle_opts: PyShuffleOpts,
        pre_aggregation: bool,
        expression_lowering: bool,
        sort_partitioned: bool,
        equi_join_broadcast_limit: u64,
        partitions_per_worker: Option<u32>,
        single_worker_ops: PySingleWorkerOps,
    },
}

#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub enum PyShuffleCompression {
    Auto,
    Lz4,
    Zstd,
    Uncompressed,
}

#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub enum PyShuffleFormat {
    Auto,
    Ipc,
    Parquet,
}

#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct PyQuerySettings {
    pub engine: PyEngine,
    pub query_type: PyQueryType,
    /// Whether the query plain should be in dot or plain text.
    pub prefer_dot: bool,
    /// Number of retries on failed tasks
    pub n_retries: u32,
    pub optimization_flags: Option<u32>,
}

impl From<PyQuerySettings> for QuerySettings {
    fn from(value: PyQuerySettings) -> Self {
        Self {
            engine: value.engine.into(),
            preferred_graph_format: if value.prefer_dot {
                GraphFormat::Dot
            } else {
                GraphFormat::Auto
            },
            n_retries: value.n_retries,
            query_type: value.query_type.into(),
            optimization_flags: value.optimization_flags,
        }
    }
}

impl From<PyEngine> for Engine {
    fn from(value: PyEngine) -> Self {
        match value {
            PyEngine::Auto => Self::Auto,
            PyEngine::InMemory => Self::InMemory,
            PyEngine::Streaming => Self::Streaming,
            PyEngine::Gpu => Self::Gpu,
        }
    }
}

impl From<PyShuffleFormat> for ShuffleFormat {
    fn from(value: PyShuffleFormat) -> Self {
        match value {
            PyShuffleFormat::Auto => Self::Auto,
            PyShuffleFormat::Ipc => Self::Ipc,
            PyShuffleFormat::Parquet => Self::Parquet,
        }
    }
}

impl From<PyShuffleCompression> for ShuffleCompression {
    fn from(value: PyShuffleCompression) -> Self {
        match value {
            PyShuffleCompression::Auto => Self::Auto,
            PyShuffleCompression::Lz4 => Self::LZ4,
            PyShuffleCompression::Zstd => Self::ZSTD,
            PyShuffleCompression::Uncompressed => Self::Uncompressed,
        }
    }
}

impl From<PyQueryType> for QueryType {
    fn from(value: PyQueryType) -> Self {
        match value {
            PyQueryType::Single() => Self::Single,
            PyQueryType::Distributed {
                shuffle_opts,
                pre_aggregation,
                expression_lowering,
                sort_partitioned,
                equi_join_broadcast_limit,
                partitions_per_worker,
                single_worker_ops,
            } => Self::Distributed(DistributedOpts {
                shuffle_opts: shuffle_opts.into(),
                pre_aggregation,
                expression_lowering,
                sort_partitioned,
                equi_join_broadcast_limit,
                partitions_per_worker,
                single_worker_ops: single_worker_ops.into(),
            }),
        }
    }
}

#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct PyLineageContext {
    pub job_namespace: String,
    pub job_name: String,
    pub parent_run_id: Option<Uuid>,
    pub parent_job_namespace: Option<String>,
    pub parent_job_name: Option<String>,
}

#[pymethods]
impl PyLineageContext {
    #[new]
    #[pyo3(signature = (job_namespace, job_name, parent_run_id=None, parent_job_namespace=None, parent_job_name=None))]
    pub fn new(
        job_namespace: String,
        job_name: String,
        parent_run_id: Option<Uuid>,
        parent_job_namespace: Option<String>,
        parent_job_name: Option<String>,
    ) -> Self {
        Self {
            job_namespace,
            job_name,
            parent_run_id,
            parent_job_namespace,
            parent_job_name,
        }
    }
}

impl From<PyLineageContext> for LineageContext {
    fn from(value: PyLineageContext) -> Self {
        Self {
            job_namespace: value.job_namespace,
            job_name: value.job_name,
            parent_run_id: value.parent_run_id,
            parent_job_namespace: value.parent_job_namespace,
            parent_job_name: value.parent_job_name,
        }
    }
}

impl From<PyLineageContext> for protos_common::LineageContext {
    fn from(value: PyLineageContext) -> Self {
        Self {
            job_namespace: value.job_namespace,
            job_name: value.job_name,
            parent_run_id: value.parent_run_id.map(|u| u.to_string()),
            parent_job_namespace: value.parent_job_namespace,
            parent_job_name: value.parent_job_name,
        }
    }
}
