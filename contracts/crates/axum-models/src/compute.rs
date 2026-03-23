use std::fmt::Display;

use chrono::{DateTime, FixedOffset, Utc};
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::{PyResult, exceptions::PyValueError, pyclass};
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use version_number::VersionNumber;

use crate::termination::TerminationModel;
use crate::{EntityOrdering, InstanceSpecsModel};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[serde(
    deny_unknown_fields,
    rename_all = "snake_case",
    tag = "mode",
    content = "settings"
)]
pub enum ClusterModeModel {
    // client_public_key is optional, it can be an empty string.
    // It will remain a String type for backwards compatibility.
    Direct { client_public_key: String },
    Proxy,
}

#[derive(Default, Clone, Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[cfg_attr(feature = "pyo3", pyclass(from_py_object, eq, eq_int))]
pub enum LogLevelModel {
    Trace,
    Debug,
    #[default]
    Info,
}

#[cfg_attr(feature = "pyo3", pyo3::pymethods)]
#[cfg(feature = "pyo3")]
impl LogLevelModel {
    #[staticmethod]
    fn from_str(s: Option<&str>) -> PyResult<Self> {
        match s {
            Some("info") | None => Ok(Self::Info),
            Some("debug") => Ok(Self::Debug),
            Some("trace") => Ok(Self::Trace),
            Some(s) => Err(PyValueError::new_err(format!(
                "Invalid LogLevelSchema: '{s}'. Expected one of {{'info','debug','trace'}}"
            ))),
        }
    }

    fn as_str(&self) -> &str {
        match self {
            LogLevelModel::Info => "info",
            LogLevelModel::Debug => "debug",
            LogLevelModel::Trace => "trace",
        }
    }
}

#[derive(Deserialize, Serialize, Copy, Debug, Clone)]
#[cfg_attr(feature = "server", derive(JsonSchema, Validate))]
pub struct PythonVersion {
    #[cfg_attr(feature = "server", garde(range(min = 3, max = 3)))]
    pub major: u8,
    #[cfg_attr(feature = "server", garde(range(min = 9)))]
    pub minor: u8,
    #[cfg_attr(feature = "server", garde(skip))]
    pub patch: u8,
}

impl Display for PythonVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "server", derive(JsonSchema, Validate))]
#[serde(deny_unknown_fields)]
pub struct RegisterComputeClusterArgs {
    #[cfg_attr(feature = "server", garde(skip))]
    pub name: String,
    #[serde(flatten)]
    #[cfg_attr(feature = "server", garde(dive))]
    pub instance: InstanceSpecsModel,
    #[cfg_attr(feature = "server", garde(range(min = 16)))]
    pub storage: Option<u32>,
    #[cfg_attr(feature = "server", garde(range(min = 16)))]
    pub big_instance_storage: Option<u32>,
    #[cfg_attr(feature = "server", garde(range(min = 1)))]
    pub cluster_size: u32,
    #[serde(default, flatten)]
    #[cfg_attr(feature = "server", garde(skip))]
    pub mode: ClusterModeModel,
    #[cfg_attr(feature = "server", garde(dive))]
    pub python_version: PythonVersion,
    #[cfg_attr(feature = "server", garde(skip), schemars(with = "String"))]
    pub polars_version: VersionNumber,
    #[cfg_attr(feature = "server", garde(skip))]
    pub labels: Option<Vec<String>>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub log_level: LogLevelModel,
    #[cfg_attr(feature = "server", garde(range(min = 10)))]
    pub idle_timeout_mins: Option<u32>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub requirements_txt: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "server", derive(JsonSchema, Validate))]
#[serde(deny_unknown_fields)]
pub struct StartComputeClusterManifestArgs {
    #[cfg_attr(feature = "server", garde(skip))]
    pub name: String,
    #[cfg_attr(feature = "server", garde(dive))]
    pub python_version: PythonVersion,
    #[cfg_attr(feature = "server", garde(skip), schemars(with = "String"))]
    pub polars_version: VersionNumber,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "server", derive(JsonSchema, Validate))]
#[serde(deny_unknown_fields)]
pub struct StartComputeClusterArgs {
    #[serde(flatten)]
    #[cfg_attr(feature = "server", garde(dive))]
    pub instance: InstanceSpecsModel,
    #[cfg_attr(feature = "server", garde(range(min = 16)))]
    pub storage: Option<u32>,
    #[cfg_attr(feature = "server", garde(range(min = 16)))]
    pub big_instance_storage: Option<u32>,
    #[cfg_attr(feature = "server", garde(range(min = 1)))]
    pub cluster_size: u32,
    #[serde(default, flatten)]
    #[cfg_attr(feature = "server", garde(skip))]
    pub mode: ClusterModeModel,
    #[cfg_attr(feature = "server", garde(dive))]
    pub python_version: PythonVersion,
    #[cfg_attr(feature = "server", garde(skip), schemars(with = "String"))]
    pub polars_version: VersionNumber,
    #[cfg_attr(feature = "server", garde(skip))]
    pub labels: Option<Vec<String>>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub log_level: Option<LogLevelModel>,
    #[cfg_attr(feature = "server", garde(range(min = 10)))]
    pub idle_timeout_mins: Option<u32>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub requirements_txt: Option<String>,
}

#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object, get_all))]
#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct ComputeClusterPublicInfoModel {
    pub cluster_id: Uuid,
    pub public_address: String,
    pub public_server_key: String,
}

#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object, get_all))]
#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct ComputeClusterNodeInfoModel {
    pub cluster_id: Uuid,
    pub private_address: Option<String>,
    pub cpus: Option<i32>,
    pub memory_mb: Option<i32>,
    pub storage_mb: Option<i32>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct SupportedVersionsModel {
    #[cfg_attr(feature = "server", schemars(with = "Vec<String>"))]
    pub polars: Vec<VersionNumber>,
}

impl EntityOrdering for ComputeClusterNodeInfoModel {
    fn order_fields() -> &'static [&'static str] {
        &["cpus", "memory_mb", "storage_mb"]
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct AwsMetricModel {
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct AwsMetricsModel {
    pub cpu_usage_percent: AwsMetricModel,
    pub memory_usage: AwsMetricModel,
    pub memory_usage_percent: AwsMetricModel,
    pub disk_usage: AwsMetricModel,
    pub disk_usage_percent: AwsMetricModel,
}

#[derive(Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(schemars::JsonSchema))]
pub struct TimeWindowOpt {
    pub start: Option<DateTime<FixedOffset>>,
    pub end: Option<DateTime<FixedOffset>>,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, eq, eq_int))]
#[cfg_attr(feature = "server", derive(JsonSchema, Validate))]
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum DBCPUArchitectureModel {
    X86_64,
    Arm64,
}

#[cfg_attr(feature = "pyo3", pyo3::pymethods)]
#[cfg(feature = "pyo3")]
impl DBCPUArchitectureModel {
    #[staticmethod]
    fn from_str(s: Option<&str>) -> PyResult<Self> {
        match s {
            Some("x86_64") | None => Ok(Self::X86_64),
            Some("arm64") => Ok(Self::Arm64),
            Some(s) => Err(PyValueError::new_err(format!(
                "Invalid DBCPUArchitecture: '{s}'. Expected 'x86_64' or 'arm64'"
            ))),
        }
    }

    fn as_str(&self) -> &str {
        match self {
            DBCPUArchitectureModel::X86_64 => "x86_64",
            DBCPUArchitectureModel::Arm64 => "arm64",
        }
    }
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, eq, eq_int))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum DBClusterModeModel {
    Proxy,
    Direct,
}

#[cfg_attr(feature = "pyo3", pyo3::pymethods)]
#[cfg(feature = "pyo3")]
impl DBClusterModeModel {
    #[staticmethod]
    fn from_str(s: Option<&str>) -> PyResult<Self> {
        match s {
            Some("direct") | None => Ok(Self::Direct),
            Some("proxy") => Ok(Self::Proxy),
            Some(s) => Err(PyValueError::new_err(format!(
                "Invalid DBClusterModeSchema: '{s}'. Expected 'proxy' or 'direct'"
            ))),
        }
    }

    fn as_str(&self) -> &str {
        match self {
            DBClusterModeModel::Direct => "direct",
            DBClusterModeModel::Proxy => "proxy",
        }
    }
}

fn csv_vec_opt<'de, D, T>(deserializer: D) -> Result<Option<Vec<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    if let Some(s) = Option::<String>::deserialize(deserializer)? {
        Ok(Some(
            s.split(',')
                .map(|item| T::deserialize(item.into_deserializer()))
                .collect::<Result<Vec<_>, _>>()?,
        ))
    } else {
        Ok(None)
    }
}

#[derive(Deserialize, Default, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct GetClusterFilterArgs {
    /// Filters out any clusters that are not in the given status.
    #[serde(default)]
    #[serde(deserialize_with = "csv_vec_opt")]
    pub status: Option<Vec<ComputeStatusModel>>,
    #[serde(default)]
    pub current_user_only: bool,
}

#[cfg_attr(feature = "pyo3", pyclass(skip_from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeTokenModel {
    pub id: Uuid,
    pub token: String,
}

#[cfg_attr(feature = "pyo3", pyclass)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeModel {
    pub id: Uuid,
    pub user_id: Uuid,
    pub workspace_id: Uuid,
    pub name: Option<String>,
    pub instance_type: Option<String>,
    pub cpu_architectures: Option<Vec<DBCPUArchitectureModel>>,
    pub req_ram_gb: Option<u32>,
    pub req_cpu_cores: Option<u32>,
    pub req_storage: Option<i32>,
    pub big_instance_type: Option<String>,
    pub req_big_instance_multiplier: Option<u32>,
    pub req_big_instance_storage: Option<i32>,
    pub ram_mib: Option<i64>,
    pub vcpus: Option<i32>,
    pub storage_gb: Option<i32>,
    pub cluster_size: u32,
    pub termination: Option<TerminationModel>,
    pub gc_inactive_hours: i32,
    pub request_time: DateTime<Utc>,
    pub mode: DBClusterModeModel,
    #[cfg_attr(feature = "server", schemars(with = "String"))]
    pub polars_version: VersionNumber,
    pub status: ComputeStatusModel,
    pub log_level: LogLevelModel,
    pub tunnel_addr: Option<String>,
}

impl EntityOrdering for ComputeModel {
    fn order_fields() -> &'static [&'static str] {
        &[
            "id",
            "request_time",
            "cluster_size",
            "storage_gb",
            "ram_mib",
            "vcpus",
        ]
    }
}

#[cfg_attr(feature = "pyo3", pyo3::pymethods)]
#[cfg(feature = "pyo3")]
impl ComputeModel {
    #[getter]
    pub fn id(&self) -> pyo3::PyResult<Uuid> {
        Ok(self.id)
    }

    #[getter]
    pub fn user_id(&self) -> pyo3::PyResult<Uuid> {
        Ok(self.user_id)
    }

    #[getter]
    pub fn workspace_id(&self) -> pyo3::PyResult<Uuid> {
        Ok(self.workspace_id)
    }

    #[getter]
    pub fn name(&self) -> pyo3::PyResult<Option<&str>> {
        Ok(self.name.as_deref())
    }

    #[getter]
    pub fn instance_type(&self) -> pyo3::PyResult<Option<&str>> {
        Ok(self.instance_type.as_deref())
    }

    #[getter]
    pub fn cpu_architectures(&self) -> pyo3::PyResult<Option<Vec<DBCPUArchitectureModel>>> {
        Ok(self.cpu_architectures.clone())
    }

    #[getter]
    pub fn req_ram_gb(&self) -> pyo3::PyResult<Option<u32>> {
        Ok(self.req_ram_gb)
    }

    #[getter]
    pub fn req_cpu_cores(&self) -> pyo3::PyResult<Option<u32>> {
        Ok(self.req_cpu_cores)
    }

    #[getter]
    pub fn req_storage(&self) -> pyo3::PyResult<Option<i32>> {
        Ok(self.req_storage)
    }

    #[getter]
    pub fn big_instance_type(&self) -> pyo3::PyResult<Option<&str>> {
        Ok(self.big_instance_type.as_deref())
    }

    #[getter]
    pub fn req_big_instance_multiplier(&self) -> pyo3::PyResult<Option<u32>> {
        Ok(self.req_big_instance_multiplier)
    }

    #[getter]
    pub fn req_big_instance_storage(&self) -> pyo3::PyResult<Option<i32>> {
        Ok(self.req_big_instance_storage)
    }

    #[getter]
    pub fn ram_mib(&self) -> pyo3::PyResult<Option<i64>> {
        Ok(self.ram_mib)
    }

    #[getter]
    pub fn vcpus(&self) -> pyo3::PyResult<Option<i32>> {
        Ok(self.vcpus)
    }

    #[getter]
    pub fn cluster_size(&self) -> pyo3::PyResult<u32> {
        Ok(self.cluster_size)
    }

    #[getter]
    pub fn termination(&self) -> pyo3::PyResult<Option<TerminationModel>> {
        Ok(self.termination.clone())
    }

    #[getter]
    pub fn gc_inactive_hours(&self) -> pyo3::PyResult<i32> {
        Ok(self.gc_inactive_hours)
    }

    #[getter]
    pub fn request_time(&self) -> pyo3::PyResult<DateTime<Utc>> {
        Ok(self.request_time)
    }

    #[getter]
    pub fn mode(&self) -> pyo3::PyResult<DBClusterModeModel> {
        Ok(self.mode)
    }

    #[getter]
    pub fn polars_version(&self) -> pyo3::PyResult<String> {
        Ok(self.polars_version.to_string())
    }

    #[getter]
    pub fn status(&self) -> pyo3::PyResult<ComputeStatusModel> {
        Ok(self.status)
    }

    #[getter]
    pub fn log_level(&self) -> pyo3::PyResult<LogLevelModel> {
        Ok(self.log_level.clone())
    }

    #[getter]
    pub fn tunnel_addr(&self) -> pyo3::PyResult<Option<&str>> {
        Ok(self.tunnel_addr.as_deref())
    }
}

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all, eq, eq_int))]
#[derive(Debug, Deserialize, Clone, Copy, Serialize, PartialEq)]
pub enum ComputeStatusModel {
    Starting = 0,
    Idle = 1,
    Running = 2,
    Stopping = 3,
    Stopped = 4,
    Failed = 7,
}

impl Display for ComputeStatusModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComputeStatusModel::Starting => write!(f, "Starting"),
            ComputeStatusModel::Idle => write!(f, "Idle"),
            ComputeStatusModel::Running => write!(f, "Running"),
            ComputeStatusModel::Stopping => write!(f, "Stopping"),
            ComputeStatusModel::Stopped => write!(f, "Stopped"),
            ComputeStatusModel::Failed => write!(f, "Failed"),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct AwsLogEventModel {
    pub timestamp: DateTime<Utc>,
    pub message: String,
}

#[cfg(feature = "server")]
#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct TokenPaginated<T: Serialize + schemars::JsonSchema> {
    pub data: T,
    pub next_token: Option<String>,
}

#[cfg(not(feature = "server"))]
#[derive(Debug, Deserialize, Serialize)]
pub struct TokenPaginated<T: Serialize> {
    pub data: T,
    pub next_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct NextToken {
    pub next_token: Option<String>,
}
