use std::fmt::Display;

use chrono::{DateTime, FixedOffset, Utc};
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::{PyResult, exceptions::PyValueError, pyclass};
use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::IntoParams;
#[cfg(feature = "server")]
use utoipa::ToSchema;
use uuid::Uuid;
use version_number::VersionNumber;

use crate::termination::TerminationSchema;
use crate::{EntityOrdering, InstanceSpecsSchema};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[serde(
    deny_unknown_fields,
    rename_all = "snake_case",
    tag = "mode",
    content = "settings"
)]
pub enum ClusterModeSchema {
    // client_public_key is optional, it can be an empty string.
    // It will remain a String type for backwards compatibility.
    Direct { client_public_key: String },
    Proxy,
}

#[derive(Default, Clone, Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[cfg_attr(feature = "pyo3", pyclass(eq, eq_int))]
pub enum LogLevelSchema {
    Trace,
    Debug,
    #[default]
    Info,
}

#[cfg_attr(feature = "pyo3", pyo3::pymethods)]
#[cfg(feature = "pyo3")]
impl LogLevelSchema {
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
            LogLevelSchema::Info => "info",
            LogLevelSchema::Debug => "debug",
            LogLevelSchema::Trace => "trace",
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "server", derive(ToSchema, Validate))]
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
#[cfg_attr(feature = "server", derive(ToSchema, Validate))]
#[serde(deny_unknown_fields)]
pub struct RegisterComputeClusterArgs {
    #[cfg_attr(feature = "server", garde(skip))]
    pub name: String,
    #[serde(flatten)]
    #[cfg_attr(feature = "server", garde(dive))]
    pub instance: InstanceSpecsSchema,
    #[cfg_attr(feature = "server", garde(range(min = 16)))]
    pub storage: Option<u32>,
    #[cfg_attr(feature = "server", garde(range(min = 16)))]
    pub big_instance_storage: Option<u32>,
    #[cfg_attr(feature = "server", garde(range(min = 1)))]
    pub cluster_size: u32,
    #[serde(default, flatten)]
    #[cfg_attr(feature = "server", garde(skip))]
    pub mode: ClusterModeSchema,
    #[cfg_attr(feature = "server", garde(dive))]
    pub python_version: PythonVersion,
    #[cfg_attr(feature = "server", garde(skip), schema(value_type = String))]
    pub polars_version: VersionNumber,
    #[cfg_attr(feature = "server", garde(skip))]
    pub labels: Option<Vec<String>>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub log_level: LogLevelSchema,
    #[cfg_attr(feature = "server", garde(range(min = 10)))]
    pub idle_timeout_mins: Option<u32>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub requirements_txt: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "server", derive(ToSchema, Validate))]
#[serde(deny_unknown_fields)]
pub struct StartComputeClusterManifestArgs {
    #[cfg_attr(feature = "server", garde(skip))]
    pub name: String,
    #[cfg_attr(feature = "server", garde(dive))]
    pub python_version: PythonVersion,
    #[cfg_attr(feature = "server", garde(skip), schema(value_type = String))]
    pub polars_version: VersionNumber,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "server", derive(ToSchema, Validate))]
#[serde(deny_unknown_fields)]
pub struct StartComputeClusterArgs {
    #[serde(flatten)]
    #[cfg_attr(feature = "server", garde(dive))]
    pub instance: InstanceSpecsSchema,
    #[cfg_attr(feature = "server", garde(range(min = 16)))]
    pub storage: Option<u32>,
    #[cfg_attr(feature = "server", garde(range(min = 16)))]
    pub big_instance_storage: Option<u32>,
    #[cfg_attr(feature = "server", garde(range(min = 1)))]
    pub cluster_size: u32,
    #[serde(default, flatten)]
    #[cfg_attr(feature = "server", garde(skip))]
    pub mode: ClusterModeSchema,
    #[cfg_attr(feature = "server", garde(dive))]
    pub python_version: PythonVersion,
    #[cfg_attr(feature = "server", garde(skip), schema(value_type = String))]
    pub polars_version: VersionNumber,
    #[cfg_attr(feature = "server", garde(skip))]
    pub labels: Option<Vec<String>>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub log_level: Option<LogLevelSchema>,
    #[cfg_attr(feature = "server", garde(range(min = 10)))]
    pub idle_timeout_mins: Option<u32>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub requirements_txt: Option<String>,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct ComputeClusterPublicInfoSchema {
    pub cluster_id: Uuid,
    pub public_address: String,
    pub public_server_key: String,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct ComputeClusterNodeInfoSchema {
    pub cluster_id: Uuid,
    pub private_address: Option<String>,
    pub cpus: Option<i32>,
    pub memory_mb: Option<i32>,
    pub storage_mb: Option<i32>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct SupportedVersionsSchema {
    #[cfg_attr(feature="server", schema(value_type = Vec<String>))]
    pub polars: Vec<VersionNumber>,
}

impl EntityOrdering for ComputeClusterNodeInfoSchema {
    fn order_fields() -> &'static [&'static str] {
        &["cpus", "memory_mb", "storage_mb"]
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct AwsMetricSchema {
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct AwsMetricsSchema {
    pub cpu_usage_percent: AwsMetricSchema,
    pub memory_usage: AwsMetricSchema,
    pub memory_usage_percent: AwsMetricSchema,
    pub disk_usage: AwsMetricSchema,
    pub disk_usage_percent: AwsMetricSchema,
}

#[derive(Deserialize, Debug)]
pub struct TimeWindowOpt {
    pub start: Option<DateTime<FixedOffset>>,
    pub end: Option<DateTime<FixedOffset>>,
}

#[cfg_attr(feature = "pyo3", pyclass(eq, eq_int))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum DBClusterModeSchema {
    Proxy,
    Direct,
}

#[cfg_attr(feature = "pyo3", pyo3::pymethods)]
#[cfg(feature = "pyo3")]
impl DBClusterModeSchema {
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
            DBClusterModeSchema::Direct => "direct",
            DBClusterModeSchema::Proxy => "proxy",
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
#[cfg_attr(feature = "server", derive(IntoParams))]
pub struct GetClusterFilterParams {
    /// Filters out any clusters that are not in the given status.
    #[serde(default)]
    #[serde(deserialize_with = "csv_vec_opt")]
    pub status: Option<Vec<ComputeStatusSchema>>,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeTokenSchema {
    pub id: Uuid,
    pub token: String,
}

#[cfg_attr(feature = "pyo3", pyclass)]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeSchema {
    pub id: Uuid,
    pub user_id: Uuid,
    pub workspace_id: Uuid,
    pub name: Option<String>,
    pub instance_type: Option<String>,
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
    pub termination: Option<TerminationSchema>,
    pub gc_inactive_hours: i32,
    pub request_time: DateTime<Utc>,
    pub mode: DBClusterModeSchema,
    #[cfg_attr(feature="server", schema(value_type = String))]
    pub polars_version: VersionNumber,
    pub status: ComputeStatusSchema,
    pub log_level: LogLevelSchema,
}

impl EntityOrdering for ComputeSchema {
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
impl ComputeSchema {
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
    pub fn termination(&self) -> pyo3::PyResult<Option<TerminationSchema>> {
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
    pub fn mode(&self) -> pyo3::PyResult<DBClusterModeSchema> {
        Ok(self.mode)
    }

    #[getter]
    pub fn polars_version(&self) -> pyo3::PyResult<String> {
        Ok(self.polars_version.to_string())
    }

    #[getter]
    pub fn status(&self) -> pyo3::PyResult<ComputeStatusSchema> {
        Ok(self.status)
    }

    #[getter]
    pub fn log_level(&self) -> pyo3::PyResult<LogLevelSchema> {
        Ok(self.log_level.clone())
    }
}

#[cfg_attr(feature = "server", derive(ToSchema))]
#[cfg_attr(feature = "pyo3", pyclass(get_all, eq, eq_int))]
#[derive(Debug, Deserialize, Clone, Copy, Serialize, PartialEq)]
pub enum ComputeStatusSchema {
    Starting = 0,
    Idle = 1,
    Running = 2,
    Stopping = 3,
    Stopped = 4,
    Failed = 7,
}

impl Display for ComputeStatusSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComputeStatusSchema::Starting => write!(f, "Starting"),
            ComputeStatusSchema::Idle => write!(f, "Idle"),
            ComputeStatusSchema::Running => write!(f, "Running"),
            ComputeStatusSchema::Stopping => write!(f, "Stopping"),
            ComputeStatusSchema::Stopped => write!(f, "Stopped"),
            ComputeStatusSchema::Failed => write!(f, "Failed"),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct AwsLogEventSchema {
    pub timestamp: DateTime<Utc>,
    pub message: String,
}

#[cfg(feature = "server")]
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct TokenPaginated<T: Serialize + ToSchema> {
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
