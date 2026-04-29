#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use version_number::VersionNumber;

#[cfg(feature = "server")]
use crate::common::validate_alphanumeric_name;
use crate::{
    DBCPUArchitectureModel, DBClusterModeModel, DefaultSortDirection, EntityOrdering,
    InstanceSpecsModel, LogLevelModel, PythonVersion,
};

#[derive(Default, Debug, Deserialize)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
pub struct ManifestQueryArgs {
    #[cfg_attr(
        feature = "server",
        garde(length(min = 3, max = 32), custom(validate_alphanumeric_name))
    )]
    pub name: String,
}

#[cfg_attr(feature = "pyo3", pyclass)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestModel {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub instance_type: Option<String>,
    pub cpu_architectures: Option<Vec<DBCPUArchitectureModel>>,
    pub big_instance_type: Option<String>,
    pub req_ram_gb: Option<u32>,
    pub req_cpu_cores: Option<u32>,
    pub req_storage: Option<i32>,
    pub req_big_instance_multiplier: Option<u32>,
    pub req_big_instance_storage: Option<i32>,
    pub cluster_size: u32,
    pub mode: DBClusterModeModel,
    pub idle_timeout_mins: Option<i32>,
    #[cfg_attr(feature = "server", schemars(with = "String"))]
    pub polars_version: VersionNumber,
    pub python_version: String,
    pub log_level: LogLevelModel,
    pub requirements_txt: Option<String>,
    /// ID of the cluster for this manifest if one is active
    pub live_cluster_id: Option<Uuid>,
}

#[cfg_attr(feature = "pyo3", pyo3::pymethods)]
#[cfg(feature = "pyo3")]
impl ManifestModel {
    #[getter]
    pub fn id(&self) -> pyo3::PyResult<Uuid> {
        Ok(self.id)
    }

    #[getter]
    pub fn workspace_id(&self) -> pyo3::PyResult<Uuid> {
        Ok(self.workspace_id)
    }

    #[getter]
    pub fn name(&self) -> pyo3::PyResult<&str> {
        Ok(self.name.as_ref())
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
    pub fn cpu_architectures(&self) -> pyo3::PyResult<Option<Vec<DBCPUArchitectureModel>>> {
        Ok(self.cpu_architectures.clone())
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
    pub fn cluster_size(&self) -> pyo3::PyResult<u32> {
        Ok(self.cluster_size)
    }

    #[getter]
    pub fn mode(&self) -> pyo3::PyResult<DBClusterModeModel> {
        Ok(self.mode)
    }

    #[getter]
    pub fn idle_timeout_mins(&self) -> pyo3::PyResult<Option<i32>> {
        Ok(self.idle_timeout_mins)
    }

    #[getter]
    pub fn polars_version(&self) -> pyo3::PyResult<String> {
        Ok(self.polars_version.to_string())
    }

    #[getter]
    pub fn python_version(&self) -> pyo3::PyResult<&str> {
        Ok(self.python_version.as_ref())
    }

    #[getter]
    pub fn log_level(&self) -> pyo3::PyResult<LogLevelModel> {
        Ok(self.log_level.clone())
    }

    #[getter]
    pub fn requirements_txt(&self) -> pyo3::PyResult<Option<String>> {
        Ok(self.requirements_txt.clone())
    }

    #[getter]
    pub fn live_cluster_id(&self) -> pyo3::PyResult<Option<Uuid>> {
        Ok(self.live_cluster_id)
    }
}

impl EntityOrdering for ManifestModel {
    fn order_fields() -> &'static [&'static str] {
        &[
            "id",
            "name",
            "cluster_size",
            "req_storage",
            "req_ram_gb",
            "req_cpu_cores",
        ]
    }

    fn default_ordering() -> Option<(&'static str, DefaultSortDirection)> {
        Some(("id", DefaultSortDirection::Desc))
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "server", derive(JsonSchema, Validate))]
#[serde(deny_unknown_fields)]
pub struct PatchManifestArgs {
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
    #[cfg_attr(feature = "server", garde(skip))]
    pub mode: DBClusterModeModel,
    #[cfg_attr(feature = "server", garde(dive))]
    pub python_version: PythonVersion,
    #[cfg_attr(feature = "server", garde(skip), schemars(with = "String"))]
    pub polars_version: VersionNumber,
    #[cfg_attr(feature = "server", garde(skip))]
    pub log_level: LogLevelModel,
    #[cfg_attr(feature = "server", garde(range(min = 10)))]
    pub idle_timeout_mins: Option<u32>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub requirements_txt: Option<String>,
}
