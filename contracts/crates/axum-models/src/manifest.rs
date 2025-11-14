#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::IntoParams;
#[cfg(feature = "server")]
use utoipa::ToSchema;
use uuid::Uuid;
use version_number::VersionNumber;

#[cfg(feature = "server")]
use crate::common::validate_alphanumeric_name;
use crate::{
    DBClusterModeSchema, EntityOrdering, InstanceSpecsSchema, LogLevelSchema, PythonVersion,
};

#[derive(Default, Debug, Deserialize)]
#[cfg_attr(feature = "server", derive(Validate, IntoParams))]
#[cfg_attr(feature="server", into_params(parameter_in = Query))]
pub struct ManifestQuery {
    #[cfg_attr(
        feature = "server",
        garde(length(min = 3, max = 32), custom(validate_alphanumeric_name))
    )]
    pub name: String,
}

#[cfg_attr(feature = "pyo3", pyclass)]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestSchema {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub instance_type: Option<String>,
    pub big_instance_type: Option<String>,
    pub req_ram_gb: Option<u32>,
    pub req_cpu_cores: Option<u32>,
    pub req_storage: Option<i32>,
    pub req_big_instance_multiplier: Option<u32>,
    pub req_big_instance_storage: Option<i32>,
    pub cluster_size: u32,
    pub mode: DBClusterModeSchema,
    pub idle_timeout_mins: Option<i32>,
    #[cfg_attr(feature="server", schema(value_type = String))]
    pub polars_version: VersionNumber,
    pub python_version: String,
    pub log_level: LogLevelSchema,
    pub requirements_txt: Option<String>,
}

#[cfg_attr(feature = "pyo3", pyo3::pymethods)]
#[cfg(feature = "pyo3")]
impl ManifestSchema {
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
    pub fn mode(&self) -> pyo3::PyResult<DBClusterModeSchema> {
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
    pub fn log_level(&self) -> pyo3::PyResult<LogLevelSchema> {
        Ok(self.log_level.clone())
    }

    #[getter]
    pub fn requirements_txt(&self) -> pyo3::PyResult<Option<String>> {
        Ok(self.requirements_txt.clone())
    }
}

impl EntityOrdering for ManifestSchema {
    fn order_fields() -> &'static [&'static str] {
        &[
            "name",
            "id",
            "cluster_size",
            "req_storage",
            "req_ram_gb",
            "req_cpu_cores",
        ]
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "server", derive(ToSchema, Validate))]
#[serde(deny_unknown_fields)]
pub struct PatchManifestArgs {
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
    #[cfg_attr(feature = "server", garde(skip))]
    pub mode: DBClusterModeSchema,
    #[cfg_attr(feature = "server", garde(dive))]
    pub python_version: PythonVersion,
    #[cfg_attr(feature = "server", garde(skip), schema(value_type = String))]
    pub polars_version: VersionNumber,
    #[cfg_attr(feature = "server", garde(skip))]
    pub log_level: LogLevelSchema,
    #[cfg_attr(feature = "server", garde(range(min = 10)))]
    pub idle_timeout_mins: Option<u32>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub requirements_txt: Option<String>,
}
