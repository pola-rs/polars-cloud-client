#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::DBCPUArchitectureModel;

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema, Validate))]
#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum InstanceSpecsModel {
    InstanceType {
        #[cfg_attr(feature = "server", garde(skip))]
        standard: String,
        #[cfg_attr(feature = "server", garde(skip))]
        big: Option<String>,
    },
    Specs {
        #[cfg_attr(feature = "server", garde(range(min = 1)))]
        cpus: u32,
        #[cfg_attr(feature = "server", garde(range(min = 1)))]
        ram_gb: u32,
        #[serde(default = "DBCPUArchitectureModel::defaults")]
        #[cfg_attr(feature = "server", garde(length(min = 1)))]
        cpu_architectures: Vec<DBCPUArchitectureModel>,
        #[cfg_attr(feature = "server", garde(range(min = 1)))]
        multiplier: Option<u32>,
    },
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct WorkspaceClusterDefaultsModel {
    /// Instance specifications
    #[cfg_attr(feature = "server", garde(dive))]
    pub instance_specs: InstanceSpecsModel,
    /// Amount of disk storage (in GiB)
    #[cfg_attr(feature = "server", garde(range(min = 16)))]
    pub storage: Option<i32>,
    /// Number of compute nodes
    #[cfg_attr(feature = "server", garde(range(min = 1)))]
    pub cluster_size: i32,
}
