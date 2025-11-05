#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::ToSchema;

#[cfg_attr(feature = "pyo3", pyclass)]
#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct Specs {
    #[cfg_attr(feature = "server", garde(range(min = 1)))]
    pub cpus: u32,
    #[cfg_attr(feature = "server", garde(range(min = 1)))]
    pub ram_gb: u32,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema, Validate))]
#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum InstanceSpecsSchema {
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
        #[cfg_attr(feature = "server", garde(range(min = 1)))]
        multiplier: Option<u32>,
    },
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct WorkspaceClusterDefaultsSchema {
    /// Instance specifications
    #[cfg_attr(feature = "server", garde(dive))]
    pub instance_specs: InstanceSpecsSchema,
    /// Amount of disk storage (in GiB)
    #[cfg_attr(feature = "server", garde(range(min = 16)))]
    pub storage: Option<i32>,
    /// Number of compute nodes
    #[cfg_attr(feature = "server", garde(range(min = 1)))]
    pub cluster_size: i32,
}
