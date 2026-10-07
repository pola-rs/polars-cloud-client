#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use version_number::VersionNumber;

use crate::PythonVersion;

/// Infrastructure a self-registering cluster runs on.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub enum OnPremDeploymentModel {
    Kubernetes,
    Ray,
    BareMetal,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "server", derive(JsonSchema, Validate))]
#[serde(deny_unknown_fields)]
pub struct RegisterComputeClusterArgs {
    #[cfg_attr(feature = "server", garde(skip))]
    pub cluster_size: u32,
    #[cfg_attr(feature = "server", garde(skip))]
    // Skip validation as this is not important for on-prem clusters
    pub python_version: PythonVersion,
    #[cfg_attr(feature = "server", garde(skip), schemars(with = "String"))]
    pub polars_version: VersionNumber,
    #[cfg_attr(feature = "server", garde(skip))]
    pub labels: Option<Vec<String>>,
    #[cfg_attr(feature = "server", garde(skip), schemars(with = "String"))]
    pub compute_plane_version: semver::Version,
    #[cfg_attr(feature = "server", garde(skip))]
    pub cluster_id: String,
    /// Missing for binaries that predate it, which count as `BareMetal`.
    #[cfg_attr(feature = "server", garde(skip))]
    pub deployment: Option<OnPremDeploymentModel>,
}
