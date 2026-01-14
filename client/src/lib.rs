mod aws;
pub mod client;
mod compute;
mod constants;
mod entry;
mod error;
mod organization;
mod query;
mod query_control_grpc;
pub mod query_grpc;
mod query_settings;
mod runtime;
mod serde_types;
mod service_account;
pub mod user;
mod workspace;

use std::sync::OnceLock;

use client::{polars_version, py_is_token_expired, python_version};
use polars_axum_models::{
    ComputeClusterPublicInfoSchema, ComputeSchema, ComputeStatusSchema, ComputeTokenSchema,
    DBClusterModeSchema, DeleteWorkspaceSchema, FileTypeSchema, LogLevelSchema, OrganizationSchema,
    QueryPlansSchema, QuerySchema, QueryStateTimingSchema, QueryStatusCodeSchema,
    QueryWithStateTimingAndResultSchema, QueryWithStateTimingSchema, QueryWithStatusSchema,
    ResultSchema, StatusSchema, TerminationReasonSchema, TerminationSchema, VersionNumber,
    WorkspaceSchema, WorkspaceSetupUrlSchema, WorkspaceStateSchema, WorkspaceWithUrlSchema,
};
use polars_backend_client::client::Versions as VersionHeaders;
use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};

use self::query_settings::PyShuffleOpts;
use crate::client::WrappedAPIClient;
use crate::error::{AuthLoadError, EncodedPolarsError, NotFoundError};
use crate::query_grpc::*;
use crate::query_settings::PyQuerySettings;
use crate::workspace::*;

#[derive(Clone, Copy)]
struct Versions {
    client: VersionNumber,
    polars: VersionNumber,
}

impl From<Versions> for VersionHeaders {
    fn from(value: Versions) -> Self {
        VersionHeaders {
            polars: value.polars.to_string().try_into().unwrap(),
            polars_cloud: value.client.to_string().try_into().unwrap(),
        }
    }
}

static VERSIONS: OnceLock<Option<(Versions, VersionHeaders)>> = OnceLock::new();

fn get_versions(py: Python) -> PyResult<Versions> {
    let locals = PyDict::new(py);
    py.run(
        c_str!(
            r#"
from importlib.metadata import version
pc_version = version("polars_cloud")
pl_version = version("polars")
            "#
        ),
        None,
        Some(&locals),
    )?;
    let polars_cloud_version: Bound<'_, PyString> =
        locals.get_item("pc_version")?.unwrap().cast_into()?;
    let polars_version: Bound<'_, PyString> =
        locals.get_item("pl_version")?.unwrap().cast_into()?;
    let polars_cloud_version = polars_cloud_version.to_cow()?;
    let polars_version = polars_version.to_cow()?;
    Ok(Versions {
        polars: polars_version.parse().map_err(|_| {
            PyRuntimeError::new_err(format!("Unsupported version of polars: {polars_version}"))
        })?,
        client: polars_cloud_version.parse().map_err(|_| {
            PyRuntimeError::new_err(format!(
                "Unsupported version of polars_cloud: {polars_cloud_version}"
            ))
        })?,
    })
}

#[pymodule]
fn polars_cloud(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    let mut err = Ok(());
    VERSIONS.get_or_init(|| match get_versions(py) {
        Err(e) => {
            err = Err(e);
            None
        },
        Ok(v) => Some((v, v.into())),
    });
    err?;

    m.add_class::<PyShuffleOpts>().unwrap();
    m.add_class::<PyQuerySettings>().unwrap();
    m.add_class::<WrappedAPIClient>().unwrap();
    m.add_class::<SchedulerClient>().unwrap();

    m.add_class::<WorkspaceSchema>().unwrap();
    m.add_class::<WorkspaceStateSchema>().unwrap();
    m.add_class::<DefaultComputeSpecs>().unwrap();

    m.add_class::<QuerySchema>().unwrap();
    m.add_class::<QueryPlansSchema>().unwrap();
    m.add_class::<QueryStatusCodeSchema>().unwrap();
    m.add_class::<StatusSchema>().unwrap();
    m.add_class::<QueryWithStatusSchema>().unwrap();
    m.add_class::<QueryStateTimingSchema>().unwrap();
    m.add_class::<QueryWithStateTimingSchema>().unwrap();
    m.add_class::<FileTypeSchema>().unwrap();
    m.add_class::<ResultSchema>().unwrap();
    m.add_class::<QueryWithStateTimingAndResultSchema>()
        .unwrap();

    m.add_class::<TerminationReasonSchema>().unwrap();
    m.add_class::<TerminationSchema>().unwrap();
    m.add_class::<DBClusterModeSchema>().unwrap();
    m.add_class::<ComputeSchema>().unwrap();
    m.add_class::<ComputeClusterPublicInfoSchema>().unwrap();
    m.add_class::<ComputeStatusSchema>().unwrap();
    m.add_class::<ComputeTokenSchema>().unwrap();

    m.add_class::<WorkspaceWithUrlSchema>().unwrap();
    m.add_class::<WorkspaceSetupUrlSchema>().unwrap();
    m.add_class::<DeleteWorkspaceSchema>().unwrap();
    m.add_class::<LogLevelSchema>().unwrap();

    m.add_class::<OrganizationSchema>().unwrap();

    m.add_class::<ClientOptions>().unwrap();

    m.add_class::<QueryPlansPy>().unwrap();
    m.add_class::<PlanFormatPy>().unwrap();

    m.add("NotFoundError", m.py().get_type::<NotFoundError>())
        .unwrap();

    m.add("AuthLoadError", m.py().get_type::<AuthLoadError>())
        .unwrap();

    m.add(
        "EncodedPolarsError",
        m.py().get_type::<EncodedPolarsError>(),
    )
    .unwrap();

    m.add_wrapped(wrap_pyfunction!(serde_types::serialize_query_settings))
        .unwrap();
    m.add_wrapped(wrap_pyfunction!(py_is_token_expired))
        .unwrap();

    m.add_wrapped(wrap_pyfunction!(polars_version)).unwrap();
    m.add_wrapped(wrap_pyfunction!(python_version)).unwrap();

    Ok(())
}
