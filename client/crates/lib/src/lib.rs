mod entry;
pub mod query_scheduler;
mod query_settings;
mod serde_types;
mod utils;
mod wrapped_client;

use std::sync::Arc;

use client_core::utils::{polars_version, py_is_token_expired, python_version};
use client_core::{
    AuthLoadError, AutoRefreshApiControlPlaneClient, ComputeClusterMisspecified,
    ComputeContextSpecs, EncodedPolarsError, NotFoundError, RUNTIME, VERSIONS, get_versions,
};
use polars_axum_models::{
    ComputeClusterPublicInfoModel, ComputeModel, ComputeStatusModel, ComputeTokenModel,
    DBCPUArchitectureModel, DBClusterModeModel, DeleteWorkspaceModel, FileTypeModel, LogLevelModel,
    OrganizationModel, QueryModel, QueryPlansModel, QueryStateTimingModel, QueryStatusCodeModel,
    QueryWithStateTimingAndResultModel, QueryWithStateTimingModel, QueryWithStatusModel,
    ResultModel, StatusModel, TerminationModel, TerminationReasonModel, WorkspaceModel,
    WorkspaceSetupUrlModel, WorkspaceStateModel, WorkspaceWithUrlModel,
};
use pyo3::exceptions::PySystemExit;
use pyo3::prelude::*;

use self::query_settings::PyShuffleOpts;
use crate::query_scheduler::*;
use crate::query_settings::{PyLineageContext, PyQuerySettings};
use crate::serde_types::{QueryDetailPy, QueryPlanTimingPy};
use crate::wrapped_client::WrappedAPIClient;
use crate::wrapped_client::workspace::DefaultComputeSpecs;

pub static CTRL_PLN_CLIENT_GLOBAL: std::sync::LazyLock<Arc<AutoRefreshApiControlPlaneClient>> =
    std::sync::LazyLock::new(|| {
        // `connect_lazy()` in tonic calls `tokio::task::spawn` internally, so we need to enter the runtime.
        let _guard = RUNTIME.0.enter();
        Arc::new(AutoRefreshApiControlPlaneClient::default())
    });

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

    m.add_class::<WorkspaceModel>().unwrap();
    m.add_class::<WorkspaceStateModel>().unwrap();
    m.add_class::<DefaultComputeSpecs>().unwrap();

    m.add_class::<QueryModel>().unwrap();
    m.add_class::<QueryPlansModel>().unwrap();
    m.add_class::<QueryStatusCodeModel>().unwrap();
    m.add_class::<StatusModel>().unwrap();
    m.add_class::<QueryWithStatusModel>().unwrap();
    m.add_class::<QueryStateTimingModel>().unwrap();
    m.add_class::<QueryWithStateTimingModel>().unwrap();
    m.add_class::<FileTypeModel>().unwrap();
    m.add_class::<ResultModel>().unwrap();
    m.add_class::<QueryWithStateTimingAndResultModel>().unwrap();

    m.add_class::<TerminationReasonModel>().unwrap();
    m.add_class::<TerminationModel>().unwrap();
    m.add_class::<DBClusterModeModel>().unwrap();
    m.add_class::<DBCPUArchitectureModel>().unwrap();
    m.add_class::<ComputeModel>().unwrap();
    m.add_class::<ComputeClusterPublicInfoModel>().unwrap();
    m.add_class::<ComputeStatusModel>().unwrap();
    m.add_class::<ComputeTokenModel>().unwrap();

    m.add_class::<WorkspaceWithUrlModel>().unwrap();
    m.add_class::<WorkspaceSetupUrlModel>().unwrap();
    m.add_class::<DeleteWorkspaceModel>().unwrap();
    m.add_class::<LogLevelModel>().unwrap();

    m.add_class::<OrganizationModel>().unwrap();

    m.add_class::<ClientOptions>().unwrap();

    m.add_class::<ComputeContextSpecs>().unwrap();

    m.add_class::<QueryPlanTimingPy>().unwrap();
    m.add_class::<QueryDetailPy>().unwrap();

    m.add_class::<QueryPlansPy>().unwrap();
    m.add_class::<PlanFormatPy>().unwrap();
    m.add_class::<ComputeVersionsPy>().unwrap();

    m.add_class::<PyLineageContext>().unwrap();

    m.add("NotFoundError", m.py().get_type::<NotFoundError>())
        .unwrap();

    m.add("AuthLoadError", m.py().get_type::<AuthLoadError>())
        .unwrap();

    m.add(
        "EncodedPolarsError",
        m.py().get_type::<EncodedPolarsError>(),
    )
    .unwrap();

    m.add(
        "ComputeClusterMisspecified",
        m.py().get_type::<ComputeClusterMisspecified>(),
    )
    .unwrap();

    m.add_wrapped(wrap_pyfunction!(serde_types::serialize_query_settings))
        .unwrap();
    m.add_wrapped(wrap_pyfunction!(py_is_token_expired))
        .unwrap();

    m.add_wrapped(wrap_pyfunction!(polars_version)).unwrap();
    m.add_wrapped(wrap_pyfunction!(python_version)).unwrap();
    m.add_wrapped(wrap_pyfunction!(utils::resolve_compute_context_specs))
        .unwrap();

    m.add_function(wrap_pyfunction!(cli_main, m)?)?;

    Ok(())
}

#[pyfunction]
fn cli_main(py: Python) -> PyResult<()> {
    let args = py
        .import("sys")?
        .getattr("argv")?
        .extract::<Vec<String>>()?;
    let result = py.detach(move || client_cli::entrypoint(args));
    result.map_err(|e| PySystemExit::new_err(format!("{e:#}")))
}
