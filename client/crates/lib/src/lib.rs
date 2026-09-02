mod cloud_export;
mod entry;
mod flight;
pub mod query_scheduler;
mod query_settings;
mod serde_types;
mod tracing;
mod utils;
mod wrapped_client;

use std::sync::Arc;

use client_core::utils::{polars_version, py_is_token_expired, python_version};
use client_core::{
    AuthLoadError, AutoRefreshApiControlPlaneClient, ComputeClusterMisspecified,
    ComputeContextSpecs, EncodedPolarsError, NotFoundError, RUNTIME, VERSIONS, get_versions,
};
use polars_axum_models::{
    AwsConnectionStatusModel, ComputeClusterNodeInfoModel, ComputeClusterPublicInfoModel,
    ComputeModel, ComputeStatusModel, ComputeTokenModel, DBCPUArchitectureModel,
    DBClusterModeModel, DeleteWorkspaceModel, FileTypeModel, LogLevelModel, ManifestModel,
    OrganizationModel, OrganizationSubscriptionStateModel, OrganizationTierModel, QueryEngineModel,
    QueryModel, QueryStateTimingModel, QueryStatusCodeModel, QueryTypeModel,
    QueryWithStateTimingAndResultModel, ResultModel, StatusModel, TerminationModel,
    TerminationReasonModel, UserModel, WorkspaceAPITokenModel, WorkspaceApiTokenWithNameModel,
    WorkspaceAwsConnectionModel, WorkspaceAwsStackModel, WorkspaceClusterDefaultsModel,
    WorkspaceDeploymentModel, WorkspaceModel, WorkspaceSetupUrlModel, WorkspaceStateModel,
    WorkspaceWithUrlModel,
};
use pyo3::exceptions::PySystemExit;
use pyo3::prelude::*;

use self::query_settings::PyShuffleOpts;
use crate::cloud_export::{QueryCloudObserver, QueryMetricPoller};
use crate::flight::FlightResult;
use crate::query_scheduler::*;
use crate::query_settings::{PyLineageContext, PyNumWorkers, PyQuerySettings};
use crate::serde_types::{QueryDetailPy, QueryInfoPy, QueryPlanTimingPy, StageStatsPy};
use crate::tracing::{TraceSpan, flush_traces, init_from_env};
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
    init_from_env();

    let mut err = Ok(());
    VERSIONS.get_or_init(|| match get_versions(py) {
        Err(e) => {
            err = Err(e);
            None
        },
        Ok(v) => Some((v, v.into())),
    });
    err?;

    m.add_class::<AwsConnectionStatusModel>()?;
    m.add_class::<ClientOptions>()?;
    m.add_class::<ComputeClusterNodeInfoModel>()?;
    m.add_class::<ComputeClusterPublicInfoModel>()?;
    m.add_class::<ComputeContextSpecs>()?;
    m.add_class::<ComputeModel>()?;
    m.add_class::<ComputeStatusModel>()?;
    m.add_class::<ComputeTokenModel>()?;
    m.add_class::<ComputeVersionsPy>()?;
    m.add_class::<DBCPUArchitectureModel>()?;
    m.add_class::<DBClusterModeModel>()?;
    m.add_class::<DefaultComputeSpecs>()?;
    m.add_class::<DeleteWorkspaceModel>()?;
    m.add_class::<FileTypeModel>()?;
    m.add_class::<FlightResult>()?;
    m.add_class::<LogLevelModel>()?;
    m.add_class::<ManifestModel>()?;
    m.add_class::<OrganizationModel>()?;
    m.add_class::<OrganizationSubscriptionStateModel>()?;
    m.add_class::<OrganizationTierModel>()?;
    m.add_class::<PlanFormatPy>()?;
    m.add_class::<TraceSpan>()?;
    m.add_class::<PyLineageContext>()?;
    m.add_class::<PyNumWorkers>()?;
    m.add_class::<PyQuerySettings>()?;
    m.add_class::<PyShuffleOpts>()?;
    m.add_class::<QueryCloudObserver>()?;
    m.add_class::<QueryDetailPy>()?;
    m.add_class::<QueryEngineModel>()?;
    m.add_class::<QueryInfoPy>()?;
    m.add_class::<QueryMetricPoller>()?;
    m.add_class::<QueryModel>()?;
    m.add_class::<QueryPlanTimingPy>()?;
    m.add_class::<QueryPlansPy>()?;
    m.add_class::<QueryStateTimingModel>()?;
    m.add_class::<QueryStatusCodeModel>()?;
    m.add_class::<QueryTypeModel>()?;
    m.add_class::<QueryWithStateTimingAndResultModel>()?;
    m.add_class::<ResultModel>()?;
    m.add_class::<SchedulerClient>()?;
    m.add_class::<StageStatsPy>()?;
    m.add_class::<StatusModel>()?;
    m.add_class::<TLSOptions>()?;
    m.add_class::<TerminationModel>()?;
    m.add_class::<TerminationReasonModel>()?;
    m.add_class::<UserModel>()?;
    m.add_class::<WorkspaceAPITokenModel>()?;
    m.add_class::<WorkspaceApiTokenWithNameModel>()?;
    m.add_class::<WorkspaceAwsConnectionModel>()?;
    m.add_class::<WorkspaceAwsStackModel>()?;
    m.add_class::<WorkspaceClusterDefaultsModel>()?;
    m.add_class::<WorkspaceDeploymentModel>()?;
    m.add_class::<WorkspaceModel>()?;
    m.add_class::<WorkspaceSetupUrlModel>()?;
    m.add_class::<WorkspaceStateModel>()?;
    m.add_class::<WorkspaceWithUrlModel>()?;
    m.add_class::<WrappedAPIClient>()?;

    m.add("NotFoundError", m.py().get_type::<NotFoundError>())?;

    m.add("AuthLoadError", m.py().get_type::<AuthLoadError>())?;

    m.add(
        "EncodedPolarsError",
        m.py().get_type::<EncodedPolarsError>(),
    )?;

    m.add(
        "ComputeClusterMisspecified",
        m.py().get_type::<ComputeClusterMisspecified>(),
    )?;

    m.add_function(wrap_pyfunction!(serde_types::serialize_query_settings, m)?)?;
    m.add_function(wrap_pyfunction!(flush_traces, m)?)?;
    m.add_function(wrap_pyfunction!(py_is_token_expired, m)?)?;
    m.add_function(wrap_pyfunction!(polars_version, m)?)?;
    m.add_function(wrap_pyfunction!(python_version, m)?)?;
    m.add_function(wrap_pyfunction!(utils::resolve_compute_context_specs, m)?)?;

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
    result.map_err(|e| PySystemExit::new_err(format!("{e}")))
}
