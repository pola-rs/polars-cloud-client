use polars_axum_models::{PythonVersion, VersionNumber};
use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi::c_str;
use pyo3::prelude::{PyDictMethods, PyStringMethods};
use pyo3::types::{PyDict, PyString};
use pyo3::{Bound, PyResult, Python};

use crate::client::Versions as VersionHeaders;

#[derive(Clone, Copy)]
pub struct Versions {
    pub python: PythonVersion,
    pub client: VersionNumber,
    pub polars: VersionNumber,
}

impl From<Versions> for VersionHeaders {
    fn from(value: Versions) -> Self {
        VersionHeaders {
            polars: value.polars.to_string().try_into().unwrap(),
            polars_cloud: value.client.to_string().try_into().unwrap(),
        }
    }
}

pub fn get_versions(py: Python) -> PyResult<Versions> {
    let locals = PyDict::new(py);
    py.run(
        c_str!(
            r#"
from importlib.metadata import version
pl_version = version("polars")
pc_version = version("polars_cloud")
            "#
        ),
        None,
        Some(&locals),
    )?;
    let polars_cloud_version: Bound<'_, PyString> =
        locals.get_item("pc_version")?.unwrap().cast_into()?;
    let polars_version: Bound<'_, PyString> =
        locals.get_item("pl_version")?.unwrap().cast_into()?;

    let python_version = {
        let version = py.version_info();
        PythonVersion {
            major: version.major,
            minor: version.minor,
            patch: version.patch,
        }
    };

    let polars_cloud_version = polars_cloud_version.to_cow()?;
    let polars_version = polars_version.to_cow()?;
    Ok(Versions {
        python: python_version,
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

pub fn get_polars_versions(py: Python) -> PyResult<(PythonVersion, VersionNumber)> {
    let locals = PyDict::new(py);
    py.run(
        c_str!(
            r#"
from importlib.metadata import version
pl_version = version("polars")
            "#
        ),
        None,
        Some(&locals),
    )?;
    let polars_version: Bound<'_, PyString> =
        locals.get_item("pl_version")?.unwrap().cast_into()?;

    let python_version = {
        let version = py.version_info();
        PythonVersion {
            major: version.major,
            minor: version.minor,
            patch: version.patch,
        }
    };

    let polars_version = polars_version.to_cow()?;
    Ok((
        python_version,
        polars_version.parse().map_err(|_| {
            PyRuntimeError::new_err(format!("Unsupported version of polars: {polars_version}"))
        })?,
    ))
}
