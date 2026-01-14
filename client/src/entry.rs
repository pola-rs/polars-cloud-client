use pyo3::marker::Ungil;
use pyo3::prelude::*;

use crate::error::ApiError;

pub trait EnterRustExt {
    /// Whenever you have a block of code in the public Python API that
    /// (potentially) takes a long time, wrap it in enter_rust. This will
    /// ensure we release the GIL.
    ///
    /// This not only can increase performance and usability, it can avoid
    /// deadlocks on the GIL for Python reentrance.
    fn enter_rust<T, E, F>(self, f: F) -> Result<T, ApiError>
    where
        F: Ungil + Send + FnOnce() -> Result<T, E>,
        T: Ungil + Send,
        E: Ungil + Send + Into<ApiError>;

    fn enter_rust_ok<T, F>(self, f: F) -> Result<T, ApiError>
    where
        Self: Sized,
        F: Ungil + Send + FnOnce() -> T,
        T: Ungil + Send,
    {
        self.enter_rust(move || Result::<T, ApiError>::Ok(f()))
    }
}

impl EnterRustExt for Python<'_> {
    fn enter_rust<T, E, F>(self, f: F) -> Result<T, ApiError>
    where
        F: Ungil + Send + FnOnce() -> Result<T, E>,
        T: Ungil + Send,
        E: Ungil + Send + Into<ApiError>,
    {
        self.detach(f).map_err(|err| err.into())
    }
}
