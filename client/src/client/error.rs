use std::io;

use anyhow::anyhow;
use pyo3::PyErr;
use thiserror::Error;

use crate::error::AuthLoadError;

#[derive(Debug, Error)]
#[error(transparent)]
pub struct AuthError(#[from] Box<dyn std::error::Error + Send + Sync>);

impl From<io::Error> for AuthError {
    fn from(_error: io::Error) -> Self {
        AuthError::new("Authentication token was not found.")
    }
}

impl From<AuthError> for PyErr {
    fn from(error: AuthError) -> Self {
        AuthLoadError::new_err(format!("{error}"))
    }
}

impl AuthError {
    pub fn new(message: &str) -> Self {
        let err_msg = format!(
            "{message}\n\tLog in with `pc login` or set the environment variables.\n\tSee https://docs.pola.rs/polars-cloud/explain/authentication/ for more details."
        );
        AuthError(anyhow!(err_msg).into())
    }
}
