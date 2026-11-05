use std::error::Error;
use std::fmt::Display;

use polars_axum_models::ErrorResponse;
use protos_common::tonic::{self, Code, Status};
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::{PyErr, Python, create_exception};
use reqwest::{StatusCode, Url};
use thiserror::Error;

use crate::{AuthError, AuthMethod};

create_exception!(polars_nanook, NotFoundError, PyException);
create_exception!(polars_nanook, AuthLoadError, PyException);
create_exception!(polars_nanook, ComputeClusterMisspecified, PyException);
create_exception!(polars_nanook, EncodedPolarsError, PyException);

type DynError = Box<dyn std::error::Error + Send + Sync>;

/// Only print your own error as the source error is printed in the cause chain
#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Error calling REST endpoint")]
    ReqwestError(#[from] reqwest::Error),
    #[error("Error calling REST endpoint")]
    MiddlewareError(#[from] reqwest_middleware::Error),
    #[error(fmt = fmt_status_error)]
    StatusError {
        status: StatusCode,
        url: Url,
        body: String,
    },
    #[error(transparent)]
    PyErr(#[from] PyErr),
    #[error(
        "{0}\n\tLog in with `nanook login` or set the environment variables.\n\tSee https://docs.pola.rs/polars-cloud/explain/authentication/ for more details."
    )]
    AuthLoadError(DynError),
    #[error(transparent)]
    ComputeClusterMisspecified(DynError),
    #[error(
        "Error setting up gRPC connection\nHint: you may need to restart the query if this error persists"
    )]
    GRPCTransportError(#[from] tonic::transport::Error),
    #[error(transparent)]
    GRPCError(GrpcStatus),
    #[error(transparent)]
    SchedulerConnectionError(#[from] SchedulerTransportError),
    #[error(transparent)]
    UuidParsingError(#[from] uuid::Error),
    // Note: `transparent` forwards `{:#}`, which makes anyhow print its whole chain.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// The server's message first, then where it came from. The server sends no error chain, so
/// this is a single level.
fn fmt_status_error(
    status: &StatusCode,
    url: &Url,
    body: &str,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    let Ok(response) = serde_json::from_str::<ErrorResponse>(body) else {
        write!(f, "{status} from {url}")?;
        let body = body.trim();
        if !body.is_empty() {
            write!(f, ": {body}")?;
        }
        return Ok(());
    };

    write!(f, "{} ({status} from {url})", response.message)?;
    let mut errors: Vec<_> = response.errors.iter().collect();
    errors.sort();
    for (field, messages) in errors {
        write!(f, "\n\t{field}: {}", messages.join("; "))?;
    }
    Ok(())
}

/// A gRPC status whose source chain is the status's own source, so the status itself is not a
/// level in the chain.
#[derive(Debug)]
pub struct GrpcStatus(Box<Status>);

impl GrpcStatus {
    /// The details of an `InvalidArgument` status that carries an encoded Polars error.
    fn encoded_polars_error(&self) -> Option<&str> {
        if self.0.code() != Code::InvalidArgument {
            return None;
        }
        str::from_utf8(self.0.details())
            .ok()
            .filter(|s| s.split_once(":").is_some())
    }
}

impl Display for GrpcStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(encoded) = self.encoded_polars_error() {
            return f.write_str(encoded);
        }
        write!(f, "Error calling gRPC endpoint, {}", self.0.code())?;
        // With a source, tonic copied the message from that source chain.
        if self.0.source().is_none() {
            write!(f, ", {}", self.0.message())?;
        }
        Ok(())
    }
}

impl Error for GrpcStatus {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.source()
    }
}

impl ApiError {
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            ApiError::ReqwestError(err) => err.status(),
            ApiError::StatusError { status, .. } => Some(*status),
            _ => None,
        }
    }
}

impl From<Status> for ApiError {
    fn from(value: Status) -> Self {
        Self::GRPCError(GrpcStatus(Box::new(value)))
    }
}

impl ApiError {
    pub(crate) fn from_with_auth_method(
        value: polars_backend_client::error::ApiError,
        auth_method: Option<AuthMethod>,
    ) -> Self {
        use polars_backend_client::error::ApiError::*;
        let method = auth_method
            .map(|m| m.to_string())
            .unwrap_or("Unknown".into());
        match value {
            ReqwestError(e) if e.status() == Some(StatusCode::UNAUTHORIZED) => {
                ApiError::StatusError {
                    status: StatusCode::UNAUTHORIZED,
                    url: e
                        .url()
                        .cloned()
                        .unwrap_or("https://pola.rs".parse().unwrap()),
                    body: format!("Authentication method {method} failed: {e}"),
                }
            },
            StatusError { status, url, .. } if status == StatusCode::UNAUTHORIZED => {
                ApiError::StatusError {
                    status,
                    url,
                    body: format!("Authentication method {method} failed."),
                }
            },
            e => ApiError::from(e),
        }
    }
}

impl From<polars_backend_client::error::ApiError> for ApiError {
    fn from(value: polars_backend_client::error::ApiError) -> Self {
        match value {
            polars_backend_client::error::ApiError::ReqwestError(e) => ApiError::ReqwestError(e),
            polars_backend_client::error::ApiError::MiddlewareError(e) => {
                ApiError::MiddlewareError(e)
            },
            polars_backend_client::error::ApiError::StatusError { status, url, body } => {
                ApiError::StatusError { status, url, body }
            },
        }
    }
}

impl From<ApiError> for PyErr {
    fn from(error: ApiError) -> Self {
        let py_err = match error {
            ApiError::PyErr(error) => return error,
            ApiError::StatusError { status, .. } if status == StatusCode::NOT_FOUND => {
                NotFoundError::new_err(error.to_string())
            },
            ApiError::GRPCError(ref status)
                if let Some(encoded) = status.encoded_polars_error() =>
            {
                EncodedPolarsError::new_err(encoded.to_owned())
            },
            ApiError::AuthLoadError(..) => AuthLoadError::new_err(error.to_string()),
            ApiError::ComputeClusterMisspecified(..) => {
                ComputeClusterMisspecified::new_err(error.to_string())
            },
            ApiError::StatusError { .. } | ApiError::UuidParsingError(..) | ApiError::Other(..) => {
                PyValueError::new_err(error.to_string())
            },
            ApiError::ReqwestError(..)
            | ApiError::MiddlewareError(..)
            | ApiError::GRPCTransportError(..)
            | ApiError::SchedulerConnectionError(..)
            | ApiError::GRPCError(..) => PyRuntimeError::new_err(error.to_string()),
        };

        let mut causes: Vec<String> = std::iter::successors(error.source(), |&e| e.source())
            .map(ToString::to_string)
            .collect();
        causes.dedup();
        Python::attach(|py| {
            let cause = causes
                .into_iter()
                .map(PyRuntimeError::new_err)
                .rev()
                .reduce(|cause, err| {
                    err.set_cause(py, Some(cause));
                    err
                });
            py_err.set_cause(py, cause);
        });
        py_err
    }
}

impl From<AuthError> for ApiError {
    fn from(value: AuthError) -> Self {
        ApiError::AuthLoadError(value.into())
    }
}

/// Scheduler context for a transport failure, preserving tonic's source-chain classification.
#[derive(Error, Debug)]
#[error(
    "Could not communicate with the compute cluster's scheduler. \
     The cluster may have stopped or restarted, or the connection may be blocked.\n\
     Check that the cluster is running and that its scheduler address, network access, \
     and TLS settings are correct. If the cluster restarted, reconnect to it."
)]
pub struct SchedulerTransportError(#[source] pub tonic::transport::Error);

/// Observatory context for a failure to reach it, keeping the reqwest error as its source.
#[derive(Error, Debug)]
#[error(
    "Could not reach the cluster's observatory at {}.\n\
     If it does not run on the scheduler host at port 3001, pass \
     `observatory=ClientOptions(...)` to `ClusterContext`.",
    url.origin().ascii_serialization()
)]
pub struct ObservatoryTransportError {
    pub url: Url,
    #[source]
    pub source: reqwest::Error,
}

pub type ApiResult<T> = std::result::Result<T, ApiError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn status_error(status: StatusCode, body: &str) -> String {
        ApiError::StatusError {
            status,
            url: "https://api.nanook.pola.rs/api/v1/workspace"
                .parse()
                .unwrap(),
            body: body.to_owned(),
        }
        .to_string()
    }

    #[test]
    fn status_error_shows_the_server_message() {
        assert_eq!(
            status_error(
                StatusCode::NOT_FOUND,
                r#"{"message": "Workspace not found", "errors": {}}"#
            ),
            "Workspace not found (404 Not Found from https://api.nanook.pola.rs/api/v1/workspace)"
        );
    }

    #[test]
    fn status_error_lists_field_errors_in_order() {
        assert_eq!(
            status_error(
                StatusCode::BAD_REQUEST,
                r#"{"message": "Invalid request", "errors": {"name": ["too long", "has spaces"], "id": ["missing"]}}"#
            ),
            "Invalid request (400 Bad Request from https://api.nanook.pola.rs/api/v1/workspace)\
             \n\tid: missing\n\tname: too long; has spaces"
        );
    }

    #[test]
    fn status_error_without_an_error_response_shows_the_body() {
        assert_eq!(
            status_error(StatusCode::BAD_GATEWAY, "  upstream unavailable\n"),
            "502 Bad Gateway from https://api.nanook.pola.rs/api/v1/workspace: upstream unavailable"
        );
        assert_eq!(
            status_error(StatusCode::INTERNAL_SERVER_ERROR, ""),
            "500 Internal Server Error from https://api.nanook.pola.rs/api/v1/workspace"
        );
    }
}
