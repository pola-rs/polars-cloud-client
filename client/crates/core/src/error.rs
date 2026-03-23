use std::error::Error;
use std::fmt::Display;

use polars_axum_models::ErrorResponse;
use protos_common::tonic::{self, Code, Status};
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::{PyErr, create_exception};
use reqwest::{StatusCode, Url};
use thiserror::Error;

use crate::{AuthError, AuthMethod};

create_exception!(polars_cloud, NotFoundError, PyException);
create_exception!(polars_cloud, AuthLoadError, PyException);
create_exception!(polars_cloud, ComputeClusterMisspecified, PyException);
create_exception!(polars_cloud, EncodedPolarsError, PyException);

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum ApiError {
    ReqwestError(#[from] reqwest::Error),
    MiddlewareError(#[from] reqwest_middleware::Error),
    StatusError {
        status: StatusCode,
        url: Url,
        body: String,
    },
    PyErr(#[from] PyErr),
    AuthLoadError(DynError),
    ComputeClusterMisspecified(DynError),
    GRPCTransportError(#[from] tonic::transport::Error),
    GRPCError(Box<Status>),
    UuidParsingError(#[from] uuid::Error),
    Other(#[from] anyhow::Error),
}

impl Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            ApiError::ReqwestError(e) => write!(
                f,
                "Error calling REST endpoint: {}",
                format_reqwest_error(e)
            ),
            ApiError::MiddlewareError(e) => write!(
                f,
                "Error calling REST endpoint: {}",
                format_middleware_error(e)
            ),
            ApiError::PyErr(error) => f.write_str(&error.to_string()),
            ApiError::StatusError {
                status,
                url: _,
                body,
            } => {
                let response = if let Ok(response) = serde_json::from_str::<ErrorResponse>(body) {
                    response
                } else {
                    ErrorResponse {
                        message: format!("Status {status} with body: {body}"),
                        errors: Default::default(),
                    }
                };

                f.write_str(&serde_json::to_string_pretty(&response).unwrap())
            },
            ApiError::AuthLoadError(error) => {
                write!(
                    f,
                    "{error}\n\tLog in with `pc login` or set the environment variables.\n\tSee https://docs.pola.rs/polars-cloud/explain/authentication/ for more details."
                )
            },
            ApiError::ComputeClusterMisspecified(error) => f.write_str(&error.to_string()),
            ApiError::GRPCTransportError(error) => {
                let hint = "Hint: you may need to restart the query if this error persists";
                write!(
                    f,
                    "Error setting up gRPC connection, {error}{}\n{hint}",
                    error.source().map_or(String::new(), |e| format!("\n{e}"))
                )
            },
            ApiError::GRPCError(status) => {
                if status.code() == Code::InvalidArgument
                    && let Ok(s) = str::from_utf8(status.details())
                    && s.split_once(":").is_some()
                {
                    return f.write_str(s);
                }
                write!(
                    f,
                    "Error calling gRPC endpoint, {}, {}{}",
                    status.code(),
                    status.message(),
                    status.source().map_or(String::new(), |e| format!("\n{e}"))
                )
            },
            ApiError::UuidParsingError(error) => f.write_str(&error.to_string()),
            ApiError::Other(error) => f.write_str(&error.to_string()),
        }
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
        Self::GRPCError(Box::new(value))
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

fn format_reqwest_error(error: &reqwest::Error) -> String {
    format!(
        "{error}{}{}",
        error.url().map_or(String::new(), |e| format!("\n{e}")),
        error.source().map_or(String::new(), |e| format!("\n{e}"))
    )
}

fn format_middleware_error(error: &reqwest_middleware::Error) -> String {
    match error {
        reqwest_middleware::Error::Middleware(e) => match e.downcast_ref() {
            Some(reqwest_retry::RetryError::Error(e)) => format_middleware_error(e),
            Some(reqwest_retry::RetryError::WithRetries { retries, err }) => {
                format!(
                    "failed after {retries} retries with: {}",
                    format_middleware_error(err)
                )
            },
            e => format!("unknown middleware error: {e:?}"),
        },
        reqwest_middleware::Error::Reqwest(e) => format_reqwest_error(e),
    }
}

impl From<ApiError> for PyErr {
    fn from(error: ApiError) -> Self {
        match error {
            ApiError::ReqwestError(..) => PyRuntimeError::new_err(error.to_string()),
            ApiError::MiddlewareError(..) => PyRuntimeError::new_err(error.to_string()),
            ApiError::PyErr(error) => error,
            ApiError::StatusError { ref status, .. } => {
                if *status == StatusCode::NOT_FOUND {
                    NotFoundError::new_err(error.to_string())
                } else {
                    PyValueError::new_err(error.to_string())
                }
            },
            ApiError::AuthLoadError(..) => AuthLoadError::new_err(error.to_string()),
            ApiError::ComputeClusterMisspecified(..) => {
                ComputeClusterMisspecified::new_err(error.to_string())
            },
            ApiError::GRPCTransportError(..) => PyRuntimeError::new_err(error.to_string()),
            ApiError::GRPCError(ref status) => {
                if status.code() == Code::InvalidArgument
                    && let Ok(s) = str::from_utf8(status.details())
                    && s.split_once(":").is_some()
                {
                    return EncodedPolarsError::new_err(s.to_owned());
                }
                PyRuntimeError::new_err(error.to_string())
            },
            ApiError::UuidParsingError(..) => PyValueError::new_err(error.to_string()),
            ApiError::Other(..) => PyValueError::new_err(error.to_string()),
        }
    }
}

impl From<AuthError> for ApiError {
    fn from(value: AuthError) -> Self {
        ApiError::AuthLoadError(value.into())
    }
}

pub type ApiResult<T> = std::result::Result<T, ApiError>;
