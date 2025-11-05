use reqwest::{StatusCode, Url};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("{0:?}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("{0}")]
    MiddlewareError(#[from] reqwest_middleware::Error),
    #[error("{status} for {url} with body: {body}")]
    StatusError {
        status: StatusCode,
        url: Url,
        body: String,
    },
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

pub(crate) type Result<T> = std::result::Result<T, ApiError>;
