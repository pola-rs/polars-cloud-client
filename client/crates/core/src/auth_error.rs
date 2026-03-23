use std::io;

use anyhow::anyhow;
use thiserror::Error;

#[derive(Debug, Error)]
#[error(transparent)]
pub struct AuthError(#[from] Box<dyn std::error::Error + Send + Sync>);

impl From<io::Error> for AuthError {
    fn from(_error: io::Error) -> Self {
        AuthError::new("Authentication token was not found.")
    }
}

impl AuthError {
    pub fn new(message: &str) -> Self {
        AuthError(anyhow!(message.to_string()).into())
    }
}
