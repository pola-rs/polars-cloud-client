use std::fmt::Formatter;
use std::path::{Path, PathBuf};
use std::{fmt, fs};

use pyo3::pyclass;

use crate::AuthError;
use crate::constants::{
    ACCESS_TOKEN_ENV, ACCESS_TOKEN_FILENAME, CONFIG_DIR, REFRESH_TOKEN_FILENAME,
};
use crate::utils::{
    get_access_token_for_service_account, get_auth_header_from_access_token_env,
    is_token_expired_with_buffer, token_as_header, use_refresh_token,
};

#[derive(Clone, Debug)]
pub enum AuthToken {
    AccessTokenNoRefresh(String),
    EnvVar(String),
    ServiceAccount {
        client_id: String,
        client_secret: String,
        token: String,
    },
    AccessToken {
        token: String,
        refresh_token: String,
    },
}

#[pyclass(from_py_object)]
#[derive(Clone)]
pub enum AuthMethod {
    AccessTokenNoRefresh,
    EnvVar,
    ServiceAccount,
    AccessToken,
}

impl AuthToken {
    pub(crate) fn method(&self) -> AuthMethod {
        match &self {
            AuthToken::AccessTokenNoRefresh(_) => AuthMethod::AccessTokenNoRefresh,
            AuthToken::EnvVar(_) => AuthMethod::EnvVar,
            AuthToken::ServiceAccount { .. } => AuthMethod::ServiceAccount,
            AuthToken::AccessToken { .. } => AuthMethod::AccessToken,
        }
    }
}

impl fmt::Display for AuthMethod {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use AuthMethod::*;
        match self {
            AccessTokenNoRefresh => {
                write!(f, "Access token without refresh token")
            },
            EnvVar => {
                write!(f, "Environment variable ({ACCESS_TOKEN_ENV})")
            },
            ServiceAccount => {
                write!(f, "Service Account")
            },
            AccessToken => {
                write!(f, "Access token")
            },
        }
    }
}

impl AuthToken {
    pub(crate) async fn from_service_account(
        client_id: String,
        client_secret: String,
        connection_pool: reqwest_middleware::ClientWithMiddleware,
    ) -> Result<Self, AuthError> {
        let token =
            get_access_token_for_service_account(&client_id, &client_secret, connection_pool)
                .await?;
        Ok(AuthToken::ServiceAccount {
            client_id,
            client_secret,
            token,
        })
    }

    pub(crate) fn new_with_token(token: String) -> Result<Self, AuthError> {
        if is_token_expired_with_buffer(&token)? {
            Err(AuthError::new("Access token without refresh is expired."))
        } else {
            Ok(AuthToken::AccessTokenNoRefresh(token))
        }
    }

    pub(crate) async fn new_from_env_or_disk(
        token_path: Option<PathBuf>,
        connection_pool: reqwest_middleware::ClientWithMiddleware,
    ) -> Result<Self, AuthError> {
        // Check if we can find a valid token from the env vars
        if let Some(token) = get_auth_header_from_access_token_env()? {
            return Ok(AuthToken::EnvVar(token));
        }

        // Check if we can find env var client_id / secret
        if let (Some(client_id), Some(client_secret)) = (
            std::env::var("POLARS_CLOUD_CLIENT_ID").ok(),
            std::env::var("POLARS_CLOUD_CLIENT_SECRET").ok(),
        ) {
            let token =
                get_access_token_for_service_account(&client_id, &client_secret, connection_pool)
                    .await?;
            return Ok(AuthToken::ServiceAccount {
                client_id,
                client_secret,
                token,
            });
        }

        // Look for a valid access token on disk, refresh if necessary
        let mut token = Self::read_tokens_from_disk(token_path.as_deref())?;
        if let Some(new_token) = token.check_and_refresh(connection_pool).await? {
            token = new_token;
        }
        Ok(token)
    }
    pub async fn check_and_refresh(
        &self,
        connection_pool: reqwest_middleware::ClientWithMiddleware,
    ) -> Result<Option<AuthToken>, AuthError> {
        match self {
            AuthToken::AccessTokenNoRefresh(token) => {
                if is_token_expired_with_buffer(token)? {
                    Err(AuthError::new("Access token without refresh is expired."))
                } else {
                    Ok(None)
                }
            },
            AuthToken::EnvVar(token) => {
                if is_token_expired_with_buffer(token)? {
                    Err(AuthError::new(
                        "Token provided in environment variable is expired.",
                    ))
                } else {
                    Ok(None)
                }
            },
            AuthToken::ServiceAccount {
                client_id,
                client_secret,
                token,
            } => {
                if !is_token_expired_with_buffer(token)? {
                    return Ok(None);
                }
                let refreshed_token =
                    get_access_token_for_service_account(client_id, client_secret, connection_pool)
                        .await?;
                Ok(Some(AuthToken::ServiceAccount {
                    client_id: client_id.clone(),
                    client_secret: client_secret.clone(),
                    token: refreshed_token,
                }))
            },
            AuthToken::AccessToken {
                token,
                refresh_token,
            } => {
                if !is_token_expired_with_buffer(token)? {
                    return Ok(None);
                }
                let tokens = use_refresh_token(refresh_token, connection_pool).await?;

                Ok(Some(AuthToken::AccessToken {
                    token: tokens.access_token,
                    refresh_token: tokens.refresh_token,
                }))
            },
        }
    }
    pub fn to_auth_header(&self) -> String {
        let token = match &self {
            AuthToken::AccessTokenNoRefresh(token) => token,
            AuthToken::EnvVar(token) => token,
            AuthToken::ServiceAccount { token, .. } => token,
            AuthToken::AccessToken { token, .. } => token,
        };
        token_as_header(token)
    }

    fn read_tokens_from_disk(path: Option<&Path>) -> Result<AuthToken, AuthError> {
        let token = fs::read_to_string(
            path.unwrap_or(CONFIG_DIR.as_path())
                .join(ACCESS_TOKEN_FILENAME),
        )?;
        let refresh_token = fs::read_to_string(
            path.unwrap_or(CONFIG_DIR.as_path())
                .join(REFRESH_TOKEN_FILENAME),
        )?;
        Ok(AuthToken::AccessToken {
            token,
            refresh_token,
        })
    }
}
