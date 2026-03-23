use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use polars_axum_models::PythonVersion;
use pyo3::exceptions::PyValueError;
use pyo3::{PyResult, Python, pyfunction};
use serde::Deserialize;
use serde_json::json;

use crate::constants::{
    ACCESS_TOKEN_ENV, ACCESS_TOKEN_FILENAME, AUTH_DOMAIN, CONFIG_DIR, LOGIN_CLIENT_ID,
    REFRESH_TOKEN_FILENAME,
};
use crate::{AuthError, TOKEN_EXPIRATION_BUFFER, VERSIONS};

#[derive(Deserialize)]
pub struct Tokens {
    pub access_token: String,
    pub refresh_token: String,
}

#[derive(Deserialize)]
pub struct AccessToken {
    pub access_token: String,
}

/// Get an auth header from the cloud access token environment variable if it exists
pub fn get_auth_header_from_access_token_env() -> Result<Option<String>, AuthError> {
    if let Ok(token) = std::env::var(ACCESS_TOKEN_ENV) {
        return token_from_environment(token).map(Some);
    };
    Ok(None)
}

pub fn write_tokens(access_token: &str, refresh_token: &str) -> Result<(), AuthError> {
    fs::create_dir_all(CONFIG_DIR.as_path())?;
    fs::write(CONFIG_DIR.join(ACCESS_TOKEN_FILENAME), access_token)?;
    fs::write(CONFIG_DIR.join(REFRESH_TOKEN_FILENAME), refresh_token)?;
    Ok(())
}

pub async fn get_access_token_for_service_account(
    username: &str,
    password: &str,
    connection_pool: reqwest_middleware::ClientWithMiddleware,
) -> Result<String, AuthError> {
    let url = format!(
        "https://{}/realms/Polars/protocol/openid-connect/token",
        *AUTH_DOMAIN
    );

    let data = json!({
        "username": format!("{}@sa.cloud.pola.rs",username),
        "password": password,
        "grant_type": "password",
        "client_id": "PolarsCloud",
    });

    let token = connection_pool
        .post(url)
        .form(&data)
        .send()
        .await
        .map_err(|e| AuthError::new(&format!("Error getting access token: {e:?}")))?
        .json::<AccessToken>()
        .await
        .map_err(|e| AuthError::new(&format!("Error parsing access token JSON: {e:?}")))?;

    Ok(token.access_token)
}

pub async fn use_refresh_token(
    refresh_token: &str,
    connection_pool: reqwest_middleware::ClientWithMiddleware,
) -> Result<Tokens, AuthError> {
    if is_token_expired_with_buffer(refresh_token)? {
        return Err(AuthError::new("The refresh token has expired."));
    }

    let url = format!(
        "https://{}/realms/Polars/protocol/openid-connect/token",
        *AUTH_DOMAIN
    );

    let data = json!({
        "client_id": LOGIN_CLIENT_ID,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    });

    let tokens = connection_pool
        .post(url)
        .form(&data)
        .send()
        .await
        .map_err(|e| AuthError::new(&format!("Error refreshing token with: {e:?}")))?
        .json::<Tokens>()
        .await
        .map_err(|e| AuthError::new(&format!("Error parsing refresh token JSON with: {e:?}")))?;

    write_tokens(&tokens.access_token, &tokens.refresh_token)?;

    Ok(tokens)
}

#[pyfunction]
pub fn py_is_token_expired(
    token: &str,
    reject_tokens_expiring_in_less_than: Option<Duration>,
) -> PyResult<bool> {
    is_token_expired(token, reject_tokens_expiring_in_less_than)
        .map_err(|_e| PyValueError::new_err("Failed to parse JWT Token"))
}

/// Checks whether token is valid for at least `TOKEN_EXPIRATION_BUFFER` more seconds.
/// This prevents most cases where we call the API with an expired token and improves the error message.
pub fn is_token_expired_with_buffer(token: &str) -> Result<bool, AuthError> {
    is_token_expired(token, Some(TOKEN_EXPIRATION_BUFFER)).map_err(|e| {
        tracing::debug!("Error parsing token: {e}");
        AuthError::new(&format!("The access token is invalid: {e}"))
    })
}

/// Checks whether the JWT token has expired
pub fn is_token_expired(
    token: &str,
    reject_tokens_expiring_in_less_than: Option<Duration>,
) -> Result<bool, AuthError> {
    let payload = token
        .split('.')
        .collect::<Vec<&str>>()
        .get(1)
        .ok_or_else(|| AuthError::new("No payload in token."))?
        .to_string();

    let payload = STANDARD_NO_PAD
        .decode(payload)
        .map_err(|_| AuthError::new("Invalid token payload."))?;

    #[derive(Debug, Deserialize)]
    struct Claims {
        exp: usize,
    }

    let claims: Claims = serde_json::from_slice(&payload)
        .map_err(|_| AuthError::new("Expiration missing from token."))?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_e| AuthError::new("Could not get current system time."))?
        .as_secs() as usize;

    let secs =
        if let Some(reject_tokens_expiring_in_less_than) = reject_tokens_expiring_in_less_than {
            reject_tokens_expiring_in_less_than.as_secs() as usize
        } else {
            0
        };

    Ok(claims.exp < now + secs)
}

pub fn token_from_environment(token: String) -> Result<String, AuthError> {
    match is_token_expired(&token, None) {
        Ok(false) => Ok(token),
        Ok(true) => Err(AuthError::new(&format!(
            "The {ACCESS_TOKEN_ENV} environment variable authentication token has expired.",
        ))),
        Err(e) => Err(AuthError::new(&format!(
            "The {ACCESS_TOKEN_ENV} environment variable authentication token is invalid with: {e}."
        ))),
    }
}

pub(crate) fn token_as_header(token: &str) -> String {
    format!("Bearer {token}")
}

#[pyfunction]
pub fn polars_version() -> String {
    VERSIONS
        .get()
        .unwrap()
        .as_ref()
        .unwrap()
        .0
        .polars
        .to_string()
}

#[pyfunction]
pub fn python_version(py: Python<'_>) -> String {
    let version = py.version_info();
    PythonVersion {
        major: version.major,
        minor: version.minor,
        patch: version.patch,
    }
    .to_string()
}
