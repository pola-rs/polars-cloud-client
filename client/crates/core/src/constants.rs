use std::sync::LazyLock;
use std::time::Duration;

use pyo3::pyfunction;

use crate::Runtime;

pub static SERVICE_NAME: &str = "polars-nanook-client";

pub static TOKEN_EXPIRATION_BUFFER: Duration = Duration::from_secs(10);

pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(Runtime::default);

pub use polars_backend_client::LOGIN_CLIENT_ID;
pub static LOGIN_AUDIENCE: &str = "account";

pub const ACCESS_TOKEN_ENV: &str = "NANOOK_ACCESS_TOKEN";
pub const CLIENT_ID_ENV: &str = "NANOOK_CLIENT_ID";
pub const CLIENT_SECRET_ENV: &str = "NANOOK_CLIENT_SECRET";
pub const USER_NAME_ENV: &str = "NANOOK_USER_NAME";
pub const SINGLE_NODE_ENV: &str = "NANOOK_SINGLE_NODE";
pub const DEFAULT_WORKSPACE_ID_ENV: &str = "NANOOK_DEFAULT_WORKSPACE_ID";
pub const DEFAULT_ORGANIZATION_ID_ENV: &str = "NANOOK_DEFAULT_ORGANIZATION_ID";
pub const ACCESS_TOKEN_PATH_ENV: &str = "NANOOK_CONFIG_DIR";
pub static ACCESS_TOKEN_FILENAME: &str = "cloud_access_token";
pub static REFRESH_TOKEN_FILENAME: &str = "cloud_refresh_token";

pub const DOMAIN_ENV: &str = "NANOOK_DOMAIN";
pub const API_ADDR_ENV: &str = "NANOOK_API_ADDR";
pub const API_DOMAIN_PREFIX_ENV: &str = "NANOOK_API_DOMAIN_PREFIX";
pub static DEFAULT_DOMAIN: &str = "prd.nanook.pola.rs";
pub static DEFAULT_API_DOMAIN_PREFIX: &str = "api";

pub static RENAMED_ENV_VARS: &[(&str, &str)] = &[
    (ACCESS_TOKEN_ENV, "POLARS_CLOUD_ACCESS_TOKEN"),
    (CLIENT_ID_ENV, "POLARS_CLOUD_CLIENT_ID"),
    (CLIENT_SECRET_ENV, "POLARS_CLOUD_CLIENT_SECRET"),
    (USER_NAME_ENV, "POLARS_CLOUD_USER_NAME"),
    (SINGLE_NODE_ENV, "POLARS_CLOUD_SINGLE_NODE"),
    (
        DEFAULT_WORKSPACE_ID_ENV,
        "POLARS_CLOUD_DEFAULT_WORKSPACE_ID",
    ),
    (
        DEFAULT_ORGANIZATION_ID_ENV,
        "POLARS_CLOUD_DEFAULT_ORGANIZATION_ID",
    ),
    (ACCESS_TOKEN_PATH_ENV, "POLARS_CLOUD_CONFIG_DIR"),
    (DOMAIN_ENV, "POLARS_CLOUD_DOMAIN"),
    (API_ADDR_ENV, "POLARS_CLOUD_API_ADDR"),
    (API_DOMAIN_PREFIX_ENV, "POLARS_CLOUD_API_DOMAIN_PREFIX"),
];

#[pyfunction]
pub fn legacy_env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().or_else(|| {
        let (_, old) = RENAMED_ENV_VARS.iter().find(|(new, _)| *new == name)?;
        std::env::var(old).ok()
    })
}

pub fn legacy_env_var_warnings() -> Vec<String> {
    RENAMED_ENV_VARS
        .iter()
        .filter(|(new, old)| std::env::var_os(new).is_none() && std::env::var_os(old).is_some())
        .map(|(new, old)| {
            format!(
                "the `{old}` environment variable is renamed to `{new}`; the old name will stop working in a future release"
            )
        })
        .collect()
}
