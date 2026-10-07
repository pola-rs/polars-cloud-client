use std::sync::LazyLock;
use std::time::Duration;

use crate::Runtime;

pub static SERVICE_NAME: &str = "polars-cloud-client";

pub static TOKEN_EXPIRATION_BUFFER: Duration = Duration::from_secs(10);

pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(Runtime::default);

pub static LOGIN_CLIENT_ID: &str = "PolarsCloud";
pub static LOGIN_AUDIENCE: &str = "account";

pub static ACCESS_TOKEN_ENV: &str = "POLARS_CLOUD_ACCESS_TOKEN";
pub static USER_NAME_ENV: &str = "POLARS_CLOUD_USER_NAME";
pub static DEFAULT_WORKSPACE_ID_ENV: &str = "POLARS_CLOUD_DEFAULT_WORKSPACE_ID";
pub static DEFAULT_ORGANIZATION_ID_ENV: &str = "POLARS_CLOUD_DEFAULT_ORGANIZATION_ID";
pub static ACCESS_TOKEN_PATH_ENV: &str = "POLARS_CLOUD_CONFIG_DIR";
pub static ACCESS_TOKEN_FILENAME: &str = "cloud_access_token";
pub static REFRESH_TOKEN_FILENAME: &str = "cloud_refresh_token";

pub static DOMAIN_ENV: &str = "POLARS_CLOUD_DOMAIN";
pub static API_ADDR_ENV: &str = "POLARS_CLOUD_API_ADDR";
pub static API_DOMAIN_PREFIX_ENV: &str = "POLARS_CLOUD_API_DOMAIN_PREFIX";
pub static DEFAULT_DOMAIN: &str = "prd.cloud.pola.rs";
pub static DEFAULT_API_DOMAIN_PREFIX: &str = "api";
