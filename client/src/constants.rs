use std::path::PathBuf;
use std::string::ToString;
use std::sync::LazyLock;

use anyhow::anyhow;
use directories::BaseDirs;

use crate::runtime::Runtime;

pub(crate) static RUNTIME: LazyLock<Runtime> = LazyLock::new(Runtime::new);

pub(crate) static LOGIN_CLIENT_ID: &str = "PolarsCloud";
pub(crate) static LOGIN_AUDIENCE: &str = "account";

pub(crate) static ACCESS_TOKEN_ENV: &str = "POLARS_CLOUD_ACCESS_TOKEN";
pub(crate) static ACCESS_TOKEN_PATH: &str = "cloud_access_token";
pub(crate) static REFRESH_TOKEN_PATH: &str = "cloud_refresh_token";

pub(crate) static AUTH_DOMAIN: LazyLock<String> = LazyLock::new(|| {
    let domain =
        std::env::var("POLARS_CLOUD_DOMAIN").unwrap_or_else(|_e| "prd.cloud.pola.rs".to_string());
    format!("auth.{domain}")
});

pub(crate) static API_ADDR: LazyLock<String> = LazyLock::new(|| {
    let domain =
        std::env::var("POLARS_CLOUD_DOMAIN").unwrap_or_else(|_e| "prd.cloud.pola.rs".to_string());
    let prefix =
        std::env::var("POLARS_CLOUD_API_DOMAIN_PREFIX").unwrap_or_else(|_e| "api".to_string());
    format!("https://{prefix}.{domain}")
});

pub(crate) static CONFIG_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    std::env::var("POLARS_CLOUD_CONFIG_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or(
            BaseDirs::new()
                .ok_or_else(|| anyhow!("Unable to determine user's config directory"))
                .unwrap()
                .config_dir()
                .join("polars_cloud"),
        )
});
