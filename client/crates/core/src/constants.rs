use std::path::PathBuf;
use std::string::ToString;
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::anyhow;
use directories::BaseDirs;

use crate::Runtime;

pub static TOKEN_EXPIRATION_BUFFER: Duration = Duration::from_secs(10);

pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(Runtime::default);

pub static LOGIN_CLIENT_ID: &str = "PolarsCloud";
pub static LOGIN_AUDIENCE: &str = "account";

pub static ACCESS_TOKEN_ENV: &str = "POLARS_CLOUD_ACCESS_TOKEN";
pub static ACCESS_TOKEN_PATH_ENV: &str = "POLARS_CLOUD_CONFIG_DIR";
pub static ACCESS_TOKEN_FILENAME: &str = "cloud_access_token";
pub static REFRESH_TOKEN_FILENAME: &str = "cloud_refresh_token";

pub static AUTH_DOMAIN: LazyLock<String> = LazyLock::new(|| {
    let domain =
        std::env::var("POLARS_CLOUD_DOMAIN").unwrap_or_else(|_e| "prd.cloud.pola.rs".to_string());
    format!("auth.{domain}")
});

pub static API_ADDR: LazyLock<String> = LazyLock::new(|| {
    if let Ok(addr) = std::env::var("POLARS_CLOUD_API_ADDR") {
        return addr;
    }
    let domain =
        std::env::var("POLARS_CLOUD_DOMAIN").unwrap_or_else(|_e| "prd.cloud.pola.rs".to_string());
    let prefix =
        std::env::var("POLARS_CLOUD_API_DOMAIN_PREFIX").unwrap_or_else(|_e| "api".to_string());
    format!("https://{prefix}.{domain}:443")
});

pub static CONFIG_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
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
