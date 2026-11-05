pub mod builder;
pub mod client;
pub mod error;
mod middleware;
#[cfg(feature = "pyo3")]
pub mod versions;
pub use middleware::RetryTransientMiddleware;

/// Keycloak client that end users authorize as: the CLI login, the frontend and the MCP
/// server's OAuth metadata all name it.
pub static LOGIN_CLIENT_ID: &str = "PolarsCloud";
