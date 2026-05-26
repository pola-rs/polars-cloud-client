use std::sync::OnceLock;

use polars_backend_client::client::Versions as VersionHeaders;
pub use polars_backend_client::versions::{Versions, get_versions};

pub static VERSIONS: OnceLock<Option<(Versions, VersionHeaders)>> = OnceLock::new();
