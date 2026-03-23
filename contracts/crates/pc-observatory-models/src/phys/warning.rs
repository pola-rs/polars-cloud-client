#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[serde(tag = "id")]
pub enum PhysNodeWarning {
    InMemoryFallback,
    MemoryIntensive,
}
