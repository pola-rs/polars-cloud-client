#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct Pagination {
    pub page: i64,
    pub limit: i64,
    pub offset: i64,
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            page: 1,
            limit: 25,
            offset: 0,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct Paginated<T> {
    pub pagination: PaginationInfo,
    pub result: Vec<T>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct PaginationInfo {
    pub page: i64,
    pub limit: i64,
    pub amount: usize,
    pub total_pages: i64,
    pub total_count: i64,
}
