use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::ToSchema;

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
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct Paginated<T> {
    pub pagination: PaginationInfo,
    pub result: Vec<T>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct PaginationInfo {
    pub page: i64,
    pub limit: i64,
    pub amount: usize,
    pub total_pages: i64,
    pub total_count: i64,
}
