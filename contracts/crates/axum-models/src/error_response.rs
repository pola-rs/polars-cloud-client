use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ErrorResponse {
    pub message: String,
    pub errors: HashMap<String, Vec<String>>,
}
