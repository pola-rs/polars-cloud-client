use std::fmt;

use prost::Message;
pub use prost::bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::common;

#[derive(Default, Clone)]
pub struct QueryInfo {
    pub labels: Vec<String>,
}

impl QueryInfo {
    pub fn encode(self) -> Bytes {
        common::QueryInfo::from(self).encode_to_vec().into()
    }

    pub fn decode(bytes: Bytes) -> Self {
        common::QueryInfo::decode(bytes).unwrap().into()
    }
}

impl From<QueryInfo> for common::QueryInfo {
    fn from(value: QueryInfo) -> Self {
        Self {
            labels: value
                .labels
                .into_iter()
                .map(|name| common::Label { name })
                .collect(),
        }
    }
}

impl From<common::QueryInfo> for QueryInfo {
    fn from(value: common::QueryInfo) -> Self {
        Self {
            labels: value.labels.into_iter().map(|label| label.name).collect(),
        }
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum FileType {
    Parquet,
    Ipc,
    Csv,
    Ndjson,
    Json,
}

impl From<common::FileType> for Option<FileType> {
    fn from(value: common::FileType) -> Self {
        Some(match value {
            common::FileType::Unspecified => return None,
            common::FileType::Parquet => FileType::Parquet,
            common::FileType::Ipc => FileType::Ipc,
            common::FileType::Csv => FileType::Csv,
            common::FileType::Ndjson => FileType::Ndjson,
            common::FileType::Json => FileType::Json,
        })
    }
}

impl From<Option<FileType>> for common::FileType {
    fn from(value: Option<FileType>) -> Self {
        let Some(filetype) = value else {
            return common::FileType::Unspecified;
        };
        match filetype {
            FileType::Parquet => common::FileType::Parquet,
            FileType::Ipc => common::FileType::Ipc,
            FileType::Csv => common::FileType::Csv,
            FileType::Ndjson => common::FileType::Ndjson,
            FileType::Json => common::FileType::Json,
        }
    }
}

pub struct QueryResult {
    pub total_stages: u32,
    pub finished_stages: u32,
    pub failed_stages: u32,
    pub errors: Vec<String>,
    pub output: Option<QueryOutput>,
}

#[derive(Debug)]
pub struct QueryOutput {
    pub sink_dst: Vec<String>,
    pub n_rows_result: u64,
    // The file type might be unknown if the server uses a different proto
    // definition than the client
    pub file_type: Option<FileType>,
}

impl fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryInfo")
            .field("total stages", &self.total_stages)
            .field("finished stages", &self.finished_stages)
            .field("failed_stages stages", &self.failed_stages)
            .finish()
    }
}

impl From<common::QueryOutput> for QueryOutput {
    fn from(value: common::QueryOutput) -> Self {
        Self {
            file_type: value.file_type().into(),
            sink_dst: value
                .destination
                .into_iter()
                .map(|dst| dst.destination)
                .collect(),
            n_rows_result: value.n_rows_result,
        }
    }
}

impl From<QueryOutput> for common::QueryOutput {
    fn from(value: QueryOutput) -> Self {
        Self {
            file_type: common::FileType::from(value.file_type).into(),
            destination: value
                .sink_dst
                .into_iter()
                .map(|destination| common::Destination { destination })
                .collect(),
            n_rows_result: value.n_rows_result,
        }
    }
}

impl From<common::QueryResult> for QueryResult {
    fn from(value: common::QueryResult) -> Self {
        Self {
            total_stages: value.total_stages,
            finished_stages: value.finished_stages,
            failed_stages: value.failed_stages,
            errors: value
                .errors
                .into_iter()
                .map(|error| error.message)
                .collect(),
            output: value.output.map(Into::into),
        }
    }
}

impl From<QueryResult> for common::QueryResult {
    fn from(value: QueryResult) -> Self {
        common::QueryResult {
            total_stages: value.total_stages,
            finished_stages: value.finished_stages,
            failed_stages: value.failed_stages,
            errors: value
                .errors
                .into_iter()
                .map(|message| common::Error { message })
                .collect(),
            output: value.output.map(Into::into),
        }
    }
}
