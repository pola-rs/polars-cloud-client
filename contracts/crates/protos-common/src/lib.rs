pub mod macros;

pub use {prost, tonic};

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/includes.rs"));
}

pub(crate) mod common {
    pub use crate::proto::polars_cloud::common::*;
}

pub const MAX_MESSAGE_LENGTH_UNLIMITED: usize = usize::MAX;

pub mod query_info;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::str::FromStr;

pub use identifier::{ComputeIdentifier, QueryIdentifier, TaskIdentifier};
pub use prost::bytes::Bytes;
pub use query_info::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl From<String> for common::Destination {
    fn from(value: String) -> Self {
        Self { destination: value }
    }
}

pub type PlanFormat = common::PlanFormat;
pub type QueryPlans = common::QueryPlans;

mod identifier {
    use super::*;

    pub type TaskIdentifier = Identifier<TaskIdKind>;
    pub type QueryIdentifier = Identifier<QueryIdKind>;
    pub type ComputeIdentifier = Identifier<ComputeIdKind>;

    /// Compile-time compute identifier kind. Can't be instantiated on purpose.
    #[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
    pub enum ComputeIdKind {}

    /// Compile-time query identifier kind. Can't be instantiated on purpose.
    #[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
    pub enum QueryIdKind {}

    /// Compile-time task identifier kind. Can't be instantiated on purpose.
    #[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
    pub enum TaskIdKind {}

    #[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
    pub struct Identifier<Kind> {
        pub inner: Uuid,
        pub _kind: PhantomData<Kind>,
    }

    impl<K> Display for Identifier<K> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.inner)
        }
    }

    impl<K> From<Uuid> for Identifier<K> {
        fn from(value: Uuid) -> Self {
            Identifier {
                inner: value,
                _kind: PhantomData,
            }
        }
    }

    impl<K> Identifier<K> {
        pub fn new() -> Self {
            Self {
                inner: Uuid::now_v7(),
                _kind: PhantomData,
            }
        }

        pub fn into_string(self) -> String {
            format!("{}", self.inner)
        }
    }

    impl<K> FromStr for Identifier<K> {
        type Err = <Uuid as FromStr>::Err;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            Ok(Self {
                inner: Uuid::from_str(s)?,
                _kind: PhantomData,
            })
        }
    }

    impl<K> Default for Identifier<K> {
        fn default() -> Self {
            Self::new()
        }
    }
}

impl From<QueryIdentifier> for common::QueryId {
    fn from(value: QueryIdentifier) -> Self {
        common::QueryId {
            query_id: value.into_string(),
        }
    }
}

impl From<common::QueryId> for QueryIdentifier {
    fn from(value: common::QueryId) -> Self {
        Self::from_str(&value.query_id).expect("invalid query identifier")
    }
}
