#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(rename_all = "snake_case")]
pub enum OrderDirection {
    Asc,
    #[default]
    Desc,
}

pub mod observatory_target;

#[derive(PartialEq, Clone, Copy, Debug, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "server", derive(JsonSchema, sqlx::Type), sqlx(transparent))]
pub struct QueryId(pub Uuid);

impl std::fmt::Display for QueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<&Vec<u8>> for QueryId {
    type Error = uuid::Error;

    fn try_from(bytes: &Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self(Uuid::from_slice(bytes)?))
    }
}

#[derive(PartialEq, Clone, Copy, Debug, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "server", derive(JsonSchema, sqlx::Type), sqlx(transparent))]
pub struct TraceId(pub Uuid);

impl TryFrom<&Vec<u8>> for TraceId {
    type Error = uuid::Error;

    fn try_from(bytes: &Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self(Uuid::from_slice(bytes)?))
    }
}

#[derive(PartialEq, Clone, Debug, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(JsonSchema, sqlx::Type), sqlx(transparent))]
pub struct SpanId(pub Vec<u8>);

#[derive(PartialEq, Clone, Copy, Debug, Hash, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[serde(transparent)]
pub struct StageNumber(pub u32);

// Manual sqlx::Type impl to avoid PgHasArrayType issues when postgres features are
// unified in workspaces that also depend on sqlx with postgres.
#[cfg(feature = "server")]
impl sqlx::Type<sqlx::Sqlite> for StageNumber {
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <u32 as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

#[cfg(feature = "server")]
impl<'r> sqlx::Decode<'r, sqlx::Sqlite> for StageNumber {
    fn decode(
        value: <sqlx::Sqlite as sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        <u32 as sqlx::Decode<'r, sqlx::Sqlite>>::decode(value).map(Self)
    }
}

#[cfg(feature = "server")]
impl sqlx::Encode<'_, sqlx::Sqlite> for StageNumber {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Sqlite as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <u32 as sqlx::Encode<'_, sqlx::Sqlite>>::encode_by_ref(&self.0, buf)
    }
}

impl std::fmt::Display for StageNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
#[derive(PartialEq, Clone, Debug, Hash, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "server", derive(JsonSchema, sqlx::Type), sqlx(transparent))]
pub struct NodeId(pub String);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

#[derive(PartialEq, Default, Clone, Copy, Debug, Hash, Eq, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "server", derive(JsonSchema, sqlx::Type), sqlx(transparent))]
pub struct ShuffleBytes(pub i64);

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize, Eq, Hash)]
#[serde(transparent)]
#[cfg_attr(feature = "server", derive(JsonSchema, sqlx::Type), sqlx(transparent))]
pub struct SpanName(pub String);

#[derive(PartialEq, Clone, Debug, Copy, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "server", derive(JsonSchema, sqlx::Type), sqlx(transparent))]
pub struct DurationNs(pub i64);

#[derive(PartialEq, Clone, Copy, Debug, Hash, Eq, Serialize, Deserialize, PartialOrd, Ord)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct StageAttemptNumber(pub u32);

// Manual sqlx::Type impl to avoid PgHasArrayType issues when postgres features are
// unified in workspaces that also depend on sqlx with postgres.
#[cfg(feature = "server")]
impl sqlx::Type<sqlx::Sqlite> for StageAttemptNumber {
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <u32 as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

#[cfg(feature = "server")]
impl<'r> sqlx::Decode<'r, sqlx::Sqlite> for StageAttemptNumber {
    fn decode(
        value: <sqlx::Sqlite as sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        <u32 as sqlx::Decode<'r, sqlx::Sqlite>>::decode(value).map(Self)
    }
}

#[cfg(feature = "server")]
impl sqlx::Encode<'_, sqlx::Sqlite> for StageAttemptNumber {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Sqlite as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <u32 as sqlx::Encode<'_, sqlx::Sqlite>>::encode_by_ref(&self.0, buf)
    }
}

impl std::fmt::Display for StageAttemptNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(PartialEq, Clone, Copy, Debug, Hash, Eq, Serialize, Deserialize, PartialOrd, Ord)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct ProducerId(pub u32);

// Manual sqlx::Type impl to avoid PgHasArrayType issues when postgres features are
// unified in workspaces that also depend on sqlx with postgres.
#[cfg(feature = "server")]
impl sqlx::Type<sqlx::Sqlite> for ProducerId {
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <u32 as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

#[cfg(feature = "server")]
impl<'r> sqlx::Decode<'r, sqlx::Sqlite> for ProducerId {
    fn decode(
        value: <sqlx::Sqlite as sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        <u32 as sqlx::Decode<'r, sqlx::Sqlite>>::decode(value).map(Self)
    }
}

#[cfg(feature = "server")]
impl sqlx::Encode<'_, sqlx::Sqlite> for ProducerId {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Sqlite as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <u32 as sqlx::Encode<'_, sqlx::Sqlite>>::encode_by_ref(&self.0, buf)
    }
}

impl std::fmt::Display for ProducerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Default, PartialEq, Clone, Copy, Debug, Hash, Eq, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "server", derive(JsonSchema, sqlx::Type), sqlx(transparent))]
pub struct OutputRows(pub i64);

#[derive(
    PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, Default, Debug, Serialize, Deserialize,
)]
#[cfg_attr(feature = "server", derive(JsonSchema, sqlx::Type), sqlx(transparent))]
pub struct PhysNodeId(pub i64);
