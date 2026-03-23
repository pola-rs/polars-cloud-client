#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema, Validate))]
pub struct NotificationDetailArgs {
    #[cfg_attr(feature = "server", garde(skip))]
    pub read: bool,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub enum NotificationDataModel {
    TestType,
    UserJoinedWorkspace {
        user_sub: String,
        workspace_id: Uuid,
    },
}

/// Wrapper around `Notification`
#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct NotificationModel {
    /// Notification id
    pub id: Uuid,
    /// User id
    pub user_id: Uuid,
    /// Timestamp of the event
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// The type of notification
    pub notification_data: NotificationDataModel,
    /// Whether this notification has been read
    pub read: bool,
    /// Creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last update timestamp
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp of the last update
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}
