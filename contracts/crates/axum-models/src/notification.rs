#[cfg(feature = "server")]
use garde::Validate;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(ToSchema, Validate))]
pub struct NotificationDetail {
    #[cfg_attr(feature = "server", garde(skip))]
    pub read: bool,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub enum NotificationDataSchema {
    TestType,
    UserJoinedWorkspace {
        user_sub: String,
        workspace_id: Uuid,
    },
}

/// Wrapper around `Notification`
#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct NotificationSchema {
    /// Notification id
    pub id: Uuid,
    /// User id
    pub user_id: Uuid,
    /// Timestamp of the event
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// The type of notification
    pub notification_data: NotificationDataSchema,
    /// Whether this notification has been read
    pub read: bool,
    /// Creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last update timestamp
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp of the last update
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}
