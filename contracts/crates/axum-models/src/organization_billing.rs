use chrono::{DateTime, Utc};
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
use serde::{Deserialize, Serialize};
#[cfg(feature = "server")]
use utoipa::ToSchema;
use uuid::Uuid;

#[cfg_attr(feature = "pyo3", pyclass(get_all, eq, eq_int))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub enum SubscriptionStatusSchema {
    SubscribePending,
    Subscribed,
    UnsubscribePending,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(Validate, ToSchema))]
pub struct BillingSubscribeSchema {
    #[cfg_attr(feature = "server", garde(skip))]
    pub registration_token: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct BillingHistogramSchema {
    pub timestamp: DateTime<Utc>,
    pub workspace_id: Uuid,
    pub workspace_name: String,
    pub tokens: i32,
}

#[cfg_attr(feature = "pyo3", pyclass(get_all))]
#[cfg_attr(feature = "server", derive(ToSchema))]
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct OrganizationBillingDetailsSchema {
    pub aws_customer_id: String,
    pub organization_id: Option<Uuid>,
    pub product_code: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub subscription_status: SubscriptionStatusSchema,
    pub subscribed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(ToSchema))]
pub struct OrganizationAwsBillingSchema {
    pub aws_customer_id: String,
    pub organization_id: Option<Uuid>,
    pub aws_account_id: Option<String>,
    pub product_code: String,
}
