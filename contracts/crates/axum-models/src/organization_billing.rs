use chrono::{DateTime, Utc};
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::OrganizationTierModel;

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all, eq, eq_int))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub enum SubscriptionStatusModel {
    SubscribePending,
    Subscribed,
    UnsubscribePending,
    Uninitialized,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all, eq, eq_int))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub enum BillingProviderModel {
    AwsMarketplace,
    Stripe,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
pub struct BillingSubscribeModel {
    #[cfg_attr(feature = "server", garde(skip))]
    pub registration_token: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct BillingHistogramModel {
    pub timestamp: DateTime<Utc>,
    pub workspace_id: Uuid,
    pub workspace_name: String,
    pub tokens: i32,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct OrganizationBillingDetailsModel {
    pub provider: BillingProviderModel,
    pub external_customer_id: String,
    pub organization_id: Option<Uuid>,
    pub external_product_id: Option<String>,
    pub external_subscription_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub subscription_status: SubscriptionStatusModel,
    pub subscribed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct OrganizationBillingModel {
    pub provider: BillingProviderModel,
    pub external_customer_id: String,
    pub organization_id: Option<Uuid>,
    pub external_product_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
pub struct StripeCheckoutSessionRequestModel {
    #[cfg_attr(feature = "server", garde(length(min = 1, max = 512)))]
    pub frontend_origin: String,
    #[cfg_attr(feature = "server", garde(skip))]
    pub tier: OrganizationTierModel,
}

/// A hosted Stripe Checkout Session the client should redirect the customer to.
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StripeCheckoutSessionModel {
    pub id: String,
    pub url: String,
}
