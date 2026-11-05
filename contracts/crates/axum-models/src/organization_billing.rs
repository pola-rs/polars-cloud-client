use chrono::{DateTime, FixedOffset, Utc};
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "pyo3")]
use pyo3::pyclass;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::OrganizationPlanModel;

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
    /// The usage-only "Polars Cloud" listing on the AWS Marketplace
    AwsMarketplace,
    /// The "Polars Nanook" listing on the AWS Marketplace, with developer and team plans
    AwsMarketplaceNanook,
    Stripe,
    Credit,
    FreeQuota,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all, eq, eq_int))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub enum CreditLedgerKindModel {
    SignupCredits,
    BilledToProvider,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct CreditLedgerEntryModel {
    pub cents: i64,
    pub kind: CreditLedgerKindModel,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct OrganizationCreditsModel {
    /// Negative once the organization used more than it was granted
    pub cents: i64,
    pub grants: Vec<CreditLedgerEntryModel>,
}

/// Which ledger entries to list, by when they were created
#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
pub struct LedgerHistoryArgs {
    /// Defaults to 3 months before `end`
    #[cfg_attr(feature = "server", garde(skip))]
    pub start: Option<DateTime<FixedOffset>>,
    /// Defaults to now
    #[cfg_attr(feature = "server", garde(skip))]
    pub end: Option<DateTime<FixedOffset>>,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all, eq, eq_int))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub enum FreeQuotaLedgerKindModel {
    MonthlyGrant,
    Expired,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct FreeQuotaLedgerEntryModel {
    pub byoc_vcpu_seconds: i64,
    pub kind: FreeQuotaLedgerKindModel,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct OrganizationFreeQuotaModel {
    /// None when the organization gets no monthly free quota
    pub monthly_byoc_vcpu_seconds: Option<i64>,
    pub remaining_byoc_vcpu_seconds: i64,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
    pub entries: Vec<FreeQuotaLedgerEntryModel>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct LimitModel {
    /// None means unlimited
    pub value: Option<u32>,
    /// How much of it the organization holds now
    pub usage: u32,
    /// Set for this organization instead of the default of its plan
    pub overridden: bool,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct OrganizationLimitsModel {
    pub max_users: LimitModel,
    pub max_service_accounts: LimitModel,
    pub max_workspaces: LimitModel,
    pub max_concurrent_clusters: LimitModel,
    pub max_serverless_vcpus: LimitModel,
    pub max_serverless_memory_gib: LimitModel,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct FreeQuotaAllowanceModel {
    /// None when the organization gets no monthly free quota
    pub monthly_byoc_vcpu_seconds: Option<i64>,
    /// The monthly quota was set for this organization instead of the default of its plan
    pub monthly_overridden: bool,
    pub remaining_byoc_vcpu_seconds: i64,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(JsonSchema))]
pub struct OrganizationPlanDetailsModel {
    pub plan: OrganizationPlanModel,
    /// A plan change that takes effect at the end of the current billing period.
    pub scheduled_plan_change: Option<ScheduledPlanChangeModel>,
    pub limits: OrganizationLimitsModel,
    pub free_quota: FreeQuotaAllowanceModel,
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
    pub cents: i64,
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
    pub subscribed_until: Option<DateTime<Utc>>,
    /// A plan change that takes effect at the end of the current billing period.
    pub scheduled_plan_change: Option<ScheduledPlanChangeModel>,
}

#[cfg_attr(feature = "pyo3", pyclass(from_py_object, get_all))]
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct ScheduledPlanChangeModel {
    pub plan: OrganizationPlanModel,
    pub at: DateTime<Utc>,
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
    #[cfg_attr(feature = "server", garde(skip))]
    pub plan: OrganizationPlanModel,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
pub struct StripePlanChangeRequestModel {
    #[cfg_attr(feature = "server", garde(skip))]
    pub plan: OrganizationPlanModel,
}

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StripePlanChangeModel {
    pub plan: OrganizationPlanModel,
    /// When the plan takes effect. `None` when it took effect immediately (an upgrade), set to
    /// the end of the current billing period for a downgrade.
    pub scheduled_at: Option<DateTime<Utc>>,
}

/// A hosted Stripe Checkout Session the client should redirect the customer to.
#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StripeCheckoutSessionModel {
    pub id: String,
    pub url: String,
}
