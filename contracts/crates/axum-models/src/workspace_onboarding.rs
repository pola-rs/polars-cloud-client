#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "server")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum OnboardingItemKeyModel {
    ExportQuery,
    ConnectInfrastructure,
    InviteMember,
}

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum OnboardingItemStatusModel {
    Initial,
    Skipped,
    Done,
}

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OnboardingItemModel {
    pub key: OnboardingItemKeyModel,
    pub status: OnboardingItemStatusModel,
    pub can_perform: bool,
}

#[cfg_attr(feature = "server", derive(JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OnboardingStatusModel {
    pub items: Vec<OnboardingItemModel>,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "server", derive(Validate, JsonSchema))]
pub struct OnboardingItemUpdateArgs {
    #[cfg_attr(feature = "server", garde(skip))]
    pub status: OnboardingItemStatusModel,
}
