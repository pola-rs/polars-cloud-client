use chrono::prelude::*;
#[cfg(feature = "server")]
use garde::Validate;
#[cfg(feature = "server")]
use regex::Regex;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(schemars::JsonSchema, Validate))]
pub struct TimeWindow {
    #[cfg_attr(feature = "server", garde(custom(is_before(&self.end))))]
    pub start: DateTime<FixedOffset>,
    #[cfg_attr(feature = "server", garde(skip))]
    pub end: DateTime<FixedOffset>,
}

#[cfg(feature = "server")]
fn is_before(
    end: &DateTime<FixedOffset>,
) -> impl FnOnce(&DateTime<FixedOffset>, &()) -> garde::Result + '_ {
    move |start, _ctx| {
        if start < end {
            Ok(())
        } else {
            Err(garde::Error::new("start must be before end"))
        }
    }
}

#[derive(Deserialize, Debug)]
#[cfg_attr(feature = "server", derive(schemars::JsonSchema, Validate))]
pub struct MetricWindow {
    #[serde(flatten)]
    #[cfg_attr(feature = "server", garde(dive))]
    pub window: TimeWindow,
    #[serde(with = "duration_seconds")]
    #[cfg_attr(feature = "server", schemars(with = "u64"))]
    #[cfg_attr(feature = "server", garde(custom(is_at_least_five_minutes)))]
    pub interval: chrono::Duration,
}

#[cfg(feature = "server")]
fn is_at_least_five_minutes(interval: &chrono::Duration, _ctx: &()) -> garde::Result {
    if *interval >= chrono::Duration::minutes(5) {
        Ok(())
    } else {
        Err(garde::Error::new("interval must be at least 5 minutes"))
    }
}

pub mod duration_seconds {
    use serde::{Deserialize, Deserializer};

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<chrono::Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(chrono::Duration::seconds(
            u64::deserialize(deserializer)? as i64
        ))
    }
}

#[derive(Clone, Copy, Debug)]
pub enum DefaultSortDirection {
    Asc,
    Desc,
}

pub trait EntityOrdering {
    fn order_fields() -> &'static [&'static str];
    fn default_ordering() -> Option<(&'static str, DefaultSortDirection)> {
        None
    }
    fn tiebreaker_field() -> Option<&'static str> {
        None
    }
}

#[cfg(feature = "server")]
pub fn validate_alphanumeric_name_opt(name: &Option<String>, ctx: &()) -> garde::Result {
    if let Some(name) = name {
        validate_alphanumeric_name(name, ctx)
    } else {
        Ok(())
    }
}

#[cfg(feature = "server")]
pub fn validate_alphanumeric_name(name: &str, _ctx: &()) -> garde::Result {
    let valid_alphabet = Regex::new(r"^[[:alnum:] -]*$").unwrap();
    let three_alpha_numeric = Regex::new(r"^(?:[[:alnum:]][ -]*){3,}$").unwrap();

    if !valid_alphabet.is_match(name) {
        return Err(garde::Error::new(
            "Only letters, numbers, spaces, and dash (-) are allowed",
        ));
    }

    if !three_alpha_numeric.is_match(name) {
        return Err(garde::Error::new(
            "Must contain at least three alphanumeric characters",
        ));
    }

    Ok(())
}
