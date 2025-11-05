use chrono::prelude::*;
#[cfg(feature = "server")]
use regex::Regex;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct TimeWindow {
    pub start: DateTime<FixedOffset>,
    pub end: DateTime<FixedOffset>,
}

#[derive(Deserialize, Debug)]
pub struct MetricWindow {
    #[serde(flatten)]
    pub window: TimeWindow,
    #[serde(with = "duration_seconds")]
    pub interval: chrono::Duration,
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

pub trait EntityOrdering {
    fn order_fields() -> &'static [&'static str];
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
