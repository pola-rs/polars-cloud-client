use std::cmp::Ordering;
use std::fmt::Display;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Debug, Clone, Copy, Hash)]
pub struct VersionNumber {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub beta: Option<u32>,
}

impl Ord for VersionNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        let version =
            (self.major, self.minor, self.patch).cmp(&(other.major, other.minor, other.patch));
        version.then_with(|| match (self.beta, other.beta) {
            (Some(l), Some(r)) => l.cmp(&r),
            // A beta of the same version is earlier than the regular version
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        })
    }
}

impl PartialOrd for VersionNumber {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl VersionNumber {
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
            beta: None,
        }
    }
    pub const fn with_beta(mut self, beta: u32) -> Self {
        self.beta = Some(beta);
        self
    }
}

#[derive(Debug)]
pub struct ParseVersionError(&'static str);

impl Display for ParseVersionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to parse version: {}", self.0)
    }
}

impl std::error::Error for ParseVersionError {}

impl From<&'static str> for ParseVersionError {
    fn from(value: &'static str) -> Self {
        Self(value)
    }
}

impl<'de> Deserialize<'de> for VersionNumber {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl Serialize for VersionNumber {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl FromStr for VersionNumber {
    type Err = ParseVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (version, beta_version) = s.split_once("b").unzip();
        let version = version.unwrap_or(s);
        let mut parts = version.splitn(3, '.');
        let major = parts
            .next()
            .ok_or("Missing major version")?
            .parse()
            .map_err(|_| "Invalid major version number")?;
        let minor = parts
            .next()
            .ok_or("Missing minor version")?
            .parse()
            .map_err(|_| "Invalid minor version number")?;
        let patch = parts
            .next()
            .ok_or("Missing patch version")?
            .parse()
            .map_err(|_| "Invalid patch version number")?;

        let beta_version = beta_version
            .map(u32::from_str)
            .transpose()
            .map_err(|_| "Invalid beta version")?;
        Ok(Self {
            major,
            minor,
            patch,
            beta: beta_version,
        })
    }
}

impl TryFrom<String> for VersionNumber {
    type Error = <VersionNumber as FromStr>::Err;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl VersionNumber {
    pub const MAX: Self = VersionNumber::new(u32::MAX, u32::MAX, u32::MAX);
    pub const MIN: Self = VersionNumber::new(u32::MIN, u32::MIN, u32::MIN);
}

impl Display for VersionNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if let Some(beta) = self.beta {
            write!(f, "b{beta}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(version: &'static str, actual: VersionNumber) {
        let parsed: VersionNumber = version.parse().unwrap();
        assert_eq!(parsed, actual)
    }

    #[test]
    fn test_parse_version_number() {
        check("1.2.0", VersionNumber::new(1, 2, 0));
        assert!(VersionNumber::from_str("1.2").is_err());
        assert!(VersionNumber::from_str("1.2b2").is_err());

        check("1.2.0b1", VersionNumber::new(1, 2, 0).with_beta(1));
        assert!(VersionNumber::from_str("1.2b").is_err());
        assert!(VersionNumber::from_str("1.2bp").is_err());
        assert!(VersionNumber::from_str("1.2a1").is_err());
    }

    #[test]
    fn test_version_cmp() {
        assert!(VersionNumber::new(1, 2, 0) > VersionNumber::new(1, 1, 8));
        assert!(VersionNumber::new(1, 1, 0) < VersionNumber::new(1, 1, 8));
        assert!(VersionNumber::new(1, 2, 0) > VersionNumber::new(1, 2, 0).with_beta(1));
        assert!(VersionNumber::new(1, 3, 0) > VersionNumber::new(1, 2, 0).with_beta(1));
        assert!(VersionNumber::new(1, 1, 0) < VersionNumber::new(1, 2, 0).with_beta(1));
    }
}
