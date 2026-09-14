use std::cmp::Ordering;
use std::fmt::Display;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// The kind of a pre-release, ordered from earliest to latest in the release cycle.
///
/// Spelled the way PEP 440 normalises them: `b` and `rc`.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy, Hash)]
pub enum PreReleaseKind {
    Beta,
    ReleaseCandidate,
}

impl PreReleaseKind {
    const fn as_str(self) -> &'static str {
        match self {
            PreReleaseKind::Beta => "b",
            PreReleaseKind::ReleaseCandidate => "rc",
        }
    }
}

/// A pre-release marker such as `b1` or `rc2`.
///
/// Pre-releases of the same kind order by number; across kinds a beta comes before a release
/// candidate.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy, Hash)]
pub struct PreRelease {
    pub kind: PreReleaseKind,
    pub number: u32,
}

impl PreRelease {
    pub const fn beta(number: u32) -> Self {
        Self {
            kind: PreReleaseKind::Beta,
            number,
        }
    }
    pub const fn rc(number: u32) -> Self {
        Self {
            kind: PreReleaseKind::ReleaseCandidate,
            number,
        }
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy, Hash)]
pub struct VersionNumber {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub pre_release: Option<PreRelease>,
}

impl Ord for VersionNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        let version =
            (self.major, self.minor, self.patch).cmp(&(other.major, other.minor, other.patch));
        version.then_with(|| match (self.pre_release, other.pre_release) {
            (Some(l), Some(r)) => l.cmp(&r),
            // A pre-release of the same version is earlier than the regular version
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
            pre_release: None,
        }
    }
    pub const fn with_beta(mut self, beta: u32) -> Self {
        self.pre_release = Some(PreRelease::beta(beta));
        self
    }
    pub const fn with_rc(mut self, rc: u32) -> Self {
        self.pre_release = Some(PreRelease::rc(rc));
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
        // The pre-release suffix starts at the first non-digit, non-dot character, e.g.
        // `1.2.0b1` or `2.0.0rc1`.
        let (version, pre_release) = match s.find(|c: char| !c.is_ascii_digit() && c != '.') {
            Some(idx) => (&s[..idx], Some(&s[idx..])),
            None => (s, None),
        };

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

        let pre_release = pre_release.map(PreRelease::from_str).transpose()?;
        Ok(Self {
            major,
            minor,
            patch,
            pre_release,
        })
    }
}

impl FromStr for PreRelease {
    type Err = ParseVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (kind, number) = if let Some(number) = s.strip_prefix("rc") {
            (PreReleaseKind::ReleaseCandidate, number)
        } else if let Some(number) = s.strip_prefix('b') {
            (PreReleaseKind::Beta, number)
        } else {
            return Err("Invalid pre-release version".into());
        };
        let number = number.parse().map_err(|_| "Invalid pre-release version")?;
        Ok(Self { kind, number })
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

impl Display for PreRelease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.kind.as_str(), self.number)
    }
}

impl Display for VersionNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if let Some(pre_release) = self.pre_release {
            write!(f, "{pre_release}")?;
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
        check("2.0.0rc1", VersionNumber::new(2, 0, 0).with_rc(1));
        check("2.0.0rc12", VersionNumber::new(2, 0, 0).with_rc(12));
        assert!(VersionNumber::from_str("1.2b").is_err());
        assert!(VersionNumber::from_str("1.2bp").is_err());
        assert!(VersionNumber::from_str("1.2.0rc").is_err());
        assert!(VersionNumber::from_str("1.2.0rcx").is_err());
        assert!(VersionNumber::from_str("1.2.0a1").is_err());
        assert!(VersionNumber::from_str("1.2.0c1").is_err());
        assert!(VersionNumber::from_str("1.2.0-rc.1").is_err());
        assert!(VersionNumber::from_str("1.2.0.dev1").is_err());
    }

    #[test]
    fn test_display_round_trip() {
        for version in ["1.2.0", "1.2.0b1", "2.0.0rc1"] {
            let parsed: VersionNumber = version.parse().unwrap();
            assert_eq!(parsed.to_string(), version);
        }
    }

    #[test]
    fn test_version_cmp() {
        assert!(VersionNumber::new(1, 2, 0) > VersionNumber::new(1, 1, 8));
        assert!(VersionNumber::new(1, 1, 0) < VersionNumber::new(1, 1, 8));
        assert!(VersionNumber::new(1, 2, 0) > VersionNumber::new(1, 2, 0).with_beta(1));
        assert!(VersionNumber::new(1, 3, 0) > VersionNumber::new(1, 2, 0).with_beta(1));
        assert!(VersionNumber::new(1, 1, 0) < VersionNumber::new(1, 2, 0).with_beta(1));

        // Within a version: beta < rc < final release
        assert!(VersionNumber::new(2, 0, 0).with_beta(9) < VersionNumber::new(2, 0, 0).with_rc(1));
        assert!(VersionNumber::new(2, 0, 0).with_rc(1) < VersionNumber::new(2, 0, 0).with_rc(2));
        assert!(VersionNumber::new(2, 0, 0).with_rc(9) < VersionNumber::new(2, 0, 0));
        assert!(VersionNumber::new(1, 99, 0) < VersionNumber::new(2, 0, 0).with_rc(1));
    }
}
