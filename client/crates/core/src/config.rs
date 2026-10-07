use std::fmt::Display;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use directories::BaseDirs;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::client_trait::ControlPlaneClient;
use crate::constants::{
    ACCESS_TOKEN_FILENAME, API_ADDR_ENV, API_DOMAIN_PREFIX_ENV, DEFAULT_API_DOMAIN_PREFIX,
    DEFAULT_DOMAIN, DEFAULT_ORGANIZATION_ID_ENV, DEFAULT_WORKSPACE_ID_ENV, DOMAIN_ENV,
    REFRESH_TOKEN_FILENAME, USER_NAME_ENV,
};
use crate::error::ApiError;
use crate::{ACCESS_TOKEN_PATH_ENV, AuthError};

const CONFIG_FILENAME: &str = "config.toml";

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PolarsCloudConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_workspace_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_organization_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_addr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_domain_prefix: Option<String>,
    pub auth: AuthConfig,

    #[serde(skip)]
    config_dir: PathBuf,
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
}

static CONFIG: LazyLock<RwLock<PolarsCloudConfig>> = LazyLock::new(|| {
    RwLock::new(PolarsCloudConfig::read_from_dir_and_migrate(
        &resolve_config_dir(),
    ))
});

/// The directory named by `POLARS_CLOUD_CONFIG_DIR`, or the platform config directory.
fn resolve_config_dir() -> PathBuf {
    std::env::var(ACCESS_TOKEN_PATH_ENV)
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            BaseDirs::new()
                .ok_or_else(|| anyhow!("Unable to determine user's config directory"))
                .unwrap()
                .config_dir()
                .join("polars_cloud")
        })
}

/// Warn the user about a config problem. Falls back to stderr when nothing is installed to
/// render the event, so warnings are not lost in the Python package.
fn warn_user(message: impl Display) {
    tracing::warn!("{message}");
    if !tracing::dispatcher::has_been_set() {
        eprintln!("polars_cloud: warning: {message}");
    }
}

impl PolarsCloudConfig {
    pub fn global() -> RwLockReadGuard<'static, PolarsCloudConfig> {
        CONFIG.read().unwrap()
    }

    pub fn global_mut() -> RwLockWriteGuard<'static, PolarsCloudConfig> {
        CONFIG.write().unwrap()
    }

    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    fn config_path(dir: &Path) -> PathBuf {
        dir.join(CONFIG_FILENAME)
    }

    /// Load the config file from `dir`, folding any legacy token files into it.
    pub fn read_from_dir_and_migrate(dir: &Path) -> Self {
        let mut config = Self::read_from_dir(dir);
        config.migrate_legacy_tokens();
        config
    }

    /// Read and parse the config file in `dir`.
    fn read_from_dir(dir: &Path) -> Self {
        let source = Self::config_path(dir);

        tracing::debug!(load_path=?source, "reading config file from disk");

        let config = match fs::read_to_string(&source) {
            Ok(contents) => Self::parse_or_quarantine(&source, &contents),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                tracing::debug!(load_path=?source, "no config file yet, starting from defaults");
                Self::default()
            },
            Err(e) => {
                warn_user(format!(
                    "config file {} could not be read ({e}), starting from defaults",
                    source.display()
                ));
                Self::default()
            },
        };

        PolarsCloudConfig {
            config_dir: dir.to_path_buf(),
            ..config
        }
    }

    fn parse_or_quarantine(source: &Path, contents: &str) -> Self {
        let error = match toml::from_str::<Self>(contents) {
            Ok(config) => return config,
            Err(e) => e,
        };

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since_epoch| since_epoch.as_secs())
            .unwrap_or(0);
        let quarantine = source.with_extension(format!("toml.corrupt.{timestamp}"));
        warn_user(format!(
            "config file {} is not valid TOML ({}); moving it to {} and starting from defaults. You may need to log in again.",
            source.display(),
            error.message(),
            quarantine.display()
        ));
        if let Err(e) = fs::rename(source, &quarantine) {
            warn_user(format!(
                "could not move the invalid config file {} aside ({e}); it will be overwritten on the next write",
                source.display()
            ));
        }

        Self::default()
    }

    /// Read the persisted tokens, re-reading the `auth` section from disk so a login that
    /// happened after this process started (`pc login` in another terminal) is picked up.
    /// Every other field keeps its in-memory value.
    pub fn load_auth() -> AuthConfig {
        let mut config = Self::global_mut();
        config.auth = Self::read_from_dir(&config.config_dir).auth;
        config.auth.clone()
    }

    /// Move tokens from the legacy token files into this config and persist them. The
    /// legacy files are only removed once the config file is written.
    fn migrate_legacy_tokens(&mut self) {
        if self.auth.access_token.is_some() || self.auth.refresh_token.is_some() {
            return;
        }

        let dir = self.config_dir.clone();
        let access_token = fs::read_to_string(dir.join(ACCESS_TOKEN_FILENAME))
            .ok()
            .map(|v| v.trim().to_string());
        let refresh_token = fs::read_to_string(dir.join(REFRESH_TOKEN_FILENAME))
            .ok()
            .map(|v| v.trim().to_string());

        if access_token.is_none() && refresh_token.is_none() {
            return;
        }

        tracing::debug!(config_dir=?dir, "migrating legacy token files into the config file");
        self.auth.access_token = access_token;
        self.auth.refresh_token = refresh_token;

        match self.flush() {
            Ok(()) => Self::remove_legacy_token_files(&dir),
            Err(e) => warn_user(format!(
                "could not persist the migrated tokens to {} ({e}); keeping the legacy token files",
                Self::config_path(&dir).display()
            )),
        }
    }

    /// Mostly for tests
    pub fn reload() {
        *CONFIG.write().unwrap() = Self::read_from_dir_and_migrate(&resolve_config_dir());
    }

    fn remove_legacy_token_files(dir: &Path) {
        for filename in [ACCESS_TOKEN_FILENAME, REFRESH_TOKEN_FILENAME] {
            let path = dir.join(filename);
            if let Err(e) = fs::remove_file(&path)
                && e.kind() != ErrorKind::NotFound
            {
                warn_user(format!(
                    "could not remove the legacy token file {} after migration ({e})",
                    path.display()
                ));
            }
        }
    }

    pub fn flush(&self) -> Result<(), AuthError> {
        let dir = self.config_dir.as_path();
        fs::create_dir_all(dir)
            .map_err(|e| AuthError::new(&format!("Failed to create config directory: {e}")))?;

        let contents = toml::to_string_pretty(self)
            .map_err(|e| AuthError::new(&format!("Failed to serialize config: {e}")))?;

        let tmp = dir.join(format!(".{CONFIG_FILENAME}.tmp"));
        fs::write(&tmp, contents)
            .map_err(|e| AuthError::new(&format!("Failed to write config: {e}")))?;

        let destination = Self::config_path(dir);

        tracing::debug!(save_path=?destination, "saving config file to disk");

        fs::rename(&tmp, destination)
            .map_err(|e| AuthError::new(&format!("Failed to move config into place: {e}")))?;

        Ok(())
    }
}

/// Order of resolution = (explicit) argument -> env -> config -> (if available) server or hardcoded default
impl PolarsCloudConfig {
    pub fn resolve_username() -> Option<String> {
        std::env::var(USER_NAME_ENV)
            .ok()
            .or_else(|| Self::global().username.clone())
    }

    /// use [`resolve_default_workspace_id`] if you want to also check the server default
    pub fn default_workspace_id() -> Option<Uuid> {
        std::env::var(DEFAULT_WORKSPACE_ID_ENV)
            .ok()
            .and_then(|value| Uuid::parse_str(value.trim()).ok())
            .or_else(|| Self::global().default_workspace_id)
    }

    pub fn default_organization_id() -> Option<Uuid> {
        std::env::var(DEFAULT_ORGANIZATION_ID_ENV)
            .ok()
            .and_then(|value| Uuid::parse_str(value.trim()).ok())
            .or_else(|| Self::global().default_organization_id)
    }

    pub fn resolve_domain() -> String {
        std::env::var(DOMAIN_ENV)
            .ok()
            .or_else(|| Self::global().domain.clone())
            .unwrap_or_else(|| DEFAULT_DOMAIN.to_owned())
    }

    pub fn resolve_auth_domain() -> String {
        format!("auth.{}", Self::resolve_domain())
    }

    pub fn resolve_api_addr() -> String {
        std::env::var(API_ADDR_ENV)
            .ok()
            .or_else(|| Self::global().api_addr.clone())
            .unwrap_or_else(|| {
                let prefix = std::env::var(API_DOMAIN_PREFIX_ENV)
                    .ok()
                    .or_else(|| Self::global().api_domain_prefix.clone())
                    .unwrap_or_else(|| DEFAULT_API_DOMAIN_PREFIX.to_owned());
                format!("https://{prefix}.{}:443", Self::resolve_domain())
            })
    }

    /// The default workspace: the explicit argument, then the environment, then the config
    /// file, then the server-side default. `Ok(None)` means no default is set anywhere;
    /// errors are only returned for failures talking to the server.
    pub async fn resolve_default_workspace_id(
        client: &dyn ControlPlaneClient,
        workspace: Option<WorkspaceRef>,
        organization: Option<OrganizationRef>,
    ) -> Result<Option<Uuid>, ApiError> {
        let explicit = match workspace {
            Some(workspace) => Some(workspace.resolve_id(client, organization).await?),
            None => None,
        };

        if let Some(id) = explicit.or_else(Self::default_workspace_id) {
            return Ok(Some(id));
        }

        Ok(client.get_logged_in_user().await?.default_workspace_id)
    }

    pub fn set_username(username: Option<String>) -> Result<(), AuthError> {
        let mut config = Self::global_mut();
        config.username = username;
        config.flush()
    }

    pub async fn set_default_organization(
        client: &dyn ControlPlaneClient,
        organization: OrganizationRef,
    ) -> Result<Uuid, ApiError> {
        let organization_id = organization.resolve_id(client).await?;

        let mut config = Self::global_mut();
        config.default_organization_id = Some(organization_id);
        config.flush()?;
        Ok(organization_id)
    }

    pub fn clear_default_workspace() -> Result<(), AuthError> {
        let mut config = Self::global_mut();
        config.default_workspace_id = None;
        config.flush()
    }

    pub fn clear_default_organization() -> Result<(), AuthError> {
        let mut config = Self::global_mut();
        config.default_organization_id = None;
        config.flush()
    }
}

impl PolarsCloudConfig {
    async fn organization_id_by_name(
        client: &dyn ControlPlaneClient,
        name: String,
    ) -> Result<Uuid, ApiError> {
        let matches: Vec<_> = client
            .get_organizations(Some(name.to_owned()))
            .await?
            .into_iter()
            .filter(|organization| organization.name.eq_ignore_ascii_case(&name))
            .collect();

        match matches.as_slice() {
            [] => Err(ApiError::Other(anyhow!(
                "No organization named {name:?} was found"
            ))),
            [organization] => Ok(organization.id),
            _ => Err(ApiError::Other(anyhow!(
                "Multiple organizations named {name:?}"
            ))),
        }
    }

    async fn workspace_id_by_name(
        client: &dyn ControlPlaneClient,
        name: String,
        organization_id: Option<Uuid>,
    ) -> Result<Uuid, ApiError> {
        let matches: Vec<_> = client
            .get_workspaces(Some(name.to_owned()), organization_id)
            .await?
            .into_iter()
            .filter(|workspace| workspace.name.eq_ignore_ascii_case(&name))
            .collect();

        match matches.as_slice() {
            [] => Err(ApiError::Other(anyhow!(
                "Workspace {name:?} does not exist"
            ))),
            [workspace] => Ok(workspace.id),
            _ => Err(ApiError::Other(anyhow!(
                "Multiple workspaces named {name:?}.\n\nHint: pass an organization to disambiguate."
            ))),
        }
    }

    pub async fn set_default_workspace(
        client: &dyn ControlPlaneClient,
        workspace: WorkspaceRef,
        organization: Option<OrganizationRef>,
    ) -> Result<Uuid, ApiError> {
        let workspace_id = workspace.resolve_id(client, organization).await?;

        let mut config = Self::global_mut();
        config.default_workspace_id = Some(workspace_id);
        config.flush()?;
        Ok(workspace_id)
    }
}

#[derive(Debug, Clone)]
pub enum WorkspaceRef {
    Name(String),
    Id(Uuid),
}

#[derive(Debug, Clone)]
pub enum OrganizationRef {
    Name(String),
    Id(Uuid),
}

impl WorkspaceRef {
    pub fn from_name_or_id(name: Option<String>, id: Option<Uuid>) -> Result<Self, ApiError> {
        match (name, id) {
            (Some(_), Some(_)) => Err(ApiError::Other(anyhow!(
                "Specify the workspace by name or by id, not both"
            ))),
            (Some(name), None) => Ok(Self::Name(name)),
            (None, Some(id)) => Ok(Self::Id(id)),
            (None, None) => Err(ApiError::Other(anyhow!(
                "Specify a workspace by name or by id"
            ))),
        }
    }

    async fn resolve_id(
        self,
        client: &dyn ControlPlaneClient,
        organization: Option<OrganizationRef>,
    ) -> Result<Uuid, ApiError> {
        match self {
            Self::Id(id) => Ok(id),
            Self::Name(name) => {
                let organization_id = match organization {
                    Some(organization) => Some(organization.resolve_id(client).await?),
                    None => None,
                };
                PolarsCloudConfig::workspace_id_by_name(client, name, organization_id).await
            },
        }
    }
}

impl OrganizationRef {
    pub fn from_name_or_id(name: Option<String>, id: Option<Uuid>) -> Result<Self, ApiError> {
        match (name, id) {
            (Some(_), Some(_)) => Err(ApiError::Other(anyhow!(
                "Specify the organization by name or by id, not both"
            ))),
            (Some(name), None) => Ok(Self::Name(name)),
            (None, Some(id)) => Ok(Self::Id(id)),
            (None, None) => Err(ApiError::Other(anyhow!(
                "Specify an organization by name or by id"
            ))),
        }
    }

    /// Resolve to an id, hitting the server only when given a name.
    async fn resolve_id(self, client: &dyn ControlPlaneClient) -> Result<Uuid, ApiError> {
        match self {
            Self::Id(id) => Ok(id),
            Self::Name(name) => PolarsCloudConfig::organization_id_by_name(client, name).await,
        }
    }
}
