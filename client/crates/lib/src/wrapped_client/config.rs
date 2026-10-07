#![allow(clippy::result_large_err)]

use client_core::{ApiError, OrganizationRef, PolarsCloudConfig, RUNTIME, WorkspaceRef};
use pyo3::{Python, pymethods};
use uuid::Uuid;

use crate::entry::EnterRustExt;
use crate::wrapped_client::WrappedAPIClient;

#[pymethods]
impl WrappedAPIClient {
    /// Resolve a workspace by name against the server and persist its id as the
    /// default. Returns the resolved id.
    #[pyo3(signature = (workspace_name=None, workspace_id=None, organization_name=None, organization_id=None))]
    pub fn set_default_workspace(
        &self,
        py: Python,
        workspace_name: Option<String>,
        workspace_id: Option<Uuid>,
        organization_name: Option<String>,
        organization_id: Option<Uuid>,
    ) -> Result<Uuid, ApiError> {
        let workspace = WorkspaceRef::from_name_or_id(workspace_name, workspace_id)?;
        let organization = match (organization_name, organization_id) {
            (None, None) => None,
            (name, id) => Some(OrganizationRef::from_name_or_id(name, id)?),
        };
        py.enter_rust(|| {
            RUNTIME.block_on(PolarsCloudConfig::set_default_workspace(
                self.client.as_ref(),
                workspace,
                organization,
            ))?
        })
    }

    #[pyo3(signature = (organization_name=None, organization_id=None))]
    pub fn set_default_organization(
        &self,
        py: Python,
        organization_name: Option<String>,
        organization_id: Option<Uuid>,
    ) -> Result<Uuid, ApiError> {
        let organization = OrganizationRef::from_name_or_id(organization_name, organization_id)?;
        py.enter_rust(|| {
            RUNTIME.block_on(PolarsCloudConfig::set_default_organization(
                self.client.as_ref(),
                organization,
            ))?
        })
    }

    /// The default workspace from the environment, the config file or the server, or
    /// `None` when no default is set anywhere. Use [`get_default_workspace_id`] to skip
    /// the server.
    #[pyo3(signature = (workspace_name=None, workspace_id=None, organization_name=None, organization_id=None))]
    pub fn resolve_default_workspace_id(
        &self,
        py: Python,
        workspace_name: Option<String>,
        workspace_id: Option<Uuid>,
        organization_name: Option<String>,
        organization_id: Option<Uuid>,
    ) -> Result<Option<Uuid>, ApiError> {
        let workspace = WorkspaceRef::from_name_or_id(workspace_name, workspace_id).ok();
        let organization = match (organization_name, organization_id) {
            (None, None) => None,
            (name, id) => Some(OrganizationRef::from_name_or_id(name, id)?),
        };

        py.enter_rust(|| {
            RUNTIME.block_on(PolarsCloudConfig::resolve_default_workspace_id(
                self.client.as_ref(),
                workspace,
                organization,
            ))?
        })
    }

    /// Drop the cached config and read it from disk again. Mostly for tests, which write
    /// config files behind the running process's back.
    pub fn reload_config(&self) {
        PolarsCloudConfig::reload();
    }

    pub fn get_default_workspace_id(&self) -> Option<Uuid> {
        PolarsCloudConfig::default_workspace_id()
    }

    pub fn get_default_organization_id(&self) -> Option<Uuid> {
        PolarsCloudConfig::default_organization_id()
    }

    pub fn clear_default_workspace(&self) -> Result<(), ApiError> {
        PolarsCloudConfig::clear_default_workspace().map_err(Into::into)
    }

    pub fn clear_default_organization(&self) -> Result<(), ApiError> {
        PolarsCloudConfig::clear_default_organization().map_err(Into::into)
    }

    #[pyo3(signature = (username=None))]
    pub fn set_username(&self, username: Option<String>) -> Result<(), ApiError> {
        PolarsCloudConfig::set_username(username).map_err(Into::into)
    }

    pub fn get_username(&self) -> Option<String> {
        PolarsCloudConfig::resolve_username()
    }
}
