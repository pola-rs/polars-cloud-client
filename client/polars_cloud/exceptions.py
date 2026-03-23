"""Module for custom Polars Cloud exceptions."""

from __future__ import annotations


class WorkspaceDeploymentError(Exception):
    """Exception raised when the workspace deployment fails."""


class VerificationTimeoutError(Exception):
    """Exception raised when verification has timed out."""


class OrganizationResolveError(Exception):
    """Exception raised when organization could not be resolved."""


class WorkspaceResolveError(Exception):
    """Exception raised when workspace could not be resolved."""
