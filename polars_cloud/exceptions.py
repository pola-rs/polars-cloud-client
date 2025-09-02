"""Module for custom Polars Cloud exceptions."""

from __future__ import annotations


class AuthenticationError(Exception):
    """Exception raised when a valid authentication token could not be found."""


class VerificationTimeoutError(Exception):
    """Exception raised when verification has timed out."""


class OrganizationResolveError(Exception):
    """Exception raised when organization could not be resolved."""


class WorkspaceResolveError(Exception):
    """Exception raised when workspace could not be resolved."""


class ComputeClusterMisspecified(Exception):
    """Exception raised when compute cluster is not correctly specified."""
