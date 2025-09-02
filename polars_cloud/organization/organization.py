from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from polars_cloud import constants
from polars_cloud.exceptions import OrganizationResolveError

if TYPE_CHECKING:
    import sys

    import polars_cloud.polars_cloud as pcr

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self


class Organization:
    """A Polars Cloud organization.

    With this class you can list the organizations the user is a member of
    or set up a new organization

    Parameters
    ----------
     name
         The organization name.
     id
         The organization id.

    Examples
    --------
    >>> pc.Organization("organization-name")
    Organization(id=UUID('xxxxxxxx-xxxx-74e0-9de7-6e2014821b44'),
        name='organization-name',
    """

    def __init__(
        self,
        name: str | None = None,
        *,
        id: UUID | None = None,
    ):
        self._name = name
        self._id = id

        self.load()

        if name is not None and name != self._name:
            msg = f"The provided organization name {name!r} and id {id!r} do not match. The ID is of an organization named {self._name!r}."
            raise OrganizationResolveError(msg)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self._id!r}, name={self._name!r}, "

    @classmethod
    def _from_id_unchecked(cls, organization_id: UUID) -> Self:
        """Creates an instance of the object without API calls or invariant checks."""
        self = object.__new__(cls)
        self._id = organization_id
        return self

    @classmethod
    def _from_api_schema(cls, organization_schema: pcr.OrganizationSchema) -> Self:
        """Parse API result into a Python object."""
        self = object.__new__(cls)
        self._update_from_api_schema(organization_schema)
        return self

    def _update_from_api_schema(
        self, organization_schema: pcr.OrganizationSchema
    ) -> None:
        """Update the object from an API result."""
        self._id = organization_schema.id
        self._name = organization_schema.name

    @property
    def id(self) -> UUID:
        """Organization id."""
        if self._id is None:
            self.load()
        assert self._id is not None
        return self._id

    @property
    def name(self) -> str:
        """Organization name."""
        if self._name is None:
            self.load()
        assert self._name is not None
        return self._name

    @classmethod
    def _parse(
        cls,
        organization: str | Organization | UUID | None,
    ) -> Self:
        """Create a Workspace based on generic user input."""
        if isinstance(organization, Organization):
            return organization  # type: ignore[return-value]
        elif isinstance(organization, str):
            return cls(name=organization)
        elif isinstance(organization, UUID):
            return cls(id=organization)
        elif organization is None:
            return cls()
        else:
            msg = f"Unknown type {type(organization)}, expected str | Organization | UUID | None"
            raise RuntimeError(msg)

    def load(self) -> None:
        """Load the organization details (e.g. name, status, id) from the control plane.

        .. note::

         Depending on the input `load` will load the `Organization` object by id / name
        """
        if self._id is not None:
            self._load_by_id()
        elif self._name is not None:
            self._load_by_name()
        else:
            msg = "No organization was set via either ID or name"
            raise OrganizationResolveError(msg)

    def _load_by_name(self) -> None:
        """Load the workspace by name."""
        organizations = constants.API_CLIENT.get_organizations(self._name)

        # The API endpoint is a substring search, but we only want the exact name
        matches = [org for org in organizations if org.name == self._name]

        if len(matches) == 0:
            msg = f"Organization {self._name!r} does not exist"
            raise OrganizationResolveError(msg)
        else:
            organization = matches[0]
            self._id = organization.id

    def _load_by_id(self) -> None:
        """Load the workspace by id."""
        assert self._id is not None
        organization = constants.API_CLIENT.get_organization(self._id)
        self._name = organization.name

    def delete(self) -> None:
        """Delete an organization.

        Examples
        --------
        >>> pc.Organization("organization-name").delete()
        """
        constants.API_CLIENT.delete_organization(self.id)

    @classmethod
    def setup(cls, name: str) -> Self:
        """Create a new organization.

        Parameters
        ----------
        name
            Name of the organization

        Examples
        --------
        >>> pc.Organization.setup("organization-name)
        Organization(id=UUID('xxxxxxxx-xxxx-74e0-9de7-6e2014821b44'),
            name='organization-name',
        """
        organization = constants.API_CLIENT.create_organization(name)
        return cls._from_api_schema(organization)

    @classmethod
    def list(cls, name: str | None = None) -> list[Organization]:
        """List all organizations the user has access to.

        Parameters
        ----------
        name
            Filter organizations by name prefix.

        Examples
        --------
        >>> pc.Organization.list()
        [Organization(id=UUID('xxxxxxxx-xxxx-4d10-a8ee-8f63329329dc'),
            name='organization-name', ,
        Organization(id=UUID('xxxxxxxx-xxxx-7863-80d6-fb4959c59856'),
            name='different-organization',]
        """
        return [
            cls._from_api_schema(s)
            for s in constants.API_CLIENT.get_organizations(name)
        ]
