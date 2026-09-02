from enum import Enum
from typing import final


@final
class ProviderType(Enum):
    """The type of infrastructure providers for workspaces."""

    AWS = 0
    KUBERNETES = 1

    def __repr__(self) -> str:
        return self.name
