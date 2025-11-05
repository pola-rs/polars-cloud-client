from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sys

    import polars_cloud.polars_cloud as pcr

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self


class WorkspaceDefaultComputeSpecs:
    """Default cluster settings of a Workspace.

    Default cluster settings of a Workspace.
    This is a global setting that is applicable to all users in the workspace.

    Parameters
    ----------
    cluster_size
        The number of nodes in the cluster
    instance_type
        The AWS instance type of each node (e.g. t2.large)
    memory
        The amount of RAM memory in GB for each node
    cpus
        The amount of vCPU cores for each node
    storage
        The amount of disk storage on each node

    Examples
    --------
    >>> workspace = pc.Workspace(name="xxx")
    >>> defaults = pc.WorkspaceDefaultComputeSpecs(memory=8, cpus=4, storage=256)
    >>> workspace.defaults = defaults
    """

    def __init__(
        self,
        cluster_size: int = 1,
        instance_type: str | None = None,
        cpus: int | None = None,
        memory: int | None = None,
        storage: int | None = None,
    ):
        self.instance_type = instance_type
        self.memory = memory
        self.cpus = cpus
        self.storage = storage
        self.cluster_size = cluster_size

    @classmethod
    def _from_api_schema(cls, value: pcr.DefaultComputeSpecs) -> Self:
        return cls(
            instance_type=value.instance_type,
            memory=value.ram_gb,
            cpus=value.cpus,
            storage=value.storage,
            cluster_size=value.cluster_size,
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"instance_type={self.instance_type!r}, "
            f"cpus={self.cpus!r} vCPU cores, "
            f"memory={self.memory!r} GB, "
            f"storage={self.storage!r} GB, "
            f"cluster_size={self.cluster_size!r} )"
        )
