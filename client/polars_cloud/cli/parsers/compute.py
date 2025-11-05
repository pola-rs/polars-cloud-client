from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    from argparse import ArgumentParser


def add_compute_parser(command_parsers: Any, common_parser: ArgumentParser) -> None:
    # Compute
    parser = command_parsers.add_parser(
        "compute",
        help="Manage Polars Cloud compute clusters.",
        parents=[common_parser],
    )
    subparsers = parser.add_subparsers(dest="compute_command")
    parser.add_argument(
        "-t",
        "--token",
        dest="token",
        type=str,
        help="A valid access token.",
    )
    parser.add_argument(
        "-p",
        "--token-path",
        dest="token_path",
        type=str,
        help="Path to a file containing a valid access token.",
    )

    # Compute - list
    _list_parser = subparsers.add_parser(
        "list",
        help="List all available compute clusters.",
        parents=[common_parser],
    )

    # Compute - start
    start_parser = subparsers.add_parser(
        "start",
        help="Start a compute cluster.",
        parents=[common_parser],
    )
    start_parser.add_argument(
        "-w",
        "--workspace",
        dest="workspace",
        type=str,
        help="The name of the workspace where the compute cluster should run.",
        required=False,
    )
    start_parser.add_argument(
        "-c",
        "--cpus",
        dest="cpus",
        type=int,
        help="The minimum number of CPUs the compute cluster should have access to.",
        required=False,
    )
    start_parser.add_argument(
        "-m",
        "--memory",
        dest="memory",
        type=int,
        help="The minimum amount of RAM (in GB) the compute cluster should have access to.",
        required=False,
    )
    start_parser.add_argument(
        "-t",
        "--instance-type",
        dest="instance_type",
        type=str,
        help="The instance type of the compute cluster.",
        required=False,
    )
    start_parser.add_argument(
        "-s",
        "--storage",
        dest="storage",
        type=int,
        help="The minimum amount of disk space (in GiB) each instance in the compute cluster should have access to.",
        required=False,
    )
    start_parser.add_argument(
        "-n",
        "--cluster-size",
        dest="cluster_size",
        type=int,
        help="The number of compute nodes in the cluster.",
        default=1,
    )
    start_parser.add_argument(
        "--wait",
        dest="wait",
        action="store_true",
        help="Wait for cluster to be running.",
        default=False,
    )

    # Compute - stop
    stop_parser = subparsers.add_parser(
        "stop",
        help="Stop a compute cluster.",
        parents=[common_parser],
    )
    stop_parser.add_argument(
        "-w",
        "--workspace",
        dest="workspace",
        type=str,
        help="The name of the workspace where the compute cluster should run.",
    )
    stop_parser.add_argument(
        "-i",
        "--id",
        dest="id",
        type=UUID,
        help="The identifier of the compute cluster.",
        required=True,
    )
    stop_parser.add_argument(
        "--wait",
        dest="wait",
        action="store_true",
        help="Wait for cluster to be terminated.",
        default=False,
    )

    # Compute - status
    status_parser = subparsers.add_parser(
        "status",
        help="Get the status of a compute cluster.",
        parents=[common_parser],
    )
    status_parser.add_argument(
        "-w",
        "--workspace",
        dest="workspace",
        type=str,
        help="The name of the workspace where the compute cluster should run.",
    )
    status_parser.add_argument(
        "-i",
        "--id",
        dest="id",
        type=UUID,
        help="The identifier of the compute cluster.",
        required=True,
    )

    # Compute - details
    details_parser = subparsers.add_parser(
        "details",
        help="Print the details of a compute cluster.",
        parents=[common_parser],
    )
    details_parser.add_argument(
        "-w",
        "--workspace",
        dest="workspace",
        type=str,
        help="The name of the workspace where the compute cluster should run.",
    )
    details_parser.add_argument(
        "-i",
        "--id",
        dest="id",
        type=UUID,
        help="The identifier of the compute cluster.",
        required=True,
    )
