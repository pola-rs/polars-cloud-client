from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from argparse import ArgumentParser


def add_setup_parser(command_parsers: Any, common_parser: ArgumentParser) -> None:
    parser = command_parsers.add_parser(
        "setup",
        help="Set up an organization and workspace to quickly run queries. Ideal to get started with Polars Cloud.",
        parents=[common_parser],
    )
    parser.add_argument(
        "-o",
        "--organization_name",
        dest="organization_name",
        type=str,
        help="The desired name of the organization.",
    )
    parser.add_argument(
        "-w",
        "--workspace_name",
        dest="workspace_name",
        type=str,
        help="The desired name of the workspace.",
    )
