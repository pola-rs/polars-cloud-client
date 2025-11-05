from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from argparse import ArgumentParser


def add_organization_parser(
    command_parsers: Any, common_parser: ArgumentParser
) -> None:
    # Workspace
    parser = command_parsers.add_parser(
        "organization",
        help="Manage Polars Cloud organizations.",
        parents=[common_parser],
    )
    subparsers = parser.add_subparsers(dest="organization_command")
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

    # Organization - list
    _org_list_parser = subparsers.add_parser(
        "list",
        help="List all active organizations",
        parents=[common_parser],
    )

    # Organization - setup
    setup_parser = subparsers.add_parser(
        "setup",
        help="Set up an organization",
        parents=[common_parser],
    )
    setup_parser.add_argument(
        "-n",
        "--name",
        dest="name",
        type=str,
        help="The desired name of the organization.",
    )

    # Organization - delete
    delete_parser = subparsers.add_parser(
        "delete",
        help="Delete an organization",
        parents=[common_parser],
    )
    delete_parser.add_argument(
        "-n",
        "--name",
        dest="name",
        type=str,
        help="The name of the organization.",
        required=True,
    )

    # Organization - details
    details_parser = subparsers.add_parser(
        "details",
        help="Print the details of an organization",
        parents=[common_parser],
    )
    details_parser.add_argument(
        "-n",
        "--name",
        dest="name",
        type=str,
        help="The name of the organization.",
        required=True,
    )
