from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from argparse import ArgumentParser


def add_workspace_parser(command_parsers: Any, common_parser: ArgumentParser) -> None:
    # Workspace
    parser = command_parsers.add_parser(
        "workspace",
        help="Manage Polars Cloud workspaces.",
        parents=[common_parser],
    )
    subparsers = parser.add_subparsers(dest="workspace_command")
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

    # Workspace - list
    _ws_list_parser = subparsers.add_parser(
        "list",
        help="List all active workspaces",
        parents=[common_parser],
    )

    # Workspace - setup
    setup_parser = subparsers.add_parser(
        "setup",
        help="Set up a workspace in AWS",
        parents=[common_parser],
    )
    setup_parser.add_argument(
        "-n",
        "--name",
        dest="name",
        type=str,
        help="The desired name of the workspace.",
    )
    setup_parser.add_argument(
        "-o",
        "--organization_name",
        dest="organization_name",
        type=str,
        help="The name of the organization.",
    )
    setup_parser.add_argument(
        "--verify",
        dest="verify",
        action="store_true",
        help="Verify workspace setup.",
        default=True,
    )

    # Workspace - verify
    verify_parser = subparsers.add_parser(
        "verify",
        help="Verify workspace setup",
        parents=[common_parser],
    )
    verify_parser.add_argument(
        "-n",
        "--name",
        dest="name",
        type=str,
        help="The name of the workspace.",
        required=True,
    )
    verify_parser.add_argument(
        "-d",
        "--interval",
        dest="interval",
        type=int,
        help="The number of seconds between each verification call.",
    )
    verify_parser.add_argument(
        "-t",
        "--timeout",
        dest="timeout",
        type=int,
        help="The number of seconds before verification fails.",
    )

    # Workspace - delete
    delete_parser = subparsers.add_parser(
        "delete",
        help="Delete a workspace",
        parents=[common_parser],
    )
    delete_parser.add_argument(
        "-n",
        "--name",
        dest="name",
        type=str,
        help="The name of the workspace.",
        required=True,
    )

    # Workspace - details
    details_parser = subparsers.add_parser(
        "details",
        help="Print the details of a workspace",
        parents=[common_parser],
    )
    details_parser.add_argument(
        "-n",
        "--name",
        dest="name",
        type=str,
        help="The name of the workspace.",
        required=True,
    )
