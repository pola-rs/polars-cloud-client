from __future__ import annotations

import argparse
import logging
import os
from typing import TYPE_CHECKING

from polars_cloud import authenticate, constants, login
from polars_cloud.cli.commands.compute import (
    get_compute_details,
    list_compute,
    start_compute,
    stop_compute,
)
from polars_cloud.cli.commands.organization import (
    delete_organization,
    get_organization_details,
    list_organizations,
    set_up_organization,
)
from polars_cloud.cli.commands.setup import (
    setup,
)
from polars_cloud.cli.commands.workspace import (
    delete_workspace,
    get_workspace_details,
    list_workspaces,
    set_up_workspace,
    verify_workspace,
)
from polars_cloud.cli.parsers import create_parser

if TYPE_CHECKING:
    from argparse import ArgumentParser, Namespace


def cli() -> None:
    """Command line interface for Polars Cloud."""
    parser = create_parser()
    args = parser.parse_args()
    _configure_logging(verbose=args.verbose)
    _set_access_token(args)
    _execute_command(parser, args)


def _configure_logging(*, verbose: bool) -> None:
    """Configure the logging format and level."""
    format = "%(asctime)s | %(levelname)s | %(message)s"
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(format=format, level=level)


def _set_access_token(args: Namespace) -> None:
    """Set the access token as environment variables if passed."""
    # The login command does not use an access token
    if args.command not in ("compute", "workspace"):
        return

    if args.token is not None:
        os.environ[constants.ACCESS_TOKEN] = args.token


def _execute_command(parser: ArgumentParser, args: Namespace) -> None:
    """Run the corresponding pipeline."""
    command = args.command
    if command is None:
        parser.print_help()
        return

    if command == "authenticate":
        authenticate()

    elif command == "login":
        login()

    elif command == "setup":
        setup(args.organization_name, args.workspace_name)

    elif command == "organization":
        subcommand = args.organization_command
        if subcommand is None:
            _print_subparser_help(parser, "organization")
            return
        elif subcommand == "list":
            list_organizations()
        elif subcommand == "setup":
            set_up_organization(args.name)
        elif subcommand == "delete":
            delete_organization(args.name)
        elif subcommand == "details":
            get_organization_details(args.name)
        else:  # Unreachable
            msg = f"invalid `organization` subcommand: {subcommand}"
            raise RuntimeError(msg)
    elif command == "workspace":
        subcommand = args.workspace_command
        if subcommand is None:
            _print_subparser_help(parser, "workspace")
            return
        elif subcommand == "list":
            list_workspaces()
        elif subcommand == "setup":
            set_up_workspace(args.name, args.organization_name, verify=args.verify)
        elif subcommand == "verify":
            verify_workspace(args.name, interval=args.interval, timeout=args.timeout)
        elif subcommand == "delete":
            delete_workspace(args.name)
        elif subcommand == "details":
            get_workspace_details(args.name)
        else:  # Unreachable
            msg = f"invalid `workspace` subcommand: {subcommand}"
            raise RuntimeError(msg)

    elif command == "compute":
        subcommand = args.compute_command
        if subcommand is None:
            _print_subparser_help(parser, "compute")
            return
        elif subcommand == "start":
            start_compute(
                workspace=args.workspace,
                cpus=args.cpus,
                memory=args.memory,
                instance_type=args.instance_type,
                storage=args.storage,
                cluster_size=args.cluster_size,
                wait=args.wait,
            )
        elif subcommand == "stop":
            stop_compute(args.workspace, args.id, wait=args.wait)
        elif subcommand == "details":
            get_compute_details(args.workspace, args.id)
        elif subcommand == "list":
            list_compute()
        else:  # Unreachable
            msg = f"invalid `compute` subcommand: {subcommand}"
            raise RuntimeError(msg)

    else:  # Unreachable
        msg = f"invalid command: {command}"
        raise RuntimeError(msg)


def _print_subparser_help(parser: ArgumentParser, subcommand: str) -> None:
    """Print the help text of the subparser for the given subcommand."""
    # Note that we rely on private attributes to retrieve the subparsers.
    # Unfortunately, there doesn't seem to be a better way
    subparsers_action = next(
        action
        for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    subparser = subparsers_action.choices.get(subcommand)

    if subparser is None:
        msg = f"subcommand {subcommand!r} not found"
        raise ValueError(msg)

    subparser.print_help()
