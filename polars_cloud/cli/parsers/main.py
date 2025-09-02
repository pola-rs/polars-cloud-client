from __future__ import annotations

from argparse import ArgumentParser

from polars_cloud._version import __version__
from polars_cloud.cli.parsers.authenticate import add_authenticate_parser
from polars_cloud.cli.parsers.compute import add_compute_parser
from polars_cloud.cli.parsers.login import add_login_parser
from polars_cloud.cli.parsers.organization import add_organization_parser
from polars_cloud.cli.parsers.setup import add_setup_parser
from polars_cloud.cli.parsers.workspace import add_workspace_parser


def create_parser() -> ArgumentParser:
    """Create the CLI parser."""
    common_parser = _create_common_parser()

    parser = _create_main_parser(common_parser)

    subparsers = parser.add_subparsers(dest="command")
    add_authenticate_parser(subparsers, common_parser)
    add_setup_parser(subparsers, common_parser)
    add_login_parser(subparsers, common_parser)
    add_organization_parser(subparsers, common_parser)
    add_workspace_parser(subparsers, common_parser)
    add_compute_parser(subparsers, common_parser)

    return parser


def _create_common_parser() -> ArgumentParser:
    """Parser with arguments common to all subparsers."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="store_true",
        help="Output debug logging messages.",
        default=False,
    )
    return parser


def _create_main_parser(common_parser: ArgumentParser) -> ArgumentParser:
    """Main parser containing all subparsers."""
    parser = ArgumentParser("pc", parents=[common_parser])
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        help="Display the version of the Polars Cloud client.",
        version=__version__,
    )
    return parser
