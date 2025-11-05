from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from argparse import ArgumentParser


def add_login_parser(command_parsers: Any, common_parser: ArgumentParser) -> None:
    command_parsers.add_parser(
        "login",
        help="Authenticate with Polars Cloud by logging in through the browser",
        parents=[common_parser],
    )
