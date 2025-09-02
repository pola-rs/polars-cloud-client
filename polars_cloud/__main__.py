"""Polars Cloud command line interface.

Can be called either with `python -m polars_cloud` or `pc`.
"""

from polars_cloud.cli import cli

__all__ = ["cli"]


if __name__ == "__main__":
    cli()
