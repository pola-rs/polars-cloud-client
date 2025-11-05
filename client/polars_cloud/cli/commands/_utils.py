from __future__ import annotations

import sys
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Generator

from polars_cloud.exceptions import (
    AuthenticationError,
    VerificationTimeoutError,
    WorkspaceDeploymentError,
)


@contextmanager
def handle_errors() -> Generator[Any]:
    """Catch any errors from the Python SDK and convert them to system exit."""
    try:
        yield
    except AuthenticationError:
        sys.exit(
            "ERROR: No valid authentication token found. Please run `pc login` first."
        )
    except (VerificationTimeoutError, WorkspaceDeploymentError) as e:
        sys.exit(f"ERROR: {e}")
    except Exception as e:
        sys.exit(f"ERROR: {e}")
