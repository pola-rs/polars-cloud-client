from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, TypeVar

from polars_cloud.polars_cloud import TraceSpan

if TYPE_CHECKING:
    from collections.abc import Callable

F = TypeVar("F", bound="Callable[..., Any]")


def traced(fn: F) -> F:
    name = getattr(fn, "__qualname__", None) or fn.__name__

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with TraceSpan(name):
            return fn(*args, **kwargs)

    return wrapper  # type: ignore[return-value]
