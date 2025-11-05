"""Contains global variable for caching the active compute context."""

from __future__ import annotations

from contextlib import ContextDecorator
from typing import TYPE_CHECKING, Any, Callable, Literal, TypeVar

if TYPE_CHECKING:
    from types import TracebackType

    from polars_cloud.context.compute import ClientContext

cached_context: ClientContext | None = None


class set_compute_context(ContextDecorator):
    """Set a compute context as the default during this session.

    Setting a compute context allows spawning queries without explicitly passing a
    compute context.

    See Also
    --------
    ComputeContext

    Examples
    --------
    ::

        small_machine = pc.ComputeContext(cpus=1, memory=2)
        large_machine = pc.ComputeContext(cpus=2, memory=4)

        lf = pl.LazyFrame({"a": [1, 2, 3, 4]})

        # Queries will execute on the small machine by default
        pc.set_compute_context(small_machine)

        with pc.set_compute_context(large_machine):
            # These queries will execute on the large machine
            lf.remote().show()
            lf.select(a_sum=pl.col("a").sum()).remote().show()


        @pc.set_compute_context(large_machine)
        def execute_on_large_machine(lf: pl.LazyFrame):
            # These queries will execute on the large machine
            lf.remote().show()
            lf.select(a_min=pl.col("a").min(), a_max=pl.col("a").max()).remote().show()

    """

    def __init__(self, context: ClientContext | None) -> None:
        super().__init__()
        self._context = context
        global cached_context
        # We set the cached context here, so it can be used as both a regular
        # function, and as a context manager
        self._old_context, cached_context = cached_context, self._context

    def _set_global_context(self, value: ClientContext | None) -> None:
        global cached_context
        cached_context = value

    def _reset_global_context(self) -> None:
        self._set_global_context(self._old_context)

    _F = TypeVar("_F", bound=Callable[..., Any])

    def __call__(self, func: _F) -> _F:
        # If this class is being used as a decorator, we need to reset the
        # global context, because it should only be set when the function is
        # entered
        self._reset_global_context()
        return super().__call__(func)

    def __enter__(self) -> None:
        # This is a no-op in the context-manager case, but is important in the
        # decorator case
        self._set_global_context(self._context)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        self._set_global_context(self._old_context)
        return False
