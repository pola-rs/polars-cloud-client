from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from collections.abc import Coroutine

T = TypeVar("T")


def run_coroutine(coroutine: Coroutine[Any, Any, T], timeout: float | None = None) -> T:
    """Run asynchronous code as if it were synchronous.

    This is required for execution in Jupyter notebook environments.
    """
    # Implementation taken from StackOverflow answer here:
    # https://stackoverflow.com/a/78911765/2344703

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # If there is no running loop, use `asyncio.run` normally
        return asyncio.run(coroutine)

    def run_in_new_loop() -> T:
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        try:
            return new_loop.run_until_complete(coroutine)
        finally:
            new_loop.close()

    if threading.current_thread() is threading.main_thread():
        if not loop.is_running():
            return loop.run_until_complete(coroutine)
        else:
            with ThreadPoolExecutor() as pool:
                future = pool.submit(run_in_new_loop)
                return future.result(timeout=timeout)
    else:
        return asyncio.run_coroutine_threadsafe(coroutine, loop).result()
