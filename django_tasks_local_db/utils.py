import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

T = TypeVar("T")
P = ParamSpec("P")


def retry(*, retries: int = 3, backoff_delay: float = 0.1) -> Callable:
    """
    Retry the given code `retries` times, raising the final error.

    `backoff_delay` can be used to add a delay between attempts.
    """

    def wrapper(f: Callable[P, T]) -> Callable[P, T]:
        @wraps(f)
        def inner_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:  # type:ignore[return]
            for attempt in range(1, retries + 1):
                try:
                    return f(*args, **kwargs)
                except KeyboardInterrupt:
                    raise
                except BaseException:
                    if attempt == retries:
                        raise
                    time.sleep(backoff_delay)

        return inner_wrapper

    return wrapper
