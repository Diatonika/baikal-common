from collections.abc import Callable
from logging import Logger

from rich.console import Console

from baikal.common.rich.log_handler import RichLogHandler


def with_handler[**P, T](
    logger: Logger, console: Console | None = None
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            with RichLogHandler(logger, console):
                return func(*args, **kwargs)

        return wrapper

    return decorator
