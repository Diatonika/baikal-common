from collections.abc import Callable

from rich.console import Console

from baikal.common.rich import ConsoleContext


def with_console[**P, T](
    console: Console,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            with ConsoleContext(console):
                return func(*args, **kwargs)

        return wrapper

    return decorator
