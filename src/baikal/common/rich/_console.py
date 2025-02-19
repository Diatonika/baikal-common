from contextvars import ContextVar, Token
from types import TracebackType
from typing import Self

from dynaconf import LazySettings
from rich import pretty
from rich.console import Console
from rich.theme import Theme


class ConsoleContext:
    _ACTIVE_CONSOLE: ContextVar[Console | None] = ContextVar(
        "_baikal_rich_console", default=None
    )
    _DEFAULT_THEME = {
        "error": "red3",
        "warning": "orange3",
        "info": "white",
        "debug": "white",
    }

    _console: Console
    _context_token: Token[Console | None]

    def __init__(self, console: Console) -> None:
        self._console = console

    @classmethod
    def from_dynaconf(cls, settings: LazySettings) -> Console:
        return cls.from_parameters(settings("baikal.common.rich.styles"))

    @classmethod
    def from_parameters(cls, styles: dict[str, str]) -> Console:
        return Console(theme=Theme(cls._DEFAULT_THEME | styles))

    @classmethod
    def active(cls) -> Console:
        active_console = cls._ACTIVE_CONSOLE.get()
        return (
            active_console
            if active_console is not None
            else ConsoleContext.from_parameters({})
        )

    def __enter__(self) -> Self:
        self._context_token = self._ACTIVE_CONSOLE.set(self._console)
        pretty.install(self._console)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._context_token.old_value != Token.MISSING:
            pretty.install(self._context_token.old_value)

        self._ACTIVE_CONSOLE.reset(self._context_token)
