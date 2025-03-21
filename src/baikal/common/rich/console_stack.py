from contextvars import ContextVar, Token
from types import TracebackType

from rich import pretty
from rich.console import Console


class RichConsoleStack:
    _ACTIVE_CONSOLE: ContextVar[Console | None] = ContextVar(
        "_baikal_rich_console", default=None
    )

    _console: Console
    _context_token: Token[Console | None]

    def __init__(self, console: Console) -> None:
        self._console = console

    @classmethod
    def active_console(cls) -> Console:
        active_console = cls._ACTIVE_CONSOLE.get()
        return active_console if active_console is not None else Console()

    @property
    def console(self) -> Console:
        return self._console

    def __enter__(self) -> Console:
        self._context_token = self._ACTIVE_CONSOLE.set(self._console)
        pretty.install(self._console)
        return self._console

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._context_token.old_value != Token.MISSING:
            pretty.install(self._context_token.old_value)

        self._ACTIVE_CONSOLE.reset(self._context_token)
