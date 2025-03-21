from logging import Logger
from types import TracebackType

from rich.console import Console
from rich.logging import RichHandler

from baikal.common.rich.console_stack import RichConsoleStack


class RichLogHandler:
    handler: RichHandler
    logger: Logger

    def __init__(self, logger: Logger, console: Console | None = None) -> None:
        self.handler = RichHandler(
            console=console or RichConsoleStack.active_console(),
            level=logger.level,
            rich_tracebacks=True,
        )

        self.logger = logger

    def __enter__(self) -> Logger:
        self.logger.addHandler(self.handler)
        return self.logger

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.logger.removeHandler(self.handler)
