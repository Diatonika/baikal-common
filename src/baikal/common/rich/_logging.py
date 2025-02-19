from logging import Logger
from types import TracebackType

from rich.console import Console
from rich.logging import RichHandler

from baikal.common.rich._console import ConsoleContext


class LogContext(RichHandler):
    logger: Logger

    def __init__(self, console: Console | None, logger: Logger) -> None:
        super().__init__(
            console=console or ConsoleContext.active(),
            level=logger.level,
            rich_tracebacks=True,
        )

        self.logger = logger

    def __enter__(self) -> Logger:
        self.logger.addHandler(self)
        return self.logger

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.logger.removeHandler(self)
