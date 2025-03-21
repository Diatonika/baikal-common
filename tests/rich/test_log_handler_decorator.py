import logging

from rich.console import Console
from rich.theme import Theme

from baikal.common.rich import RichConsoleStack, with_handler


@with_handler(logging.getLogger("_mock_function_one"))
def _mock_log_function() -> None:
    logger = logging.getLogger("_mock_function_one")

    with RichConsoleStack.active_console().capture() as capture:
        logger.error("[mock-style-one] _mock_function_one [/]")

    assert "_mock_function_one" in capture.get()


def test_logging_decorator() -> None:
    with RichConsoleStack(
        Console(theme=Theme({"mock-style-one": "bold green"}), width=1000)
    ):
        _mock_log_function()
