import logging

from rich.console import Console

from baikal.common.rich import RichConsoleStack, RichLogHandler


def test_explicit_console_logging() -> None:
    logger = logging.getLogger("test_explicit_console_logging")

    with (
        RichConsoleStack(Console(width=1000)) as console,
        console.capture() as capture,
        RichLogHandler(logger, console) as logger,
    ):
        logger.error("test_explicit_console_logging")

    assert "test_explicit_console_logging" in capture.get()


def test_implicit_console_logging() -> None:
    logger = logging.getLogger("test_implicit_console_logging")

    with (
        RichConsoleStack(Console(width=1000)) as console,
        console.capture() as capture,
        RichLogHandler(logger, None) as logger,
    ):
        logger.error("test_implicit_console_logging")

    assert "test_implicit_console_logging" in capture.get()


def test_nested_logging() -> None:
    logger_zero = logging.getLogger("level-zero")
    logger_one = logging.getLogger("level-one")

    with (
        RichConsoleStack(Console(width=1000)) as console_zero,
        console_zero.capture() as capture_zero,
        RichLogHandler(logger_zero, console_zero) as logger_zero,
    ):
        with (
            RichConsoleStack(Console(width=1000)) as console_one,
            console_one.capture() as capture_one,
            RichLogHandler(logger_one, console_one) as logger_one,
        ):
            logger_one.error("logger-one")

        logger_zero.error("logger-zero")

    assert "logger-zero" in capture_zero.get()
    assert "logger-one" in capture_one.get()

    assert "logger-zero" not in capture_one.get()
    assert "logger-one" not in capture_zero.get()


def test_parallel_logging() -> None:
    logger = logging.getLogger("test_parallel_logging")

    console_zero = Console(width=1000)
    console_one = Console(width=1000)

    with (
        console_zero.capture() as capture_zero,
        console_one.capture() as capture_one,
        RichLogHandler(logger, console_zero),
        RichLogHandler(logger, console_one),
    ):
        logger.error("[yellow] test_parallel_logging [/yellow]")

    assert "[yellow]" in capture_zero.get()
    assert "[yellow]" in capture_one.get()

    assert "[/yellow]" in capture_one.get()
    assert "[/yellow]" in capture_zero.get()
