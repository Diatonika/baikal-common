import logging

from baikal.common.rich import ConsoleContext, LogContext


def test_explicit_console_logging() -> None:
    logger = logging.getLogger("test_explicit_console_logging")

    with (
        ConsoleContext.from_parameters({}) as console,
        LogContext(console, logger) as logger,
    ):
        logger.error("[error] test_explicit_console_logging [/]")


def test_implicit_console_logging() -> None:
    logger = logging.getLogger("test_implicit_console_logging")

    with LogContext(None, logger) as logger:
        logger.error("[error] test_implicit_console_logging [/]")


def test_nested_logging() -> None:
    logger_zero = logging.getLogger("level-zero")
    logger_one = logging.getLogger("level-one")

    with (
        ConsoleContext.from_parameters({}) as console,
        LogContext(console, logger_zero) as logger_zero,
    ):
        with LogContext(console, logger_one) as logger_one:
            logger_one.error("[error] logger-one [/]")

        logger_zero.error("[error] logger-zero [/]")
