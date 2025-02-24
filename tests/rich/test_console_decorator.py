from rich.console import Console
from rich.theme import Theme

from baikal.common.rich import ConsoleContext, with_console


@with_console(Console(theme=Theme({"mock-style-one": "bold green"})))
def _mock_function_one() -> None:
    console = ConsoleContext.active()
    assert console.get_style("mock-style-one", default=None) is not None


def test_basic_console_decorator() -> None:
    _mock_function_one()


@with_console(Console(theme=Theme({"mock-style-two": "bold red"})))
def _mock_function_two() -> None:
    _mock_function_one()
    console = ConsoleContext.active()
    assert console.get_style("mock-style-two", default=None) is not None


def test_nested_console_decorator() -> None:
    _mock_function_two()
