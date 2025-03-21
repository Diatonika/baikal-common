from rich.console import Console
from rich.theme import Theme

from baikal.common.rich import RichConsoleStack


def test_console_print() -> None:
    with RichConsoleStack(Console(style="yellow")) as console:
        console.print("test_console_print")


def test_nested_console() -> None:
    console_zero = Console(theme=Theme({"level-zero": "bold red"}))
    console_one = Console(theme=Theme({"level-one": "bold green"}))

    with RichConsoleStack(console_zero) as console_zero:
        with RichConsoleStack(console_one) as console_one:
            console_one.print("[level-one] level-one [/]")

        console_zero.print("[level-zero] level-zero [/]")
