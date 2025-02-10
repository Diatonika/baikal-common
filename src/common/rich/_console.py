from typing import Self

from dynaconf import LazySettings
from rich import pretty
from rich.console import Console as RichConsole
from rich.theme import Theme


class Console(RichConsole):
    DEFAULT_THEME = {
        "error": "red3",
        "warning": "orange3",
        "info": "white",
        "debug": "white",
    }

    @classmethod
    def from_dynaconf(cls, settings: LazySettings, *, install: bool = True) -> Self:
        return cls.from_parameters(
            settings("baikal.common.rich.styles"), install=install
        )

    @classmethod
    def from_parameters(cls, styles: dict[str, str], *, install: bool = True) -> Self:
        console = cls(theme=Theme(cls.DEFAULT_THEME | styles))

        if install:
            pretty.install(console)

        return console
