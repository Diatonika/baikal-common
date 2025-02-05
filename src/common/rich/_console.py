from dynaconf import Dynaconf, Validator
from rich import pretty
from rich.console import Console
from rich.theme import Theme

SETTINGS = Dynaconf(environments=True, validators=[Validator("themes", default={})])
THEME = Theme(
    {
        "error": "red3",
        "warning": "orange3",
        "info": "white",
        "debug": "white",
    }
    | SETTINGS("themes")
)

CONSOLE = Console(theme=THEME)
pretty.install(CONSOLE)
