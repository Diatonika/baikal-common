from dynaconf import Dynaconf
from rich.console import Console
from rich.theme import Theme


def console_from_dynaconf(settings: Dynaconf) -> Console:
    parameters = settings.to_dict()

    if "theme" in parameters:
        parameters["theme"] = Theme(**parameters["theme"])

    return Console(**parameters)
