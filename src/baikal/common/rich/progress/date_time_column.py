import datetime

from typing import Any

from rich.console import RenderableType
from rich.progress import ProgressColumn, Task
from rich.text import Text


class DateTimeColumn(ProgressColumn):
    """Progress indicator for date(time)-based progress.

    Shows progress as <current> / {target} text.
    """

    def __init__(
        self,
        formatting: str = "%Y-%m-%d %H:%M:%S",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._formatting = formatting

    def render(self, task: Task) -> RenderableType:
        current = task.fields.get("current")
        current_string = (
            "?"
            if not isinstance(current, datetime.date)
            else current.strftime(self._formatting)
        )

        target = task.fields.get("target")
        target_string = (
            "?"
            if not isinstance(target, datetime.date)
            else target.strftime(self._formatting)
        )

        return Text.from_markup(f"{current_string} / {target_string}")
