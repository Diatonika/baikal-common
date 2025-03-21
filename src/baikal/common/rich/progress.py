from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)


class RichProgress(Progress):
    COLUMNS = (
        TextColumn("[progress.description]{task.description}", justify="left"),
        TextColumn("｜", justify="center"),
        TaskProgressColumn(justify="right"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("•", justify="center"),
        TimeElapsedColumn(),
        TextColumn("•", justify="center"),
        TimeRemainingColumn(),
    )

    def __init__(self, console: Console | None, *, hide: bool = False) -> None:
        super().__init__(*self.COLUMNS, console=console, disable=hide)
