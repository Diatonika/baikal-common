from rich.progress import BarColumn, MofNCompleteColumn
from rich.progress import Progress as RichProgress
from rich.progress import (
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from common.rich._console import CONSOLE


class Progress(RichProgress):

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

    def __init__(self, *, hide: bool = False):
        super().__init__(*self.COLUMNS, console=CONSOLE, disable=hide)
