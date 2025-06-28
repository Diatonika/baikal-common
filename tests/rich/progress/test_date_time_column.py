import datetime

from rich.console import Console
from rich.progress import Progress

from baikal.common.rich.progress import DateTimeColumn


def test_date_time_indicator() -> None:
    console = Console()

    with (
        console.capture() as capture,
        Progress(DateTimeColumn(), console=console) as progress,
    ):
        current = datetime.date(2020, 1, 1)
        target = datetime.date(2020, 1, 2)

        task = progress.add_task(
            "test_date_time_indicator",
            current=current,
            target=target,
        )

        progress.update(task, current=current, refresh=True)

    assert capture.get() == "2020-01-01 00:00:00 / 2020-01-02 00:00:00\n"
