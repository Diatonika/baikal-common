from rich.console import Console

from baikal.common.rich import RichProgress


def test_default_console() -> None:
    with RichProgress(None) as progress:
        for _ in progress.track(range(5), total=5, description="test_default_console"):
            pass


def test_rich_console() -> None:
    with RichProgress(Console()) as progress:
        for _ in progress.track(range(5), total=5, description="test_rich_console"):
            pass
