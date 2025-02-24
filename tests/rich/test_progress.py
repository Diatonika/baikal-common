from baikal.common.rich import ConsoleContext, Progress


def test_default_console() -> None:
    with Progress(None) as progress:
        for _ in progress.track(range(5), total=5, description="test_default_console"):
            pass


def test_rich_console() -> None:
    console = ConsoleContext.from_parameters()

    with Progress(console) as progress:
        for _ in progress.track(range(5), total=5, description="test_rich_console"):
            pass
