from pathlib import Path

import pytest

from dynaconf import Dynaconf

from baikal.common.rich import ConsoleContext


def test_basic_config(datadir: Path) -> None:
    settings = Dynaconf(
        environments=False, settings_files=[datadir / "basic-config.toml"]
    )

    console = ConsoleContext.from_dynaconf(settings)
    assert console.get_style("mock-style", default=None) is not None


@pytest.mark.parametrize(
    ("env", "style"), [("develop", "develop-style"), ("production", "production-style")]
)
def test_layered_config(datadir: Path, env: str, style: str) -> None:
    settings = Dynaconf(
        environments=True,
        settings_files=[datadir / "layered-config.toml"],
        env=env,
    )

    console = ConsoleContext.from_dynaconf(settings)
    assert console.get_style(style, default=None) is not None


def test_console_print() -> None:
    with ConsoleContext.from_parameters() as console:
        console.print("[info] test_console_print [/]")


def test_nested_console() -> None:
    with ConsoleContext.from_parameters({"level-zero": "bold red"}) as console_zero:
        with ConsoleContext.from_parameters({"level-one": "bold green"}) as console_one:
            console_one.print("[level-one] level-one [/]")

        console_zero.print("[level-zero] level-zero [/]")
