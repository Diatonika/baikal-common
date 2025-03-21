from pathlib import Path

import pytest

from dynaconf import Dynaconf

from baikal.common.rich import console_from_dynaconf


def test_basic_config(datadir: Path) -> None:
    settings = Dynaconf(
        environments=False, settings_files=[datadir / "basic-config.toml"]
    )

    console = console_from_dynaconf(settings["baikal.common.rich.console"])
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

    console = console_from_dynaconf(settings["baikal.common.rich.console"])
    assert console.get_style(style, default=None) is not None
