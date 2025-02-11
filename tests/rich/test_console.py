from pathlib import Path

import pytest

from dynaconf import Dynaconf

from baikal.common.rich import Console


def test_basic_config(datadir: Path) -> None:
    settings = Dynaconf(
        environments=False, settings_files=[datadir / "basic-config.toml"]
    )

    console = Console.from_dynaconf(settings)
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

    console = Console.from_dynaconf(settings)
    assert console.get_style(style, default=None) is not None
