import datetime

import pytest

from baikal.common.rich.progress import TimeFraction


def test_time_fraction() -> None:
    calculator = TimeFraction(
        datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC),
        datetime.datetime(2020, 1, 2, tzinfo=datetime.UTC),
    )

    current = datetime.datetime(2020, 1, 1, 12, tzinfo=datetime.UTC)
    assert calculator.fraction(current) == pytest.approx(0.5)
