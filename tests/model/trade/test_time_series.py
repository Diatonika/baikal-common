from datetime import datetime

import pytest
from polars import DataFrame

from baikal.common.model.exceptions import CastError
from baikal.common.model.trade import TimeSeries


def test_time_series_cast() -> None:
    frame = DataFrame({"date_time": ["2024-01-01T00:00:00+00:00"]})
    frame = TimeSeries.polar().cast(frame)

    date_time = frame["date_time"].first()
    assert isinstance(date_time, datetime)
    assert date_time == datetime.fromisoformat("2024-01-01T00:00:00+00:00")


def test_time_series_cast_fail() -> None:
    frame = DataFrame({"date_time": ["2024-01-01T25:00:00+01:00"]})
    with pytest.raises(CastError):
        TimeSeries.polar().cast(frame)


def test_time_series_timezone_coercion() -> None:
    time_series = DataFrame({"date_time": ["2024-01-01T05:00:00+05:00"]})
    time_series = TimeSeries.polar().cast(time_series)

    date_time = time_series["date_time"].first()
    assert isinstance(date_time, datetime)
    assert date_time == datetime.fromisoformat("2024-01-01T00:00:00+00:00")

