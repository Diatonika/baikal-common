from datetime import datetime

import pytest

from pandera.errors import SchemaError
from pandera.typing.polars import DataFrame

from baikal.common.models import TimeSeries


def test_time_series_init() -> None:
    _ = DataFrame[TimeSeries]({"date_time": ["2024-01-01T00:00:00+00:00"]})


def test_time_series_timezone_coercion() -> None:
    time_series = DataFrame[TimeSeries]({"date_time": ["2024-01-01T05:00:00+05:00"]})

    date_time = time_series["date_time"].first()
    assert date_time == datetime.fromisoformat("2024-01-01T00:00:00+00:00")


def test_time_series_init_fail() -> None:
    with pytest.raises(SchemaError):
        _ = DataFrame[TimeSeries]({"date_time": ["2024-01-01T25:00:00+01:00"]})
