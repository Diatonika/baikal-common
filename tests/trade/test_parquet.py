import datetime

from pathlib import Path

from pandera.typing.polars import DataFrame
from polars import (
    DataFrame as PolarDataFrame,
    col,
    datetime_range,
    len as length,
    scan_parquet,
)

from baikal.common.trade.models import OHLC
from baikal.common.trade.parquet import (
    ParquetTimeSeriesPartition,
    ParquetTimeSeriesWriter,
)


def test_parquest_time_series_writer(tmp_path: Path) -> None:
    writer = ParquetTimeSeriesWriter[OHLC](
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    with writer:
        for range_start in datetime_range(
            datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC),
            datetime.datetime(2021, 1, 1, tzinfo=datetime.UTC),
            "11d",
            closed="left",
            eager=True,
        ):
            polar_frame = PolarDataFrame(
                {
                    OHLC.date_time: datetime_range(
                        range_start,
                        range_start + datetime.timedelta(days=11),
                        "1m",
                        closed="left",
                        eager=True,
                    ),
                }
            ).with_columns(
                open=col(OHLC.date_time).dt.hour(),
                high=col(OHLC.date_time).dt.hour() + 1,
                low=col(OHLC.date_time).dt.hour() - 1,
                close=col(OHLC.date_time).dt.hour(),
            )

            writer.write(DataFrame[OHLC](polar_frame))

    loaded = scan_parquet(tmp_path / "parquet-data", schema=OHLC.polar_schema())

    assert loaded.select(length()).collect().item() == 538_560

    assert loaded.select(OHLC.date_time).min().collect().item() == datetime.datetime(
        2020, 1, 1, tzinfo=datetime.UTC
    )

    assert loaded.select(OHLC.date_time).max().collect().item() == datetime.datetime(
        2021, 1, 8, 23, 59, tzinfo=datetime.UTC
    )

    assert loaded.null_count().collect().sum_horizontal().item() == 0
