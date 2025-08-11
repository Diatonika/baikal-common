from datetime import UTC, datetime, timedelta
from pathlib import Path

from pandera.typing.polars import DataFrame
from polars import (
    DataFrame as PolarDataFrame,
    col,
    datetime_range,
    len as length,
    scan_parquet,
)
from pyarrow.parquet import read_metadata

from baikal.common.trade.models import OHLC
from baikal.common.trade.parquet import (
    ParquetTimeSeriesPartition,
    ParquetTimeSeriesWriter,
)


def _write_parquet_sample(writer: ParquetTimeSeriesWriter) -> None:
    with writer:
        for range_start in datetime_range(
            datetime(2020, 1, 1, tzinfo=UTC),
            datetime(2021, 1, 1, tzinfo=UTC),
            "11d",
            closed="left",
            eager=True,
        ):
            polar_frame = (
                PolarDataFrame(
                    {
                        OHLC.date_time: datetime_range(
                            range_start,
                            range_start + timedelta(days=11),
                            "1m",
                            closed="left",
                            eager=True,
                        ),
                    }
                )
                .with_columns(
                    open=col(OHLC.date_time).dt.hour(),
                    high=col(OHLC.date_time).dt.hour() + 1,
                    low=col(OHLC.date_time).dt.hour() - 1,
                    close=col(OHLC.date_time).dt.hour(),
                )
                .filter(col(OHLC.date_time) < datetime(2021, 1, 1, tzinfo=UTC))
            )

            writer.write(DataFrame[OHLC](polar_frame))


def test_parquest_time_series_writer(tmp_path: Path) -> None:
    writer = ParquetTimeSeriesWriter(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    _write_parquet_sample(writer)

    loaded = scan_parquet(
        tmp_path / "parquet-data" / "**" / "*.parquet",
        glob=True,
        schema=OHLC.polar_schema(),
    )

    assert loaded.select(length()).collect().item() == 527_040

    assert loaded.select(OHLC.date_time).min().collect().item() == datetime(
        2020, 1, 1, tzinfo=UTC
    )

    assert loaded.select(OHLC.date_time).max().collect().item() == datetime(
        2020, 12, 31, 23, 59, tzinfo=UTC
    )

    assert loaded.null_count().collect().sum_horizontal().item() == 0


def test_parquet_writer_metadata(tmp_path: Path) -> None:
    writer = ParquetTimeSeriesWriter(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    _write_parquet_sample(writer)

    first_parquet = next((tmp_path / "parquet-data").rglob("**/*.parquet"))
    metadata = read_metadata(first_parquet)

    for row_group_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(row_group_index)

        for column_index in range(row_group.num_columns):
            column = row_group.column(column_index)
            assert column.statistics is not None
            assert column.statistics.has_min_max

        assert len(row_group.sorting_columns) == 1


def test_parquet_writer_global_metadata(tmp_path: Path) -> None:
    writer = ParquetTimeSeriesWriter(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    _write_parquet_sample(writer)
    metadata = read_metadata(tmp_path / "parquet-data" / "_metadata")
    print(metadata)

    for row_group_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(row_group_index)

        for column_index in range(row_group.num_columns):
            column = row_group.column(column_index)
            assert column.statistics is not None
            assert column.statistics.has_min_max

        assert len(row_group.sorting_columns) == 1

    first_batch = metadata.row_group(0).column(0)
    first_statistics = first_batch.statistics
    assert first_statistics is not None

    min_date_time = first_statistics.min
    assert datetime(2020, 1, 1, tzinfo=UTC) == min_date_time

    last_batch = metadata.row_group(metadata.num_row_groups - 1).column(0)
    last_statistics = last_batch.statistics
    assert last_statistics is not None

    max_date_time = last_statistics.max
    assert datetime(2020, 12, 31, 23, 59, tzinfo=UTC) == max_date_time


def test_mmap_time_series_reader(tmp_path: Path) -> None:
    writer = ParquetTimeSeriesWriter(
        tmp_path / "parquet-data", ParquetTimeSeriesPartition.MONTH
    )

    _write_parquet_sample(writer)

    # schema = arrow_schema(
    #     cast(
    #         Mapping[str, DataType],
    #         {
    #             OHLC.date_time: timestamp("us"),
    #             OHLC.open: float64(),
    #             OHLC.high: float64(),
    #             OHLC.low: float64(),
    #             OHLC.close: float64(),
    #         },
    #     )
    # )
