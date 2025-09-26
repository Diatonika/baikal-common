from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, assert_never

import pytest

from polars import (
    DataFrame as PolarDataFrame,
    LazyFrame as PolarLazyFrame,
    col,
)
from pyarrow import Table as ArrowTable

from baikal.common.dataset.arrow import (
    TimeSeriesSlicer,
    from_parquet,
    memory_map,
)
from baikal.common.dataset.arrow.time_series_metadata import TimeSeriesMetaData
from baikal.common.dataset.parquet import ParquetTimeSeriesPartition
from baikal.common.models import OHLC
from tests.dataset.util import write_parquet_sample

# region Utility


def _arrow_dataset_date_slice(
    dataset: TimeSeriesSlicer,
    start: datetime,
    end: datetime,
) -> ArrowTable:
    return dataset.slice(start, end)


def _polar_dataset_date_slice(
    dataset: PolarLazyFrame,
    engine: Literal["in-memory", "streaming"],
    start: datetime,
    end: datetime,
    on: str,
) -> PolarDataFrame:
    return dataset.filter(col(on) >= start, col(on) < end).collect(engine=engine)


def _benchmark_date_slicing(
    parquet_path: Path,
    arrow_path: Path,
    benchmark: Callable[..., Any],
    framework: Literal["time-series-slicer", "polars", "polars-streaming"],
    start: datetime,
    end: datetime,
) -> None:
    batches = memory_map(from_parquet(parquet_path, arrow_path), TimeSeriesMetaData)

    match framework:
        case "time-series-slicer":
            slicer = TimeSeriesSlicer.from_batches(batches.batches)
            sliced = benchmark(_arrow_dataset_date_slice, slicer, start, end)
            assert len(sliced)

        case "polars":
            polar_frame = batches.to_polars().set_sorted(OHLC.date_time)

            sliced = benchmark(
                _polar_dataset_date_slice,
                polar_frame.lazy(),
                "in-memory",
                start,
                end,
                OHLC.date_time,
            )

            assert len(sliced)

        case "polars-streaming":
            polar_frame = batches.to_polars().set_sorted(OHLC.date_time)

            sliced = benchmark(
                _polar_dataset_date_slice,
                polar_frame.lazy(),
                "streaming",
                start,
                end,
                OHLC.date_time,
            )

            assert len(sliced)

        case _:
            assert_never(framework)


# endregion


@pytest.mark.parametrize(
    "framework", ["time-series-slicer", "polars", "polars-streaming"]
)
def test_benchmark_date_slice_short(
    tmp_path: Path,
    benchmark: Callable[..., Any],
    framework: Literal["time-series-slicer", "polars", "polars-streaming"],
) -> None:
    write_parquet_sample(
        tmp_path / "parquet-data",
        ParquetTimeSeriesPartition.MONTH,
        start=datetime(2020, 1, 1, tzinfo=UTC),
        end=datetime(2025, 1, 1, tzinfo=UTC),
    )

    _benchmark_date_slicing(
        tmp_path / "parquet-data",
        tmp_path / "arrow-data",
        benchmark,
        framework,
        datetime(2024, 2, 25, 4, 30, tzinfo=UTC),
        datetime(2024, 2, 25, 5, 45, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    "framework", ["time-series-slicer", "polars", "polars-streaming"]
)
def test_benchmark_date_slice_long(
    tmp_path: Path,
    benchmark: Callable[..., Any],
    framework: Literal["time-series-slicer", "polars", "polars-streaming"],
) -> None:
    write_parquet_sample(
        tmp_path / "parquet-data",
        ParquetTimeSeriesPartition.MONTH,
        start=datetime(2020, 1, 1, tzinfo=UTC),
        end=datetime(2025, 1, 1, tzinfo=UTC),
    )

    _benchmark_date_slicing(
        tmp_path / "parquet-data",
        tmp_path / "arrow-data",
        benchmark,
        framework,
        datetime(2021, 2, 15, 12, 30, tzinfo=UTC),
        datetime(2024, 7, 10, 17, 45, tzinfo=UTC),
    )
